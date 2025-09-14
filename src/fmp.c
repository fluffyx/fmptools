/* FMP Tools - A library for reading FileMaker Pro databases
 * Copyright (c) 2020 Evan Miller (except where otherwise noted)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#define _XOPEN_SOURCE 600 /* strptime */
#define _POSIX_C_SOURCE 200809L /* fmemopen */
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iconv.h>
#include <stdarg.h>
#include <libgen.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "fmp.h"
#include "fmp_internal.h"

#define MAGICK "\x00\x01\x00\x00\x00\x02\x00\x01\x00\x05\x00\x02\x00\x02\xC0"

typedef struct fmp_ctx_s {
    int did_print_current_path;
    fmp_column_t *current_column;
    fmp_table_t *current_table;
    size_t current_row;
    size_t last_column;
    size_t num_tables;
    fmp_table_t *tables;
    size_t num_columns;
    fmp_column_t *columns;
    void *user_ctx;
} fmp_ctx_t;

static void copy_fixed_string(char *dst, size_t dst_len, const void *buf, size_t buf_len) {
    if (buf_len > dst_len - 1)
        buf_len = dst_len - 1;
    memcpy(dst, buf, buf_len);
    dst[buf_len] = '\0';
}

static void copy_pascal_string(char *dst, size_t dst_len, const void *buf) {
    const unsigned char *chars = (const unsigned char *)buf;
    size_t len = *chars++;
    if (len >= dst_len)
        len = dst_len-1;
    memcpy(dst, chars, len);
    dst[len] = '\0';
}

void debug(const char *fmt, ...) {
    va_list argp;
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
}

fmp_error_t read_header(fmp_file_t *ctx) {
    char buf[1024];

    if (!fread(buf, sizeof(buf), 1, ctx->stream))
        return FMP_ERROR_READ;

    if (memcmp(buf, MAGICK, sizeof(MAGICK)-1)) {
        return FMP_ERROR_BAD_MAGIC_NUMBER;
    }

    if (memcmp(&buf[15], "HBAM7", 5) == 0) {
        // or 4096
        ctx->sector_size = 4096;
        ctx->xor_mask = 0x5A;
        ctx->prev_sector_offset = 4;
        ctx->next_sector_offset = 8;
        ctx->payload_len_offset = -1;
        ctx->sector_head_len = 20;
        ctx->version_num = (buf[521] == 0x1E) ? 12 : 7;
        /* Big-endian flag somewhere? */
        /* Don't set ctx->converter; we use a custom decoder */
    } else {
        ctx->sector_size = 1024;
        ctx->prev_sector_offset = 2;
        ctx->next_sector_offset = 6;
        ctx->payload_len_offset = 12;
        ctx->sector_head_len = 14;
        ctx->sector_index_shift = 1;
        ctx->converter = iconv_open("UTF-8", "MACINTOSH");
    }
    if (ctx->converter == (iconv_t)-1) {
        return FMP_ERROR_UNSUPPORTED_CHARACTER_SET;
    }

    copy_fixed_string(ctx->version_date_string, sizeof(ctx->version_date_string), &buf[531], 7);
#ifdef HAVE_STRPTIME
    strptime(ctx->version_date_string, "%d%b%y", &ctx->version_date);
#endif
    copy_pascal_string(ctx->version_string, sizeof(ctx->version_string), &buf[541]);

    // Throwaway sector
    if (ctx->sector_size == 1024) {
        fseek(ctx->stream, 2 * ctx->sector_size, SEEK_SET);
    } else {
        fseek(ctx->stream, ctx->sector_size, SEEK_SET);
    }

    return FMP_OK;
}

uint64_t path_value(fmp_chunk_t *chunk, fmp_data_t *path) {
    if (!path)
        return 0;
    if (path->len == 1)
        return path->bytes[0];
    if (path->len == 2)
        return 0x80 + ((path->bytes[0] & 0x7F) << 8) + path->bytes[1];
    if (path->len == 3) {
        if (chunk->version_num < 7)
            return 0xC000 + ((path->bytes[0] & 0x3F) << 16) + (path->bytes[1] << 8) + path->bytes[2];
        return 0x80 + (path->bytes[1] << 8) + path->bytes[2];
    }
    return 0;
}

int path_is(fmp_chunk_t *chunk, fmp_data_t *path, uint64_t value) {
    return path_value(chunk, path) == value;
}

void convert(iconv_t converter, uint8_t xor_mask,
        char *dst, size_t dst_len, uint8_t *src, size_t src_len) {
    char *input_bytes = (char *)src;
    size_t input_bytes_left = src_len;
    if (xor_mask) {
        input_bytes = malloc(input_bytes_left);
        for (int i=0; i<input_bytes_left; i++) {
            input_bytes[i] = xor_mask ^ src[i];
        }
        src = (uint8_t *)input_bytes;
    }
    while (input_bytes_left && input_bytes[0] == ' ') {
        input_bytes++;
        input_bytes_left--;
    }
    char *output_bytes = dst;
    size_t output_bytes_left = dst_len;
    if (converter) {
        iconv(converter, &input_bytes, &input_bytes_left, &output_bytes, &output_bytes_left);
    } else {
        convert_scsu_to_utf8(&input_bytes, &input_bytes_left, &output_bytes, &output_bytes_left);
    }
    if (output_bytes_left) {
        dst[dst_len-output_bytes_left] = '\0';
    } else if (dst_len) {
        dst[dst_len-1] = '\0';
    }
    if (xor_mask) {
        free(src);
    }
}

int table_path_depth(fmp_chunk_t *chunk) {
    if (chunk->version_num < 7)
        return chunk->path_level;
    return chunk->path_level - 1;
}

int table_path_match_start1(fmp_chunk_t *chunk, int depth, int val) {
    if (table_path_depth(chunk) != depth)
        return 0;
    if (chunk->version_num < 7)
        return path_is(chunk, chunk->path[0], val);
    return path_value(chunk, chunk->path[0]) >= 128 && path_is(chunk, chunk->path[1], val);
}

int table_path_match_start2(fmp_chunk_t *chunk, int depth, int val1, int val2) {
    if (table_path_depth(chunk) != depth)
        return 0;
    if (chunk->version_num < 7)
        return path_is(chunk, chunk->path[0], val1) && path_is(chunk, chunk->path[1], val2);
    return (path_value(chunk, chunk->path[0]) >= 128 &&
            path_is(chunk, chunk->path[1], val1) && path_is(chunk, chunk->path[2], val2));
}

chunk_status_t process_chunk(fmp_file_t *file, fmp_chunk_t *chunk,
        chunk_handler handle_chunk, void *user_ctx) {
    chunk->path = file->path;
    chunk->path_level = file->path_level;
    chunk->version_num = file->version_num;
    if (chunk->type == FMP_CHUNK_PATH_POP) {
        if (file->path_level)
            file->path_level--;
    }
    if (chunk->type == FMP_CHUNK_PATH_PUSH) {
        if (file->path_level + 1 > file->path_capacity)
            file->path = realloc(file->path, (file->path_capacity *= 2) * sizeof(fmp_data_t *));
        file->path[file->path_level++] = &chunk->data;
    }
    return handle_chunk(chunk, user_ctx);
}

fmp_error_t process_chunk_chain(fmp_file_t *file, fmp_chunk_t *chunk,
        chunk_handler handle_chunk, void *user_ctx) {
    file->path_level = 0;
    while (chunk) {
        chunk_status_t status = process_chunk(file, chunk, handle_chunk, user_ctx);
        if (status == CHUNK_ABORT)
            return FMP_ERROR_USER_ABORTED;
        if (status == CHUNK_DONE)
            break;
        if (status == CHUNK_NEXT)
            chunk = chunk->next;
    }
    return FMP_OK;
}

void free_chunk_chain(fmp_block_t *block) {
    fmp_chunk_t *chunk = block->chunk;
    while (chunk) {
        fmp_chunk_t *next = chunk->next;
        free(chunk);
        chunk = next;
    }
    block->chunk = NULL;
}

/* Load a block on-demand from mmap'd memory */
static fmp_block_t *load_block_from_mmap(fmp_file_t *file, int block_idx) {
    if (block_idx < 0 || block_idx >= file->num_blocks) {
        return NULL;
    }

    /* Check if we have space in the blocks array AND should cache */
    if (block_idx < file->blocks_allocated) {
        /* Check if already loaded */
        if (file->blocks[block_idx]) {
            return file->blocks[block_idx];
        }
    }

    /* Calculate offset in file */
    size_t offset = (block_idx + 1) * file->sector_size;
    if (offset >= file->file_size) {
        return NULL;
    }

    /* Get pointer to sector data in mmap */
    uint8_t *sector = ((uint8_t *)file->mmap_base) + offset;

    /* Create block from sector */
    fmp_error_t error = FMP_OK;
    fmp_block_t *block = new_block_from_sector(file, sector, &error);

    /* For large files, don't cache blocks - they'll be freed after use */
    /* Only cache the first few blocks for repeated access */
    if (block && error == FMP_OK && block_idx < 100 && block_idx < file->blocks_allocated) {
        file->blocks[block_idx] = block;
    }

    return block;
}

fmp_error_t process_blocks(fmp_file_t *file,
        block_handler handle_block,
        chunk_handler handle_chunk,
        void *user_ctx) {
    fmp_error_t retval = FMP_OK;
    int next_block = 2;

    /* Debug: check memory allocation */
    if (file->use_mmap) {
        fprintf(stderr, "Processing blocks with mmap, num_blocks=%zu\n", file->num_blocks);
    }

    /* For large files, don't track all visited blocks - just detect loops */
    int *blocks_visited = NULL;
    int max_iterations = file->num_blocks * 2;  /* Safety limit */
    int iteration = 0;

    /* Only allocate visited array for small files */
    if (file->num_blocks < 100000) {
        blocks_visited = calloc(file->num_blocks, sizeof(int));
    }

    do {
        fmp_block_t *block = NULL;

        /* Load block on-demand for mmap'd files */
        if (file->use_mmap) {
            block = load_block_from_mmap(file, next_block - 1);
        } else {
            block = file->blocks[next_block - 1];
        }

        if (!block) {
            retval = FMP_ERROR_BAD_SECTOR;
            break;
        }

        retval = process_block(file, block);

        /* Only track visits for smaller files */
        if (blocks_visited && next_block - 1 < file->num_blocks) {
            if (blocks_visited[next_block-1]) {
                /* Loop detected */
                break;
            }
            blocks_visited[next_block-1] = 1;
        }

        if (retval != FMP_OK) {
            break;
        }
        block->this_id = next_block;
        if (!handle_block || handle_block(block, user_ctx))
            retval = process_chunk_chain(file, block->chunk, handle_chunk, user_ctx);
        int saved_next_id = block->next_id;

        /* CRITICAL: Free the block if it's not cached (for large mmap files) */
        int should_free = 0;
        if (file->use_mmap) {
            if (next_block - 1 >= file->blocks_allocated) {
                /* Block index is beyond our cache array */
                should_free = 1;
            } else if (!file->blocks[next_block - 1]) {
                /* Block wasn't cached */
                should_free = 1;
            }
        }

        if (should_free) {
            free_chunk_chain(block);
            free(block);
        }

        next_block = saved_next_id;

        /* Safety check for large files without visited tracking */
        if (++iteration > max_iterations) {
            fprintf(stderr, "Warning: Too many iterations, possible loop\n");
            break;
        }
    } while (next_block != 0 && next_block - 1 < file->num_blocks && retval == FMP_OK);

    if (blocks_visited)
        free(blocks_visited);

    return retval;
}

static fmp_file_t *fmp_file_from_stream(FILE *stream, const char *filename, fmp_error_t *errorCode) {
    uint8_t *sector = NULL;
    fmp_error_t retval = FMP_OK;
    fmp_file_t *file = calloc(1, sizeof(fmp_file_t));
    fmp_block_t *first_block = NULL;
    file->stream = stream;

    if (fseek(stream, 0, SEEK_END) == -1) {
        retval = FMP_ERROR_SEEK;
        goto cleanup;
    }
    file->path_capacity = 16;
    file->path = calloc(file->path_capacity, sizeof(fmp_data_t *));
    file->file_size = ftello(stream);
    rewind(stream);

    if (filename) 
        snprintf(file->filename, sizeof(file->filename), "%s", filename);

    retval = read_header(file);
    if (retval != FMP_OK)
        goto cleanup;

    sector = malloc(file->sector_size);
    if (!sector) {
        retval = FMP_ERROR_MALLOC;
        goto cleanup;
    }
    if (!fread(sector, file->sector_size, 1, file->stream)) {
        retval = FMP_ERROR_READ;
        goto cleanup;
    }

    first_block = new_block_from_sector(file, sector, &retval);
    if (!first_block)
        goto cleanup;

    if (first_block->next_id == 0 ||
        (first_block->next_id + 1 + (file->version_num < 7)) * file->sector_size != file->file_size) {
        retval = FMP_ERROR_BAD_SECTOR_COUNT;
        goto cleanup;
    }

    file = realloc(file, sizeof(fmp_file_t) + first_block->next_id * sizeof(fmp_block_t *));
    if (!file) {
        retval = FMP_ERROR_MALLOC;
        goto cleanup;
    }
    file->num_blocks = first_block->next_id;
    file->blocks_allocated = first_block->next_id;  /* For non-mmap, we allocate all blocks */
    file->blocks[0] = first_block;
    first_block = NULL;

    memset(&file->blocks[1], 0, (file->num_blocks - 1) * sizeof(fmp_block_t *));

    int index = 1;
    while (fread(sector, file->sector_size, 1, file->stream) && index < file->num_blocks) {
        fmp_block_t *block = new_block_from_sector(file, sector, &retval);
        if (!block)
            goto cleanup;
        file->blocks[index++] = block;
    }

    if (index != file->num_blocks)
        retval = FMP_ERROR_BAD_SECTOR_COUNT;

cleanup:
    free(sector);
    free(first_block);

    if (retval != FMP_OK) {
        if (file)
            fmp_close_file(file);
        if (errorCode)
            *errorCode = retval;
        return NULL;
    }
    return file;
}

fmp_file_t *fmp_open_buffer(const void *buffer, size_t len, fmp_error_t *errorCode) {
    FILE *stream = NULL;
#ifdef HAVE_FMEMOPEN
    stream = fmemopen((void *)buffer, len, "r");
#else
    if (errorCode)
        *errorCode = FMP_ERROR_NO_FMEMOPEN;
    return NULL;
#endif
    if (!stream) {
        if (errorCode)
            *errorCode = FMP_ERROR_OPEN;
        return NULL;
    }
    return fmp_file_from_stream(stream, NULL, errorCode);
}

/* Enhanced version with mmap support for large files */
static fmp_file_t *fmp_open_file_mmap(const char *path, fmp_error_t *errorCode) {
    struct stat st;
    int fd = -1;
    void *mmap_base = NULL;
    fmp_file_t *file = NULL;
    fmp_error_t retval = FMP_OK;

    /* Open file for reading */
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        if (errorCode) *errorCode = FMP_ERROR_OPEN;
        return NULL;
    }

    /* Get file size */
    if (fstat(fd, &st) < 0) {
        close(fd);
        if (errorCode) *errorCode = FMP_ERROR_OPEN;
        return NULL;
    }

    /* Map the file into memory */
    mmap_base = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_base == MAP_FAILED) {
        close(fd);
        if (errorCode) *errorCode = FMP_ERROR_MALLOC;
        return NULL;
    }

    /* Allocate basic file structure */
    file = calloc(1, sizeof(fmp_file_t));
    if (!file) {
        munmap(mmap_base, st.st_size);
        close(fd);
        if (errorCode) *errorCode = FMP_ERROR_MALLOC;
        return NULL;
    }

    /* Initialize mmap fields */
    file->mmap_base = mmap_base;
    file->mmap_fd = fd;
    file->use_mmap = 1;
    file->file_size = st.st_size;

    /* Extract filename */
    char *path_copy = strdup(path);
    if (path_copy) {
        snprintf(file->filename, sizeof(file->filename), "%s", basename(path_copy));
        free(path_copy);
    }

    /* Read header from mmap'd memory */
    uint8_t *buf = (uint8_t *)mmap_base;
    if (memcmp(buf, MAGICK, sizeof(MAGICK)-1)) {
        retval = FMP_ERROR_BAD_MAGIC_NUMBER;
        goto cleanup_error;
    }

    /* Parse header */
    if (memcmp(&buf[15], "HBAM7", 5) == 0) {
        file->sector_size = 4096;
        file->xor_mask = 0x5A;
        file->prev_sector_offset = 4;
        file->next_sector_offset = 8;
        file->payload_len_offset = -1;
        file->sector_head_len = 20;
        file->version_num = (buf[521] == 0x1E) ? 12 : 7;
    } else {
        file->sector_size = 1024;
        file->xor_mask = 0x00;
        file->sector_index_shift = 9;
        file->prev_sector_offset = 4;
        file->next_sector_offset = 8;
        file->payload_len_offset = 12;
        file->sector_head_len = 14;

        if (memcmp(&buf[15], "HBAM3", 5) == 0) {
            file->version_num = 3;
            file->converter = iconv_open("UTF-8", "MACINTOSH");
        } else if (memcmp(&buf[15], "HBAM5", 5) == 0) {
            file->version_num = 5;
            file->converter = iconv_open("UTF-8", "WINDOWS-1252");
        }
    }

    /* Allocate path tracking */
    file->path_capacity = 16;
    file->path = calloc(file->path_capacity, sizeof(fmp_data_t *));
    if (!file->path) {
        retval = FMP_ERROR_MALLOC;
        goto cleanup_error;
    }

    /* Parse first block to get block count */
    fmp_block_t *first_block = new_block_from_sector(file, buf + file->sector_size, &retval);
    if (!first_block || retval != FMP_OK) {
        goto cleanup_error;
    }

    if (first_block->next_id == 0 ||
        (first_block->next_id + 1 + (file->version_num < 7)) * file->sector_size != file->file_size) {
        free(first_block);
        retval = FMP_ERROR_BAD_SECTOR_COUNT;
        goto cleanup_error;
    }

    file->num_blocks = first_block->next_id;

    /* For mmap files, DON'T allocate huge array - just keep basic structure */
    /* We'll allocate a smaller initial array and grow as needed */
    size_t initial_blocks = 1024; /* Start with space for 1024 blocks */
    if (file->num_blocks < initial_blocks) {
        initial_blocks = file->num_blocks;
    }

    /* Reallocate file structure with limited block pointers */
    fmp_file_t *new_file = realloc(file, sizeof(fmp_file_t) + initial_blocks * sizeof(fmp_block_t *));
    if (!new_file) {
        free(first_block);
        retval = FMP_ERROR_MALLOC;
        goto cleanup_error;
    }
    file = new_file;

    /* Important: Restore mmap pointers after realloc */
    file->mmap_base = mmap_base;
    file->mmap_fd = fd;
    file->use_mmap = 1;
    file->blocks_allocated = initial_blocks;

    /* Initialize only the allocated portion */
    memset(file->blocks, 0, initial_blocks * sizeof(fmp_block_t *));
    file->blocks[0] = first_block;

    /* We'll load blocks on-demand instead of all at once */
    /* This is the key change - we don't load all blocks into memory */

    if (errorCode) *errorCode = FMP_OK;
    return file;

cleanup_error:
    if (file) {
        if (file->path) free(file->path);
        free(file);
    }
    munmap(mmap_base, st.st_size);
    close(fd);
    if (errorCode) *errorCode = retval;
    return NULL;
}

fmp_file_t *fmp_open_file(const char *path, fmp_error_t *errorCode) {
    struct stat st;

    /* Check file size first */
    if (stat(path, &st) == 0) {
        /* Use mmap for files larger than 100MB */
        if (st.st_size > 100 * 1024 * 1024) {
            fprintf(stderr, "File size %lld bytes, using mmap\n", (long long)st.st_size);
            return fmp_open_file_mmap(path, errorCode);
        }
    }

    /* Fall back to original implementation for smaller files */
    fmp_file_t *file = NULL;
    FILE *stream = fopen(path, "r");
    if (!stream) {
        if (errorCode)
            *errorCode = FMP_ERROR_OPEN;
        return NULL;
    }
    char *path_copy = strdup(path);
    file = fmp_file_from_stream(stream, basename(path_copy), errorCode);
    free(path_copy);
    return file;
}

void fmp_close_file(fmp_file_t *file) {
    if (file->stream)
        fclose(file->stream);
    if (file->converter)
        iconv_close(file->converter);
    if (file->path)
        free(file->path);

    /* Handle mmap cleanup */
    if (file->use_mmap) {
        if (file->mmap_base) {
            munmap(file->mmap_base, file->file_size);
        }
        if (file->mmap_fd >= 0) {
            close(file->mmap_fd);
        }
    }

    for (int i=0; i<file->num_blocks; i++) {
        fmp_block_t *block = file->blocks[i];
        if (block) {
            free_chunk_chain(block);
            free(block);
        }
    }
    free(file);
}
