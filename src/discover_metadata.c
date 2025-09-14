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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "fmp.h"
#include "fmp_internal.h"

typedef struct fmp_discover_metadata_ctx_s {
    fmp_file_t *file;
    fmp_metadata_t *metadata;
} fmp_discover_metadata_ctx_t;

static void ensure_columns_capacity(fmp_metadata_t *metadata, size_t table_index) {
    if (table_index >= metadata->columns_capacity) {
        size_t new_capacity = table_index + 128; /* Grow by 128 tables at a time */
        metadata->columns = realloc(metadata->columns, new_capacity * sizeof(fmp_column_array_t *));
        memset(&metadata->columns[metadata->columns_capacity], 0,
               (new_capacity - metadata->columns_capacity) * sizeof(fmp_column_array_t *));
        metadata->columns_capacity = new_capacity;
    }

    if (!metadata->columns[table_index]) {
        metadata->columns[table_index] = calloc(1, sizeof(fmp_column_array_t));
    }
}

static void handle_table(fmp_chunk_t *chunk, fmp_discover_metadata_ctx_t *ctx, size_t table_index) {
    fmp_table_array_t *tables = ctx->metadata->tables;

    if (table_index > tables->count) {
        size_t old_count = tables->count;
        tables->count = table_index;
        tables->tables = realloc(tables->tables, tables->count * sizeof(fmp_table_t));
        memset(&tables->tables[old_count], 0, (table_index - old_count) * sizeof(fmp_table_t));
    }

    fmp_table_t *current_table = &tables->tables[table_index - 1];
    if (chunk->ref_simple == 16) {
        convert(ctx->file->converter, ctx->file->xor_mask,
                current_table->utf8_name, sizeof(current_table->utf8_name),
                chunk->data.bytes, chunk->data.len);
        current_table->index = table_index;

        /* Ensure we have a column array for this table */
        ensure_columns_capacity(ctx->metadata, table_index);
    }
}

static void handle_column(fmp_chunk_t *chunk, fmp_discover_metadata_ctx_t *ctx,
                         size_t table_index, size_t column_index) {
    ensure_columns_capacity(ctx->metadata, table_index);
    fmp_column_array_t *columns = ctx->metadata->columns[table_index];

    if (column_index > columns->count) {
        size_t old_count = columns->count;
        columns->count = column_index;
        columns->columns = realloc(columns->columns, columns->count * sizeof(fmp_column_t));
        memset(&columns->columns[old_count], 0, (column_index - old_count) * sizeof(fmp_column_t));
    }

    fmp_column_t *current_column = &columns->columns[column_index - 1];

    if (chunk->ref_simple == 16) {
        /* Column name (v7+) */
        convert(ctx->file->converter, ctx->file->xor_mask,
                current_column->utf8_name, sizeof(current_column->utf8_name),
                chunk->data.bytes, chunk->data.len);
        current_column->index = column_index;
    } else if (chunk->ref_simple == 1) {
        /* Column name (v3-v6) */
        convert(ctx->file->converter, ctx->file->xor_mask,
                current_column->utf8_name, sizeof(current_column->utf8_name),
                chunk->data.bytes, chunk->data.len);
        current_column->index = column_index;
    } else if (chunk->ref_simple == 2) {
        /* Column type and collation (v3-v6) */
        if (chunk->data.bytes[1] <= FMP_COLUMN_TYPE_GLOBAL) {
            current_column->type = chunk->data.bytes[1];
        } else {
            current_column->type = FMP_COLUMN_TYPE_UNKNOWN;
        }
        current_column->collation = chunk->data.bytes[3];
    }
}

static chunk_status_t handle_chunk_discover_v7(fmp_chunk_t *chunk, fmp_discover_metadata_ctx_t *ctx) {
    /* Check if this is a table definition chunk */
    if (path_is(chunk, chunk->path[0], 3) && path_is(chunk, chunk->path[1], 16) &&
        path_is(chunk, chunk->path[2], 5) && path_value(chunk, chunk->path[3]) >= 128) {

        if (chunk->type == FMP_CHUNK_FIELD_REF_SIMPLE && chunk->ref_simple == 16) {
            size_t table_index = path_value(chunk, chunk->path[3]) - 128;
            handle_table(chunk, ctx, table_index);
        }
        return CHUNK_NEXT;
    }

    /* Check if this is a column definition chunk */
    if (path_value(chunk, chunk->path[0]) >= 128) {
        size_t table_index = path_value(chunk, chunk->path[0]) - 128;

        if (chunk->type == FMP_CHUNK_FIELD_REF_SIMPLE && table_path_match_start2(chunk, 3, 3, 5)) {
            fmp_data_t *column_path = chunk->path[chunk->path_level - 1];
            size_t column_index = path_value(chunk, column_path);

            if (chunk->ref_simple == 16) {
                handle_column(chunk, ctx, table_index, column_index);
            }
        }
        return CHUNK_NEXT;
    }

    /* Stop scanning once we're past the metadata section */
    if (path_value(chunk, chunk->path[0]) > 3 && path_value(chunk, chunk->path[0]) < 128) {
        return CHUNK_DONE;
    }

    return CHUNK_NEXT;
}

static chunk_status_t handle_chunk_discover_v3(fmp_chunk_t *chunk, fmp_discover_metadata_ctx_t *ctx) {
    /* For v3-v6, there's only one table and columns are in path[0] <= 3 */
    if (path_value(chunk, chunk->path[0]) > 3)
        return CHUNK_DONE;

    if (chunk->type != FMP_CHUNK_FIELD_REF_SIMPLE)
        return CHUNK_NEXT;

    /* Ensure we have the single table */
    if (!ctx->metadata->tables->count) {
        ctx->metadata->tables->count = 1;
        ctx->metadata->tables->tables = calloc(1, sizeof(fmp_table_t));
        ctx->metadata->tables->tables[0].index = 1;
        snprintf(ctx->metadata->tables->tables[0].utf8_name,
                sizeof(ctx->metadata->tables->tables[0].utf8_name),
                "%s", ctx->file->filename);

        /* Strip off extension */
        size_t len = strlen(ctx->metadata->tables->tables[0].utf8_name);
        for (int i = len - 1; i > 0; i--) {
            if (ctx->metadata->tables->tables[0].utf8_name[i] == '.') {
                ctx->metadata->tables->tables[0].utf8_name[i] = '\0';
                break;
            }
        }

        ensure_columns_capacity(ctx->metadata, 1);
    }

    /* Handle columns for the single table */
    if (table_path_match_start2(chunk, 3, 3, 5)) {
        fmp_data_t *column_path = chunk->path[chunk->path_level - 1];
        size_t column_index = path_value(chunk, column_path);
        handle_column(chunk, ctx, 1, column_index);
    }

    return CHUNK_NEXT;
}

static chunk_status_t handle_chunk_discover_metadata(fmp_chunk_t *chunk, void *ctxp) {
    fmp_discover_metadata_ctx_t *ctx = (fmp_discover_metadata_ctx_t *)ctxp;

    if (chunk->version_num >= 7) {
        return handle_chunk_discover_v7(chunk, ctx);
    } else {
        return handle_chunk_discover_v3(chunk, ctx);
    }
}

fmp_metadata_t *fmp_discover_all_metadata(fmp_file_t *file, fmp_error_t *errorCode) {
    fmp_metadata_t *metadata = calloc(1, sizeof(fmp_metadata_t));
    metadata->tables = calloc(1, sizeof(fmp_table_array_t));

    fmp_discover_metadata_ctx_t ctx = {
        .file = file,
        .metadata = metadata
    };

    fmp_error_t retval = process_blocks(file, NULL, handle_chunk_discover_metadata, &ctx);

    /* Compact tables array */
    int j = 0;
    for (int i = 0; i < metadata->tables->count; i++) {
        if (metadata->tables->tables[i].index) {
            if (i != j) {
                memmove(&metadata->tables->tables[j], &metadata->tables->tables[i], sizeof(fmp_table_t));

                /* Also move the corresponding columns */
                if (i + 1 < metadata->columns_capacity && metadata->columns[i + 1]) {
                    if (j + 1 >= metadata->columns_capacity) {
                        ensure_columns_capacity(metadata, j + 1);
                    }
                    metadata->columns[j + 1] = metadata->columns[i + 1];
                    if (i != j) {
                        metadata->columns[i + 1] = NULL;
                    }
                }
            }
            j++;
        }
    }
    metadata->tables->count = j;

    /* Compact columns arrays */
    for (size_t t = 1; t <= metadata->tables->count && t < metadata->columns_capacity; t++) {
        if (metadata->columns[t]) {
            fmp_column_array_t *columns = metadata->columns[t];
            j = 0;
            for (int i = 0; i < columns->count; i++) {
                if (columns->columns[i].index) {
                    if (i != j) {
                        memmove(&columns->columns[j], &columns->columns[i], sizeof(fmp_column_t));
                    }
                    j++;
                }
            }
            columns->count = j;
        }
    }

    if (errorCode)
        *errorCode = retval;

    if (retval != FMP_OK) {
        fmp_free_metadata(metadata);
        return NULL;
    }

    return metadata;
}

void fmp_free_metadata(fmp_metadata_t *metadata) {
    if (!metadata)
        return;

    if (metadata->tables) {
        free(metadata->tables->tables);
        free(metadata->tables);
    }

    if (metadata->columns) {
        for (size_t i = 0; i < metadata->columns_capacity; i++) {
            if (metadata->columns[i]) {
                free(metadata->columns[i]->columns);
                free(metadata->columns[i]);
            }
        }
        free(metadata->columns);
    }

    free(metadata);
}
