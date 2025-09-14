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

typedef struct table_read_state_s {
    size_t current_row;
    size_t last_row;
    size_t last_column;
    unsigned char *long_string_buf;
    size_t long_string_len;
    size_t long_string_used;
    fmp_column_array_t *columns;
} table_read_state_t;

typedef struct fmp_read_all_values_ctx_s {
    fmp_file_t *file;
    fmp_metadata_t *metadata;
    fmp_table_value_handler handle_value;
    void *user_ctx;
    table_read_state_t *table_states;  /* Array of states, one per table */
    size_t table_states_capacity;
} fmp_read_all_values_ctx_t;

static void ensure_table_state(fmp_read_all_values_ctx_t *ctx, size_t table_index) {
    if (table_index >= ctx->table_states_capacity) {
        size_t new_capacity = table_index + 128;
        ctx->table_states = realloc(ctx->table_states, new_capacity * sizeof(table_read_state_t));
        memset(&ctx->table_states[ctx->table_states_capacity], 0,
               (new_capacity - ctx->table_states_capacity) * sizeof(table_read_state_t));
        ctx->table_states_capacity = new_capacity;
    }

    if (!ctx->table_states[table_index].columns &&
        table_index < ctx->metadata->columns_capacity &&
        ctx->metadata->columns[table_index]) {
        ctx->table_states[table_index].columns = ctx->metadata->columns[table_index];
    }
}

static int path_is_table_data(fmp_chunk_t *chunk) {
    return table_path_match_start1(chunk, 2, 5);
}

static int path_row(fmp_chunk_t *chunk) {
    if (chunk->version_num < 7)
        return path_value(chunk, chunk->path[1]);
    return path_value(chunk, chunk->path[2]);
}

static int path_is_long_string(fmp_chunk_t *chunk, table_read_state_t *state) {
    if (!table_path_match_start1(chunk, 3, 5))
        return 0;
    uint64_t column_index = path_value(chunk, chunk->path[chunk->version_num < 7 ? 2 : 3]);
    if (state->last_column == 0 || column_index < state->last_column) {
        return path_row(chunk) > state->last_row;
    }
    return path_row(chunk) == state->last_row;
}

static chunk_status_t process_value_for_table(fmp_chunk_t *chunk, fmp_read_all_values_ctx_t *ctx,
                                              size_t table_index, table_read_state_t *state) {
    fmp_column_t *column = NULL;
    int long_string = 0;
    size_t column_index = 0;

    if (!state->columns)
        return CHUNK_NEXT;

    if (path_is_long_string(chunk, state)) {
        if (chunk->type == FMP_CHUNK_FIELD_REF_SIMPLE && chunk->ref_simple == 0)
            return CHUNK_NEXT; /* Rich-text formatting */
        long_string = 1;
        column_index = path_value(chunk, chunk->path[chunk->path_level-1]);
    } else if (path_is_table_data(chunk)) {
        if (chunk->type == FMP_CHUNK_FIELD_REF_SIMPLE && chunk->ref_simple <= state->columns->count
                && chunk->ref_simple != 252 /* Special metadata value? */) {
            column_index = chunk->ref_simple;
        } else if (chunk->type == FMP_CHUNK_DATA_SEGMENT && chunk->segment_index <= state->columns->count) {
            column_index = chunk->segment_index;
        }
    }

    if (column_index == 0 || column_index > state->columns->count)
        return CHUNK_NEXT;

    /* Find the column with this index */
    column = NULL;
    for (size_t i = 0; i < state->columns->count; i++) {
        if (state->columns->columns[i].index == column_index) {
            column = &state->columns->columns[i];
            break;
        }
    }

    if (!column)
        return CHUNK_NEXT;

    /* Handle long string continuation */
    if (column->index != state->last_column && state->long_string_used) {
        if (ctx->handle_value && state->last_column > 0) {
            char utf8_value[state->long_string_used*4+1];
            fmp_column_t *last_col = NULL;
            for (size_t i = 0; i < state->columns->count; i++) {
                if (state->columns->columns[i].index == state->last_column) {
                    last_col = &state->columns->columns[i];
                    break;
                }
            }
            if (last_col) {
                convert(ctx->file->converter, ctx->file->xor_mask,
                        utf8_value, sizeof(utf8_value), state->long_string_buf, state->long_string_used);
                if (ctx->handle_value(table_index, state->current_row, last_col,
                        utf8_value, ctx->user_ctx) == FMP_HANDLER_ABORT)
                    return CHUNK_ABORT;
            }
        }
        state->long_string_used = 0;
    }

    /* Check for new row */
    if (path_row(chunk) != state->last_row || column->index < state->last_column) {
        state->current_row++;
    }

    if (long_string) {
        /* Accumulate long string data */
        size_t old_size = state->long_string_used;
        state->long_string_used += chunk->data.len;
        if (state->long_string_used > state->long_string_len) {
            state->long_string_len = state->long_string_used + 1024;
            state->long_string_buf = realloc(state->long_string_buf, state->long_string_len);
        }
        memcpy(&state->long_string_buf[old_size], chunk->data.bytes, chunk->data.len);
    } else {
        /* Handle regular value */
        char utf8_value[chunk->data.len*4+1];
        convert(ctx->file->converter, ctx->file->xor_mask,
                utf8_value, sizeof(utf8_value), chunk->data.bytes, chunk->data.len);
        if (ctx->handle_value) {
            if (ctx->handle_value(table_index, state->current_row, column,
                    utf8_value, ctx->user_ctx) == FMP_HANDLER_ABORT)
                return CHUNK_ABORT;
        }
    }

    state->last_row = path_row(chunk);
    state->last_column = column->index;

    return CHUNK_NEXT;
}

static chunk_status_t handle_chunk_read_all_values_v7(fmp_chunk_t *chunk, fmp_read_all_values_ctx_t *ctx) {
    /* Determine which table this chunk belongs to */
    size_t path0 = path_value(chunk, chunk->path[0]);

    if (path0 < 128) {
        /* Not table data */
        return CHUNK_NEXT;
    }

    size_t table_index = path0 - 128;

    /* Find the actual table with this index */
    fmp_table_t *table = NULL;
    for (size_t i = 0; i < ctx->metadata->tables->count; i++) {
        if (ctx->metadata->tables->tables[i].index == table_index) {
            table = &ctx->metadata->tables->tables[i];
            break;
        }
    }

    if (!table || table->skip) {
        return CHUNK_NEXT;
    }

    ensure_table_state(ctx, table_index);
    table_read_state_t *state = &ctx->table_states[table_index];

    if (chunk->type != FMP_CHUNK_FIELD_REF_SIMPLE && chunk->type != FMP_CHUNK_DATA_SEGMENT)
        return CHUNK_NEXT;

    return process_value_for_table(chunk, ctx, table_index, state);
}

static chunk_status_t handle_chunk_read_all_values_v3(fmp_chunk_t *chunk, fmp_read_all_values_ctx_t *ctx) {
    /* For v3-v6, there's only one table at index 1 */
    if (path_value(chunk, chunk->path[0]) > 3)
        return CHUNK_NEXT;

    ensure_table_state(ctx, 1);
    table_read_state_t *state = &ctx->table_states[1];

    if (chunk->type != FMP_CHUNK_FIELD_REF_SIMPLE && chunk->type != FMP_CHUNK_DATA_SEGMENT)
        return CHUNK_NEXT;

    return process_value_for_table(chunk, ctx, 1, state);
}

static chunk_status_t handle_chunk_read_all_values(fmp_chunk_t *chunk, void *ctxp) {
    fmp_read_all_values_ctx_t *ctx = (fmp_read_all_values_ctx_t *)ctxp;

    if (chunk->version_num >= 7) {
        return handle_chunk_read_all_values_v7(chunk, ctx);
    } else {
        return handle_chunk_read_all_values_v3(chunk, ctx);
    }
}

fmp_error_t fmp_read_all_values(fmp_file_t *file, fmp_metadata_t *metadata,
                                fmp_table_value_handler handle_value, void *user_ctx) {
    fmp_read_all_values_ctx_t ctx = {
        .file = file,
        .metadata = metadata,
        .handle_value = handle_value,
        .user_ctx = user_ctx,
        .table_states = NULL,
        .table_states_capacity = 0
    };

    fmp_error_t retval = process_blocks(file, NULL, handle_chunk_read_all_values, &ctx);

    /* Clean up table states */
    if (ctx.table_states) {
        for (size_t i = 0; i < ctx.table_states_capacity; i++) {
            if (ctx.table_states[i].long_string_buf) {
                /* Flush any pending long string */
                if (ctx.table_states[i].long_string_used && ctx.handle_value) {
                    char utf8_value[ctx.table_states[i].long_string_used*4+1];
                    fmp_column_t *last_col = NULL;
                    if (ctx.table_states[i].columns) {
                        for (size_t j = 0; j < ctx.table_states[i].columns->count; j++) {
                            if (ctx.table_states[i].columns->columns[j].index == ctx.table_states[i].last_column) {
                                last_col = &ctx.table_states[i].columns->columns[j];
                                break;
                            }
                        }
                    }
                    if (last_col) {
                        convert(file->converter, file->xor_mask,
                                utf8_value, sizeof(utf8_value),
                                ctx.table_states[i].long_string_buf,
                                ctx.table_states[i].long_string_used);
                        handle_value(i, ctx.table_states[i].current_row, last_col,
                                    utf8_value, user_ctx);
                    }
                }
                free(ctx.table_states[i].long_string_buf);
            }
        }
        free(ctx.table_states);
    }

    return retval;
}
