/* FMP Tools - Optimized SQLite export with single-scan data reading
 * Copyright (c) 2020 Evan Miller (except where otherwise noted)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../fmp.h"
#include "usage.h"

typedef struct table_context_s {
    sqlite3_stmt *insert_stmt;
    int *column_index_map;
    int max_column_index;
    int last_row;
    fmp_table_t *table;
    fmp_column_array_t *columns;
} table_context_t;

typedef struct fmp_sqlite_all_ctx_s {
    sqlite3 *db;
    fmp_metadata_t *metadata;
    table_context_t *table_contexts;  /* Array indexed by table index */
    size_t table_contexts_capacity;
} fmp_sqlite_all_ctx_t;

/* Handler for all table values in single scan */
fmp_handler_status_t handle_all_values(int table_index, int row, fmp_column_t *column,
                                       const char *value, void *ctxp) {
    fmp_sqlite_all_ctx_t *ctx = (fmp_sqlite_all_ctx_t *)ctxp;

    if (table_index >= ctx->table_contexts_capacity || !ctx->table_contexts[table_index].insert_stmt) {
        return FMP_HANDLER_OK;  /* Skip tables we're not processing */
    }

    table_context_t *tctx = &ctx->table_contexts[table_index];

    /* Check if we need to execute the previous row */
    if (tctx->last_row != row && tctx->last_row > 0) {
        int rc = sqlite3_step(tctx->insert_stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "Error inserting data into table %s: %s\n",
                    tctx->table ? tctx->table->utf8_name : "(unknown)", sqlite3_errmsg(ctx->db));
            return FMP_HANDLER_ABORT;
        }
        rc = sqlite3_reset(tctx->insert_stmt);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error resetting INSERT statement for table %s: %s\n",
                    tctx->table ? tctx->table->utf8_name : "(unknown)", sqlite3_errmsg(ctx->db));
            return FMP_HANDLER_ABORT;
        }
        sqlite3_clear_bindings(tctx->insert_stmt);
    }

    /* Map column index to parameter position */
    int param_pos = 0;
    if (tctx->column_index_map && column->index <= tctx->max_column_index) {
        param_pos = tctx->column_index_map[column->index];
    }

    if (param_pos == 0) {
        /* Skip columns not in schema */
        return FMP_HANDLER_OK;
    }

    int rc = sqlite3_bind_text(tctx->insert_stmt, param_pos, value, strlen(value), SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error binding parameter for table %s at position %d: %s\n",
                tctx->table ? tctx->table->utf8_name : "(unknown)", param_pos, sqlite3_errmsg(ctx->db));
        return FMP_HANDLER_ABORT;
    }

    tctx->last_row = row;
    return FMP_HANDLER_OK;
}

static char *create_table_query(fmp_table_t *table, fmp_column_array_t *columns) {
    size_t len = 100 + strlen(table->utf8_name);
    for (int i = 0; i < columns->count; i++) {
        len += strlen(columns->columns[i].utf8_name) + 20;
    }

    char *query = malloc(len);
    char *p = query;

    p += sprintf(p, "CREATE TABLE \"%s\" (", table->utf8_name);
    for (int i = 0; i < columns->count; i++) {
        if (i > 0) p += sprintf(p, ", ");
        p += sprintf(p, "\"%s\" TEXT", columns->columns[i].utf8_name);
    }
    p += sprintf(p, ")");

    return query;
}

static char *create_insert_query(fmp_table_t *table, fmp_column_array_t *columns) {
    size_t len = 100 + strlen(table->utf8_name);
    for (int i = 0; i < columns->count; i++) {
        len += strlen(columns->columns[i].utf8_name) + 10;
    }

    char *query = malloc(len);
    char *p = query;

    p += sprintf(p, "INSERT INTO \"%s\" (", table->utf8_name);
    for (int i = 0; i < columns->count; i++) {
        if (i > 0) p += sprintf(p, ", ");
        p += sprintf(p, "\"%s\"", columns->columns[i].utf8_name);
    }
    p += sprintf(p, ") VALUES (");
    for (int i = 0; i < columns->count; i++) {
        if (i > 0) p += sprintf(p, ", ");
        p += sprintf(p, "?");
    }
    p += sprintf(p, ")");

    return query;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s input.fmp output.db\n", argv[0]);
        return 1;
    }

    const char *input_file = argv[1];
    const char *output_file = argv[2];

    fmp_error_t error = FMP_OK;

    /* Open FileMaker file */
    fmp_file_t *file = fmp_open_file(input_file, &error);
    if (!file) {
        fprintf(stderr, "Error opening file: %d\n", error);
        return 1;
    }

    /* Discover all metadata in single scan */
    fprintf(stderr, "Discovering all tables and columns...\n");
    fmp_metadata_t *metadata = fmp_discover_all_metadata(file, &error);
    if (!metadata) {
        fprintf(stderr, "Error discovering metadata: %d\n", error);
        fmp_close_file(file);
        return 1;
    }
    fprintf(stderr, "Found %zu tables\n", metadata->tables->count);

    /* Open SQLite database */
    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(output_file, &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error opening SQLite file: %s\n", sqlite3_errmsg(db));
        fmp_free_metadata(metadata);
        fmp_close_file(file);
        return 1;
    }

    /* Set SQLite pragmas for speed */
    sqlite3_exec(db, "PRAGMA journal_mode = OFF;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous = 0;", NULL, NULL, NULL);

    /* Prepare context for all tables */
    fmp_sqlite_all_ctx_t ctx = {
        .db = db,
        .metadata = metadata,
        .table_contexts = NULL,
        .table_contexts_capacity = 0
    };

    /* Find max table index */
    size_t max_table_index = 0;
    for (size_t i = 0; i < metadata->tables->count; i++) {
        if (metadata->tables->tables[i].index > max_table_index) {
            max_table_index = metadata->tables->tables[i].index;
        }
    }

    /* Allocate table contexts */
    ctx.table_contexts_capacity = max_table_index + 1;
    ctx.table_contexts = calloc(ctx.table_contexts_capacity, sizeof(table_context_t));

    /* Create tables and prepare statements */
    fprintf(stderr, "Creating tables and preparing statements...\n");
    for (size_t i = 0; i < metadata->tables->count; i++) {
        fmp_table_t *table = &metadata->tables->tables[i];

        /* Get columns for this table */
        fmp_column_array_t *columns = NULL;
        if (table->index < metadata->columns_capacity) {
            columns = metadata->columns[table->index];
        }

        if (!columns || columns->count == 0) {
            fprintf(stderr, "Skipping table %s (no columns)\n", table->utf8_name);
            continue;
        }

        /* Create table */
        char *create_query = create_table_query(table, columns);
        fprintf(stderr, "Creating table %s...\n", table->utf8_name);
        rc = sqlite3_exec(db, create_query, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error creating table: %s\n", sqlite3_errmsg(db));
            free(create_query);
            continue;
        }
        free(create_query);

        /* Prepare insert statement */
        char *insert_query = create_insert_query(table, columns);
        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(db, insert_query, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error preparing statement: %s\n", sqlite3_errmsg(db));
            free(insert_query);
            continue;
        }
        free(insert_query);

        /* Create column index map */
        int max_idx = 0;
        for (int j = 0; j < columns->count; j++) {
            if (columns->columns[j].index > max_idx) {
                max_idx = columns->columns[j].index;
            }
        }

        int *col_map = calloc(max_idx + 1, sizeof(int));
        for (int j = 0; j < columns->count; j++) {
            col_map[columns->columns[j].index] = j + 1;  /* SQLite params are 1-based */
        }

        /* Store context for this table */
        ctx.table_contexts[table->index] = (table_context_t){
            .insert_stmt = stmt,
            .column_index_map = col_map,
            .max_column_index = max_idx,
            .last_row = 0,
            .table = table,
            .columns = columns
        };
    }

    /* Read all data in a single scan */
    fprintf(stderr, "Reading all table data in single scan...\n");
    error = fmp_read_all_values(file, metadata, handle_all_values, &ctx);
    if (error != FMP_OK) {
        fprintf(stderr, "Error reading values: %d\n", error);
    }

    /* Execute any pending inserts */
    fprintf(stderr, "Finalizing inserts...\n");
    for (size_t i = 0; i < ctx.table_contexts_capacity; i++) {
        if (ctx.table_contexts[i].insert_stmt && ctx.table_contexts[i].last_row > 0) {
            rc = sqlite3_step(ctx.table_contexts[i].insert_stmt);
            if (rc != SQLITE_DONE) {
                fprintf(stderr, "Error executing final insert for table: %s\n",
                        sqlite3_errmsg(db));
            }
        }
    }

    /* Clean up */
    fprintf(stderr, "Cleaning up...\n");
    for (size_t i = 0; i < ctx.table_contexts_capacity; i++) {
        if (ctx.table_contexts[i].insert_stmt) {
            sqlite3_finalize(ctx.table_contexts[i].insert_stmt);
        }
        if (ctx.table_contexts[i].column_index_map) {
            free(ctx.table_contexts[i].column_index_map);
        }
    }
    free(ctx.table_contexts);

    sqlite3_close(db);
    fmp_free_metadata(metadata);
    fmp_close_file(file);

    fprintf(stderr, "Done!\n");
    return 0;
}