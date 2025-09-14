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

typedef struct fmp_sqlite_ctx_s {
    sqlite3 *db;
    sqlite3_stmt *insert_stmt;
    char *table_name;
    int last_row;
} fmp_sqlite_ctx_t;

fmp_handler_status_t handle_value(int row, fmp_column_t *column, const char *value, void *ctxp) {
    fmp_sqlite_ctx_t *ctx = (fmp_sqlite_ctx_t *)ctxp;
    if (ctx->last_row != row && ctx->last_row > 0) {
        int rc = sqlite3_step(ctx->insert_stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "Error inserting data into SQLite table: %s\n", sqlite3_errmsg(ctx->db));
            return FMP_HANDLER_ABORT;
        }
        rc = sqlite3_reset(ctx->insert_stmt);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error resetting INSERT statement: %s\n", sqlite3_errmsg(ctx->db));
            return FMP_HANDLER_ABORT;
        }
        sqlite3_clear_bindings(ctx->insert_stmt);
    }
    int rc = sqlite3_bind_text(ctx->insert_stmt, column->index, value, strlen(value), SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error binding parameter: %s\n", sqlite3_errmsg(ctx->db));
        return FMP_HANDLER_ABORT;
    }
    ctx->last_row = row;
    return FMP_HANDLER_OK;
}

static size_t create_query_length(fmp_table_t *table, fmp_column_array_t *columns) {
    size_t len = 0;
    len += sizeof("CREATE TABLE \"\" ();");
    len += strlen(table->utf8_name);
    for (int j=0; j<columns->count; j++) {
        len += sizeof("\"\" TEXT")-1;
        len += strlen(columns->columns[j].utf8_name);
        if (j < columns->count) {
            len += sizeof(", ")-1;
        }
    }
    return len;
}

static size_t insert_query_length(fmp_table_t *table, fmp_column_array_t *columns) {
    size_t len = 0;
    len += sizeof("INSERT INTO \"\" () VALUES ();");
    len += strlen(table->utf8_name);
    for (int j=0; j<columns->count; j++) {
        len += sizeof("\"\"")-1;
        len += strlen(columns->columns[j].utf8_name);
        len += sizeof("\"\"")-1;
        len += sizeof("?NNNNN")-1;
        if (j < columns->count) {
            len += sizeof(", ")-1;
            len += sizeof(", ")-1;
        }
    }
    return len;
}

/* Cache management functions */
static int use_cache = 1;  /* Global flag to control cache usage */

static char* get_cache_filename(const char* fmp_path) {
    struct stat st;
    if (stat(fmp_path, &st) != 0) {
        return NULL;
    }

    /* Create cache filename based on input file */
    char* cache_file = malloc(strlen(fmp_path) + 32);
    if (!cache_file) return NULL;

    /* Use size and mtime to create unique cache name */
    snprintf(cache_file, strlen(fmp_path) + 32, "%s.cache_%lld_%ld.json",
             fmp_path, (long long)st.st_size, (long)st.st_mtime);

    return cache_file;
}

static int is_cache_valid(const char* cache_file, const char* fmp_path) {
    if (!use_cache) return 0;

    struct stat cache_st, fmp_st;

    /* Check if cache file exists */
    if (stat(cache_file, &cache_st) != 0) {
        return 0;  /* Cache doesn't exist */
    }

    /* Check if FMP file exists and get its stats */
    if (stat(fmp_path, &fmp_st) != 0) {
        return 0;  /* FMP file doesn't exist? */
    }

    /* Cache is valid if it's newer than the FMP file */
    return cache_st.st_mtime >= fmp_st.st_mtime;
}

static int save_metadata_cache(const char* cache_file,
                               fmp_table_array_t* tables,
                               fmp_column_array_t** all_columns) {
    FILE* fp = fopen(cache_file, "w");
    if (!fp) {
        fprintf(stderr, "Warning: Could not create cache file %s\n", cache_file);
        return -1;
    }

    /* Write simple JSON format */
    fprintf(fp, "{\n");
    fprintf(fp, "  \"version\": 1,\n");
    fprintf(fp, "  \"created\": %ld,\n", (long)time(NULL));
    fprintf(fp, "  \"tables\": [\n");

    for (int i = 0; i < tables->count; i++) {
        fmp_table_t* table = &tables->tables[i];
        fmp_column_array_t* columns = all_columns[i];

        fprintf(fp, "    {\n");
        fprintf(fp, "      \"index\": %d,\n", table->index);
        fprintf(fp, "      \"skip\": %d,\n", table->skip);
        fprintf(fp, "      \"name\": \"%s\",\n", table->utf8_name);
        fprintf(fp, "      \"columns\": [\n");

        for (int j = 0; j < columns->count; j++) {
            fmp_column_t* col = &columns->columns[j];
            fprintf(fp, "        {\"index\": %d, \"type\": %d, \"collation\": %d, \"name\": \"%s\"}",
                    col->index, col->type, col->collation, col->utf8_name);
            if (j < columns->count - 1) fprintf(fp, ",");
            fprintf(fp, "\n");
        }

        fprintf(fp, "      ]\n");
        fprintf(fp, "    }");
        if (i < tables->count - 1) fprintf(fp, ",");
        fprintf(fp, "\n");
    }

    fprintf(fp, "  ]\n");
    fprintf(fp, "}\n");
    fclose(fp);

    fprintf(stderr, "Cache saved to %s\n", cache_file);
    return 0;
}

static int load_metadata_cache(const char* cache_file,
                               fmp_table_array_t** tables_out,
                               fmp_column_array_t*** all_columns_out) {
    FILE* fp = fopen(cache_file, "r");
    if (!fp) {
        return -1;
    }

    /* Get file size */
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    /* Read entire file into buffer */
    char* buffer = malloc(file_size + 1);
    if (!buffer) {
        fclose(fp);
        return -1;
    }

    if (fread(buffer, 1, file_size, fp) != file_size) {
        free(buffer);
        fclose(fp);
        return -1;
    }
    buffer[file_size] = '\0';
    fclose(fp);

    /* Simple JSON parsing - very basic, assumes well-formed cache */
    char* p = buffer;

    /* Count tables - look for table objects, not all index fields */
    int table_count = 0;
    char* scan = p;
    while ((scan = strstr(scan, "\"name\": \""))) {
        /* Check if this is a table name (has columns array after it) */
        char* columns_check = strstr(scan, "\"columns\": [");
        if (columns_check && columns_check - scan < 100) {  /* Close enough to be same table */
            table_count++;
        }
        scan++;
    }

    /* Allocate arrays */
    fmp_table_array_t* tables = calloc(1, sizeof(fmp_table_array_t));
    tables->count = 0;
    tables->tables = calloc(table_count, sizeof(fmp_table_t));

    fmp_column_array_t** all_columns = calloc(table_count, sizeof(fmp_column_array_t*));

    /* Parse tables - simple approach */
    char* table_start = strstr(p, "\"tables\": [");
    if (!table_start) {
        free(buffer);
        free(tables->tables);
        free(tables);
        free(all_columns);
        return -1;
    }

    int table_idx = 0;
    char* current = table_start;

    /* Find each table object */
    current = strstr(current, "{");
    while (current && table_idx < table_count) {
        /* Look for table-level index field */
        char* table_index = strstr(current, "\"index\":");
        if (!table_index) break;

        fmp_table_t* table = &tables->tables[table_idx];

        /* Parse table fields */
        sscanf(table_index, "\"index\": %d", &table->index);

        char* skip_field = strstr(current, "\"skip\":");
        if (skip_field && skip_field < strstr(current, "\"columns\":")) {
            sscanf(skip_field, "\"skip\": %d", &table->skip);
        }

        char* name_field = strstr(current, "\"name\": \"");
        if (name_field && name_field < strstr(current, "\"columns\":")) {
            name_field += strlen("\"name\": \"");
            char* name_end = strchr(name_field, '"');
            if (name_end) {
                size_t name_len = name_end - name_field;
                if (name_len > sizeof(table->utf8_name) - 1) {
                    name_len = sizeof(table->utf8_name) - 1;
                }
                strncpy(table->utf8_name, name_field, name_len);
                table->utf8_name[name_len] = '\0';
            }
        }

        /* Count columns for this table */
        char* columns_start = strstr(current, "\"columns\": [");
        char* columns_end = strstr(columns_start, "]");
        int col_count = 0;
        if (columns_start && columns_end) {
            char* col_scan = columns_start;
            while (col_scan < columns_end) {
                col_scan = strstr(col_scan, "{\"index\":");
                if (!col_scan || col_scan >= columns_end) break;
                col_count++;
                col_scan++;
            }
        }

        /* Allocate columns array */
        all_columns[table_idx] = calloc(1, sizeof(fmp_column_array_t));
        all_columns[table_idx]->count = col_count;
        all_columns[table_idx]->columns = calloc(col_count, sizeof(fmp_column_t));

        /* Parse columns */
        char* col_current = columns_start;
        for (int j = 0; j < col_count; j++) {
            col_current = strstr(col_current, "{");
            if (!col_current || col_current > columns_end) break;

            fmp_column_t* col = &all_columns[table_idx]->columns[j];

            char* idx = strstr(col_current, "\"index\":");
            if (idx && idx < columns_end) {
                sscanf(idx, "\"index\": %d", &col->index);
            }

            char* type = strstr(col_current, "\"type\":");
            if (type && type < columns_end) {
                sscanf(type, "\"type\": %d", (int*)&col->type);
            }

            char* collation = strstr(col_current, "\"collation\":");
            if (collation && collation < columns_end) {
                sscanf(collation, "\"collation\": %d", (int*)&col->collation);
            }

            char* col_name = strstr(col_current, "\"name\": \"");
            if (col_name && col_name < columns_end) {
                col_name += strlen("\"name\": \"");
                char* col_name_end = strchr(col_name, '"');
                if (col_name_end) {
                    size_t col_name_len = col_name_end - col_name;
                    if (col_name_len > sizeof(col->utf8_name) - 1) {
                        col_name_len = sizeof(col->utf8_name) - 1;
                    }
                    strncpy(col->utf8_name, col_name, col_name_len);
                    col->utf8_name[col_name_len] = '\0';
                }
            }

            col_current = strchr(col_current, '}');
            if (!col_current) break;
            col_current++;
        }

        table_idx++;
        tables->count++;

        /* Move to next table object - find the closing brace after columns */
        current = columns_end;
        if (current) {
            current = strstr(current, "},");  /* End of this table object */
            if (current) {
                current = strstr(current, "{");  /* Start of next table object */
            }
        }
    }

    free(buffer);

    *tables_out = tables;
    *all_columns_out = all_columns;

    fprintf(stderr, "Cache loaded from %s (%zu tables)\n", cache_file, tables->count);
    return 0;
}

int main(int argc, char *argv[]) {
    /* Parse command line options */
    int arg_offset = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--no-cache") == 0) {
            use_cache = 0;
            arg_offset++;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [options] input.fmp output.db\n", argv[0]);
            printf("Options:\n");
            printf("  --no-cache    Skip metadata cache, force fresh scan\n");
            printf("  --help, -h    Show this help message\n");
            return 0;
        }
    }

    if (argc - arg_offset != 3) {
        print_usage_and_exit(argc, argv);
    }

    const char* input_file = argv[1 + arg_offset];
    const char* output_file = argv[2 + arg_offset];

    sqlite3 *db = NULL;
    char *zErrMsg = NULL;
    fmp_error_t error = FMP_OK;

    fmp_file_t *file = fmp_open_file(input_file, &error);
    if (!file) {
        fprintf(stderr, "Error code: %d\n", error);
        return 1;
    }

    fmp_table_array_t *tables = NULL;
    fmp_column_array_t **all_columns = NULL;
    int cache_loaded = 0;

    /* Try to use cache if enabled */
    char* cache_file = get_cache_filename(input_file);
    if (cache_file && is_cache_valid(cache_file, input_file)) {
        if (load_metadata_cache(cache_file, &tables, &all_columns) == 0) {
            cache_loaded = 1;
            fprintf(stderr, "Using cached metadata, skipping table/column discovery\n");
        }
    }

    /* If cache not loaded, do the discovery */
    if (!cache_loaded) {
        fprintf(stderr, "Discovering tables...\n");
        tables = fmp_list_tables(file, &error);
        if (!tables) {
            fprintf(stderr, "Error code: %d\n", error);
            if (cache_file) free(cache_file);
            return 1;
        }

        /* Discover all columns and store them */
        all_columns = calloc(tables->count, sizeof(fmp_column_array_t*));
        fprintf(stderr, "Discovering columns for %zu tables...\n", tables->count);
        for (int i = 0; i < tables->count; i++) {
            fmp_table_t *table = &tables->tables[i];
            all_columns[i] = fmp_list_columns(file, table, &error);
            if (!all_columns[i]) {
                fprintf(stderr, "Error getting columns for table %s: %d\n", table->utf8_name, error);
                /* Continue anyway, some tables might work */
                all_columns[i] = calloc(1, sizeof(fmp_column_array_t));
                all_columns[i]->count = 0;
                all_columns[i]->columns = NULL;
            }
        }

        /* Save to cache if we have a cache filename */
        if (cache_file && use_cache) {
            save_metadata_cache(cache_file, tables, all_columns);
        }
    }

    int rc = sqlite3_open_v2(output_file, &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error opening SQLite file\n");
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA journal_mode = OFF;\n", NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error setting journal_mode = OFF\n");
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA synchronous = 0;\n", NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error setting synchronous = 0\n");
        return 1;
    }

    char *create_query = NULL;
    char *insert_query = NULL;

    for (int i=0; i<tables->count; i++) {
        fmp_table_t *table = &tables->tables[i];
        fmp_column_array_t *columns = all_columns[i];
        if (!columns || columns->count == 0) {
            fprintf(stderr, "Skipping table %s (no columns)\n", table->utf8_name);
            continue;
        }
        size_t create_query_len = create_query_length(table, columns);
        size_t insert_query_len = insert_query_length(table, columns);
        create_query = realloc(create_query, create_query_len);
        insert_query = realloc(insert_query, insert_query_len);

        char *p = create_query;
        char *q = insert_query;
        p += snprintf(p, create_query_len, "CREATE TABLE \"%s\" (", table->utf8_name);
        q += snprintf(q, insert_query_len, "INSERT INTO \"%s\" (", table->utf8_name);
        for (int j=0; j<columns->count; j++) {
            fmp_column_t *column = &columns->columns[j];
            char *colname = strdup(column->utf8_name);
            size_t colname_len = strlen(colname);
            for (int k=0; k<colname_len; k++) {
                if (colname[k] == ' ')
                    colname[k] = '_';
            }
            p += snprintf(p, create_query_len - (p - create_query), "\"%s\" TEXT", colname);
            q += snprintf(q, insert_query_len - (q - insert_query), "\"%s\"", colname);
            if (j < columns->count - 1) {
                p += snprintf(p, create_query_len - (p - create_query), ", ");
                q += snprintf(q, insert_query_len - (q - insert_query), ", ");
            }
            free(colname);
        }
        p += snprintf(p, create_query_len - (p - create_query), ");");
        q += snprintf(q, insert_query_len - (q - insert_query), ") VALUES (");
        for (int j=0; j<columns->count; j++) {
            fmp_column_t *column = &columns->columns[j];
            q += snprintf(q, insert_query_len - (q - insert_query), "?%d", column->index);
            if (j < columns->count - 1)
                q += snprintf(q, insert_query_len - (q - insert_query), ", ");
        }
        q += snprintf(q, insert_query_len - (q - insert_query), ");");

        fprintf(stderr, "CREATE TABLE \"%s\"\n", table->utf8_name);
        rc = sqlite3_exec(db, create_query, NULL, NULL, &zErrMsg);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error creating SQL table: %s\n", zErrMsg);
            fprintf(stderr, "Statement was: %s\n", create_query);
            return 1;
        }

        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(db, insert_query, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error preparing SQL statement: %d\n", rc);
            fprintf(stderr, "Statement was: %s\n", insert_query);
            return 1;
        }

        fmp_sqlite_ctx_t ctx = { .db = db, .table_name = table->utf8_name, .insert_stmt = stmt };
        fmp_read_values(file, table, &handle_value, &ctx);
        if (ctx.last_row) {
            int rc = sqlite3_step(stmt);
            if (rc != SQLITE_DONE) {
                fprintf(stderr, "Error inserting data into SQLite table: %s\n", sqlite3_errmsg(db));
                return 1;
            }
        }
        sqlite3_finalize(stmt);
        /* Don't free columns here anymore - we'll free them all at the end */
    }

    free(create_query);
    free(insert_query);

    /* Free all columns arrays */
    if (all_columns) {
        for (int i = 0; i < tables->count; i++) {
            if (all_columns[i]) {
                fmp_free_columns(all_columns[i]);
            }
        }
        free(all_columns);
    }

    fmp_free_tables(tables);
    sqlite3_close(db);
    fmp_close_file(file);

    if (cache_file) free(cache_file);

    return 0;
}
