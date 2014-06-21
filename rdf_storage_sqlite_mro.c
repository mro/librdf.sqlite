//
// rdf_storage_sqlite_mro.c
//
// Created by Marcus Rohrmoser on 19.05.14.
//
// Copyright (c) 2014, Marcus Rohrmoser mobile Software
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted
// provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions
// and the following disclaimer.
//
// 2. The software must not be used for military or intelligence or related purposes nor
// anything that's in conflict with human rights as declared in http://www.un.org/en/documents/udhr/ .
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
// IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

#include "rdf_storage_sqlite_mro.h"

const char *LIBRDF_STORAGE_SQLITE_MRO = "http://purl.mro.name/rdf/sqlite/mro";

#include <stdlib.h>
// #include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <redland.h>
#include <rdf_storage.h>
#include <sqlite3.h>

#pragma mark Basic Types & Constants

typedef enum {
    BOOL_NO = 0,
    BOOL_YES = 1
} boolean_t;

typedef uint32_t id_t;
static const id_t NULL_ID = 0;
static inline boolean_t isNULL_ID(const id_t x)
{
    return x == 0;
}


#define RET_ERROR 1
#define RET_OK 0
#define F_OK 0

/** C-String type for URIs. */
typedef unsigned char *str_uri_t;
/** C-String type for blank identifiers. */
typedef unsigned char *str_blank_t;
/** C-String type for literal values. */
typedef unsigned char *str_lit_val_t;
/** C-String type for (xml) language ids. */
typedef char *str_lang_t;

#define NULL_LANG ( (str_lang_t)"" )
#define isNULL_LANG(x) ( (x) == NULL || (x)[0] == '\0' )

#define NULL_URI ( (str_uri_t)"" )
#define isNULL_URI(x) ( (x) == NULL || (x)[0] == '\0' )

#define isNULL_BLANK(x) ( (x) == NULL || (x)[0] == '\0' )


/** index into synchronous_flags */
typedef enum {
    SYNC_UNKONWN = -1,
    SYNC_OFF = 0,
    SYNC_NORMAL = 1,
    SYNC_FULL = 2
} syncronous_flag_t;
static const char *const synchronous_flags[4] = {
    "off", "normal", "full", NULL
};


typedef struct legacy_query_t legacy_query_t;
struct legacy_query_t
{
    unsigned char *query;
    legacy_query_t *next;
};

typedef struct
{
    sqlite3 *db;

    const char *name;
    size_t name_len;
    boolean_t is_new;
    syncronous_flag_t synchronous;
    boolean_t in_transaction;

    boolean_t in_stream;
    legacy_query_t *in_stream_queries;

    // compiled statements, lazy init
    sqlite3_stmt *stmt_txn_start;
    sqlite3_stmt *stmt_txn_commit;
    sqlite3_stmt *stmt_txn_rollback;
    sqlite3_stmt *stmt_uri_so_set;
    sqlite3_stmt *stmt_uri_p_set;
    sqlite3_stmt *stmt_uri_t_set;
    sqlite3_stmt *stmt_uri_c_set;
    sqlite3_stmt *stmt_literal_set;
    sqlite3_stmt *stmt_blank_so_set;
    sqlite3_stmt *stmt_parts_get;
    sqlite3_stmt *stmt_spocs_get;
    sqlite3_stmt *stmt_spocs_set;

    sqlite3_stmt *stmt_size;
    sqlite3_stmt *stmt_find;
}
instance_t;


#pragma mark librdf convenience


#define LIBRDF_MALLOC(type, size) (type)malloc(size)
#define LIBRDF_CALLOC(type, size, count) (type)calloc(count, size)
#define LIBRDF_FREE(type, ptr) free( (type)ptr )
// #define LIBRDF_BAD_CAST(t, v) (t)(v)


static inline instance_t *get_instance(librdf_storage *storage)
{
    return (instance_t *)librdf_storage_get_instance(storage);
}


static inline librdf_world *get_world(librdf_storage *storage)
{
    return librdf_storage_get_world(storage);
}


static inline void free_hash(librdf_hash *hash)
{
    if( hash )
        librdf_free_hash(hash);
}


static inline librdf_node_type node_type(librdf_node *node)
{
    return node == NULL ? LIBRDF_NODE_TYPE_UNKNOWN : librdf_node_get_type(node);
}


/** SHARED pointer - copy in case but don't free.
 */
static inline librdf_uri *node_uri(librdf_node *node)
{
    return node == NULL ? NULL : librdf_node_get_uri(node);
}


#pragma mark Sqlite Debug/Profile


/* https://sqlite.org/eqp.html#section_2
** Argument pStmt is a prepared SQL statement. This function compiles
** an EXPLAIN QUERY PLAN command to report on the prepared statement,
** and prints the report to stdout using printf().
*/
static int printExplainQueryPlan(sqlite3_stmt *pStmt)
{
    const char *zSql = sqlite3_sql(pStmt);
    if( zSql == 0 ) return SQLITE_ERROR;

    char *zExplain = sqlite3_mprintf("EXPLAIN QUERY PLAN %s", zSql);
    if( zExplain == 0 ) return SQLITE_NOMEM;

    sqlite3_stmt *pExplain; /* Compiled EXPLAIN QUERY PLAN command */
    const int rc = sqlite3_prepare_v2(sqlite3_db_handle(pStmt), zExplain, -1, &pExplain, 0);
    sqlite3_free(zExplain);
    if( rc != SQLITE_OK ) return rc;

    while( SQLITE_ROW == sqlite3_step(pExplain) ) {
        const int iSelectid = sqlite3_column_int(pExplain, 0);
        const int iOrder = sqlite3_column_int(pExplain, 1);
        const int iFrom = sqlite3_column_int(pExplain, 2);
        const char *zDetail = (const char *)sqlite3_column_text(pExplain, 3);

        printf("%d %d %d %s\n", iSelectid, iOrder, iFrom, zDetail);
    }

    return sqlite3_finalize(pExplain);
}


// http://stackoverflow.com/a/6618833
static void trace(void *context, const char *sql)
{
    fprintf(stderr, "Query: %s\n", sql);
}


// http://stackoverflow.com/a/6618833
static void profile(void *context, const char *sql, const sqlite3_uint64 ns)
{
    fprintf(stderr, "Query: %s\n", sql);
    fprintf(stderr, "Execution Time: %llu ms\n", ns / 1000000);
}


#pragma mark Sqlite Convenience


/** Sqlite Result Code, e.g. SQLITE_OK */
typedef int sqlite_rc_t;

static sqlite_rc_t log_error(sqlite3 *db, const char *sql, const sqlite_rc_t rc)
{
    if( rc != SQLITE_OK ) {
        const char *errmsg = sqlite3_errmsg(db);
        librdf_log(NULL, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "SQLite SQL error - %s (%d)\n%s", errmsg, rc, sql);
    }
    return rc;
}


static sqlite3_stmt *prep_stmt(sqlite3 *db, sqlite3_stmt **stmt_p, const char *zSql)
{
    assert(db && "db handle is NULL");
    assert(stmt_p && "statement is NULL");
    assert(zSql && "SQL is NULL");
    if( *stmt_p ) {
        assert(0 == strcmp(sqlite3_sql(*stmt_p), zSql) && "ouch");
        const sqlite_rc_t rc0 = sqlite3_reset(*stmt_p);
        assert(rc0 == SQLITE_OK && "couldn't reset SQL statement");
    } else {
        const char *remainder = NULL;
        const int len_zSql = (int)strlen(zSql) + 1;
        const sqlite_rc_t rc0 = log_error( db, zSql, sqlite3_prepare_v2(db, zSql, len_zSql, stmt_p, &remainder) );
        assert(rc0 == SQLITE_OK && "couldn't compile SQL statement");
        assert(*remainder == '\0' && "had remainder");
    }
    assert(*stmt_p && "statement is NULL");
    return *stmt_p;
}


static void finalize_stmt(sqlite3_stmt **pStmt)
{
    if( *pStmt == NULL )
        return;
    sqlite3_reset(*pStmt);
    const sqlite_rc_t rc = sqlite3_finalize(*pStmt);
    assert(rc == SQLITE_OK && "ouch");
    *pStmt = NULL;
}


static inline sqlite_rc_t bind_int(sqlite3_stmt *stmt, const char *name, const id_t _id)
{
    assert(stmt && "stmt mandatory");
    assert(name && "name mandatory");
    const int idx = sqlite3_bind_parameter_index(stmt, name);
    return idx == 0 ? SQLITE_OK : sqlite3_bind_int64(stmt, idx, _id);
}


static inline sqlite_rc_t bind_text(sqlite3_stmt *stmt, const char *name, const unsigned char *text, const size_t text_len)
{
    assert(stmt && "stmt mandatory");
    assert(name && "name mandatory");
    const int idx = sqlite3_bind_parameter_index(stmt, name);
    return idx == 0 ? SQLITE_OK : ( text == NULL ? sqlite3_bind_null(stmt, idx) : sqlite3_bind_text(stmt, idx, (const char *)text, (int)text_len, SQLITE_STATIC) );
}


static inline sqlite_rc_t bind_null(sqlite3_stmt *stmt, const char *name)
{
    return bind_text(stmt, name, NULL, 0);
}


static inline sqlite_rc_t bind_uri(sqlite3_stmt *stmt, const char *name, librdf_uri *uri)
{
    size_t len = 0;
    return bind_text(stmt, name, uri == NULL ? NULL_URI : librdf_uri_as_counted_string(uri, &len), len);
}


static inline const str_uri_t column_uri_string(sqlite3_stmt *stmt, const int iCol)
{
    const str_uri_t ret = (str_uri_t)sqlite3_column_text(stmt, iCol);
    return isNULL_URI(ret) ? NULL : ret;
}


static inline const str_blank_t column_blank_string(sqlite3_stmt *stmt, const int iCol)
{
    const str_blank_t ret = (str_blank_t)sqlite3_column_text(stmt, iCol);
    return isNULL_BLANK(ret) ? NULL : ret;
}


static inline const str_lang_t column_language(sqlite3_stmt *stmt, const int iCol)
{
    const str_lang_t ret = (const str_lang_t)sqlite3_column_text(stmt, iCol);
    return isNULL_LANG(ret) ? NULL : ret;
}


static id_t insert(sqlite3 *db, sqlite3_stmt *stmt)
{
    const sqlite_rc_t rc3 = sqlite3_step(stmt);
    if( rc3 == SQLITE_DONE ) {
        const sqlite3_int64 r = sqlite3_last_insert_rowid(db);
        const sqlite_rc_t rc2 = sqlite3_reset(stmt);
        assert(rc2 == SQLITE_OK && "ouch");
        return (id_t)r;
    }
    return -rc3;
}


static sqlite_rc_t transaction_start(librdf_storage *storage)
{
    instance_t *db_ctx = get_instance(storage);
    if( db_ctx->in_transaction )
        return SQLITE_MISUSE;
    const sqlite_rc_t rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_start), "BEGIN IMMEDIATE TRANSACTION;") );
    db_ctx->in_transaction = rc == SQLITE_DONE;
    assert(db_ctx->in_transaction != BOOL_NO && "ouch");
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}


static sqlite_rc_t transaction_commit(librdf_storage *storage, const sqlite_rc_t begin)
{
    if( begin != SQLITE_OK )
        return SQLITE_OK;
    instance_t *db_ctx = get_instance(storage);
    if( db_ctx->in_transaction == BOOL_NO )
        return SQLITE_MISUSE;
    const sqlite_rc_t rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_commit), "COMMIT  TRANSACTION;") );
    db_ctx->in_transaction = !(rc == SQLITE_DONE);
    assert(db_ctx->in_transaction == BOOL_NO && "ouch");
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}


static sqlite_rc_t transaction_rollback(librdf_storage *storage, const sqlite_rc_t begin)
{
    if( begin != SQLITE_OK )
        return SQLITE_OK;
    instance_t *db_ctx = get_instance(storage);
    if( db_ctx->in_transaction == BOOL_NO )
        return SQLITE_MISUSE;
    const sqlite_rc_t rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_rollback), "ROLLBACK TRANSACTION;") );
    db_ctx->in_transaction = !(rc == SQLITE_DONE);
    assert(db_ctx->in_transaction == BOOL_NO && "ouch");
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}


#pragma mark -


#pragma mark Legacy


static int librdf_storage_sqlite_exec(librdf_storage *storage, const char *request, sqlite3_callback callback, void *arg, boolean_t fail_ok)
{
    instance_t *db_ctx = get_instance(storage);

    /* sqlite crashes if passed in a NULL sql string */
    if( !request )
        return RET_ERROR;

#if defined(LIBRDF_DEBUG) && LIBRDF_DEBUG > 2
    LIBRDF_DEBUG2("SQLite exec '%s'\n", request);
#endif

    char *errmsg = NULL;
    sqlite_rc_t status = sqlite3_exec(db_ctx->db, (const char *)request, callback, arg, &errmsg);
    if( fail_ok )
        status = SQLITE_OK;

    if( status != SQLITE_OK ) {
        if( status == SQLITE_LOCKED && !callback && db_ctx->in_stream ) {
            /* error message from sqlite3_exec needs to be freed on both sqlite 2 and 3 */
            if( errmsg )
                sqlite3_free(errmsg);

            legacy_query_t *query = LIBRDF_CALLOC( legacy_query_t *, 1, sizeof(*query) );
            if( !query )
                return RET_ERROR;

            const size_t query_buf = strlen( (char *)request ) + 1;
            query->query = LIBRDF_MALLOC(unsigned char *, query_buf);
            if( !query->query ) {
                LIBRDF_FREE(legacy_query_t *, query);
                return RET_ERROR;
            }

            strncpy( (char *)query->query, (char *)request, query_buf );

            if( !db_ctx->in_stream_queries )
                db_ctx->in_stream_queries = query;
            else {
                legacy_query_t *q = db_ctx->in_stream_queries;
                while( q->next )
                    q = q->next;
                q->next = query;
            }
            status = SQLITE_OK;
        } else {
            librdf_log(librdf_storage_get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
                       "SQLite database %s SQL exec '%s' failed - %s (%d)", db_ctx->name, request, errmsg, status);
            /* error message from sqlite3_exec needs to be freed on both sqlite 2 and 3 */
            if( errmsg )
                sqlite3_free(errmsg);
        }
    }

    return status == SQLITE_OK ? RET_OK : RET_ERROR;
}


#pragma mark Internal Implementation


#define TABLE_URIS_SO_NAME "so_uris"
#define TABLE_URIS_P_NAME "p_uris"
#define TABLE_URIS_T_NAME "t_uris"
#define TABLE_URIS_C_NAME "c_uris"
#define TABLE_BLANKS_SO_NAME "so_blanks"
#define TABLE_LITERALS_NAME "o_literals"
#define TABLE_TRIPLES_NAME "spocs"


static id_t create_uri(instance_t *db_ctx, librdf_uri *uri, sqlite3_stmt *stmt)
{
    if( !uri )
        return NULL_ID;
    assert(stmt && "Statement is null");
    size_t len = 0;
    const str_uri_t txt = (const str_uri_t)librdf_uri_as_counted_string(uri, &len);
    const sqlite_rc_t rc0 = sqlite3_bind_text(stmt, 1, (const char *)txt, (int)len, SQLITE_STATIC);
    assert(rc0 == SQLITE_OK && "bind fail");
    return insert(db_ctx->db, stmt);
}


static inline id_t create_uri_so(instance_t *db_ctx, librdf_node *node)
{
    if( !node || !librdf_node_is_resource(node) )
        return NULL_ID;
    return create_uri( db_ctx, node_uri(node), prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_so_set), "INSERT INTO " TABLE_URIS_SO_NAME " (uri) VALUES (?)") );
}


static inline id_t create_uri_p(instance_t *db_ctx, librdf_node *node)
{
    if( !node || !librdf_node_is_resource(node) )
        return NULL_ID;
    return create_uri( db_ctx, node_uri(node), prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_p_set), "INSERT INTO " TABLE_URIS_P_NAME " (uri) VALUES (?)") );
}


static inline id_t create_uri_c(instance_t *db_ctx, librdf_node *node)
{
    if( !node || !librdf_node_is_resource(node) )
        return NULL_ID;
    return create_uri( db_ctx, node_uri(node), prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_c_set), "INSERT INTO " TABLE_URIS_C_NAME " (uri) VALUES (?)") );
}


static inline id_t create_uri_t(instance_t *db_ctx, librdf_node *node)
{
    if( !node || !librdf_node_is_literal(node) )
        return NULL_ID;
    librdf_uri *uri = librdf_node_get_literal_value_datatype_uri(node);
    if( !uri )
        return NULL_ID;
    return create_uri( db_ctx, uri, prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_t_set), "INSERT INTO " TABLE_URIS_T_NAME " (uri) VALUES (?)") );
}


static id_t create_blank(instance_t *db_ctx, librdf_node *node, sqlite3_stmt *stmt)
{
    if( !node || !librdf_node_is_blank(node) )
        return NULL_ID;
    size_t len = 0;
    str_blank_t val = librdf_node_get_counted_blank_identifier(node, &len);
    if( val == NULL )
        return NULL_ID;
    const sqlite_rc_t rc0 = sqlite3_bind_text(stmt, 1, (const char *)val, (int)len, SQLITE_STATIC);
    assert(rc0 == SQLITE_OK && "bind fail");
    return insert(db_ctx->db, stmt);
}


static inline id_t create_blank_so(instance_t *db_ctx, librdf_node *node)
{
    return create_blank( db_ctx, node, prep_stmt(db_ctx->db, &(db_ctx->stmt_blank_so_set), "INSERT INTO " TABLE_BLANKS_SO_NAME " (blank) VALUES (?)") );
}


static id_t create_literal(instance_t *db_ctx, librdf_node *node, const id_t datatype_id, sqlite3_stmt *stmt)
{
    if( !node || !librdf_node_is_literal(node) )
        return NULL_ID;
    // void * p = librdf_node_get_literal_value_datatype_uri(node);
    size_t len = 0;
    str_lit_val_t txt = librdf_node_get_literal_value_as_counted_string(node, &len);
    str_lang_t language = librdf_node_get_literal_value_language(node);
    if( language == NULL )
        language = NULL_LANG;
    const sqlite_rc_t rc0 = sqlite3_bind_text(stmt, 1, (const char *)txt, (int)len, SQLITE_STATIC);
    assert(rc0 == SQLITE_OK && "bind fail");
    const sqlite_rc_t rc1 = sqlite3_bind_int64(stmt, 2, datatype_id);
    assert(rc1 == SQLITE_OK && "bind fail");
    const sqlite_rc_t rc2 = sqlite3_bind_text(stmt, 3, language, -1, SQLITE_STATIC);
    assert(rc2 == SQLITE_OK && "bind fail");
    return insert(db_ctx->db, stmt);
}


static inline id_t create_literal_o(instance_t *db_ctx, librdf_node *node, const id_t datatype_id)
{
    return create_literal( db_ctx, node, datatype_id, prep_stmt(db_ctx->db, &(db_ctx->stmt_literal_set), "INSERT INTO " TABLE_LITERALS_NAME " (text,datatype,language) VALUES (?,?,?)") );
}


/** Must match input parameter slots (until PART_C) of lookup_triple_sql and insert_triple_sql
 */
typedef enum {
    PART_S_U = 0,
    PART_S_B,
    PART_P_U,
    PART_O_U,
    PART_O_B,
    PART_O_L,
    PART_C,
    PART_DATATYPE, // indirect triple part - literal-datatype.
    PART_TRIPLE, // not strictly a triple part, reserved if I manage to return parts and triple in one go (either is fine)
}
part_t;


static sqlite_rc_t bind_stmt(sqlite3_stmt *stmt, librdf_node *context_node, librdf_statement *statement)
{
    sqlite_rc_t rc = SQLITE_OK;
    size_t len = 0;
    librdf_node_type t = LIBRDF_NODE_TYPE_UNKNOWN;
    part_t i = PART_S_U;
    // add some constants for reporting results back
    // node type markers
    rc = bind_int(stmt, ":t_uri", t = LIBRDF_NODE_TYPE_RESOURCE);
    rc = bind_int(stmt, ":t_blank", t = LIBRDF_NODE_TYPE_BLANK);
    rc = bind_int(stmt, ":t_literal", t = LIBRDF_NODE_TYPE_LITERAL);
    rc = bind_int(stmt, ":t_none", t = LIBRDF_NODE_TYPE_UNKNOWN);
    // triple part markers
    rc = bind_int(stmt, ":part_s_u", i = PART_S_U);
    rc = bind_int(stmt, ":part_s_b", i = PART_S_B);
    rc = bind_int(stmt, ":part_p_u", i = PART_P_U);
    rc = bind_int(stmt, ":part_o_u", i = PART_O_U);
    rc = bind_int(stmt, ":part_o_b", i = PART_O_B);
    rc = bind_int(stmt, ":part_o_l", i = PART_O_L);
    rc = bind_int(stmt, ":part_c", i = PART_C);
    rc = bind_int(stmt, ":part_datatype", i = PART_DATATYPE);
    rc = bind_int(stmt, ":part_triple", i = PART_TRIPLE);

    librdf_node *s = librdf_statement_get_subject(statement);
    t = node_type(s);
    rc = bind_int(stmt, ":s_type", t);
    if( t == LIBRDF_NODE_TYPE_UNKNOWN ) {
        rc = bind_null(stmt, ":s_uri");
        rc = bind_null(stmt, ":s_blank");
    } else {
        rc = bind_uri( stmt, ":s_uri", node_uri(s) );
        rc = bind_text(stmt, ":s_blank", librdf_node_get_counted_blank_identifier(s, &len), len);
    }

    librdf_node *p = librdf_statement_get_predicate(statement);
    t = node_type(p);
    rc = bind_int(stmt, ":p_type", t);
    if( t == LIBRDF_NODE_TYPE_UNKNOWN )
        rc = bind_null(stmt, ":p_uri");
    else
        rc = bind_uri( stmt, ":p_uri", node_uri(p) );

    librdf_node *o = librdf_statement_get_object(statement);
    t = node_type(o);
    rc = bind_int(stmt, ":o_type", t);
    if( t == LIBRDF_NODE_TYPE_UNKNOWN ) {
        rc = bind_null(stmt, ":o_uri");
        rc = bind_null(stmt, ":o_blank");
        rc = bind_null(stmt, ":o_text");
        rc = bind_null(stmt, ":o_datatype");
        rc = bind_null(stmt, ":o_language");
    } else {
        rc = bind_uri( stmt, ":o_uri", node_uri(o) );
        rc = bind_text(stmt, ":o_blank", librdf_node_get_counted_blank_identifier(o, &len), len);
        rc = bind_text(stmt, ":o_text", librdf_node_get_literal_value_as_counted_string(o, &len), len);
        rc = bind_uri( stmt, ":o_datatype", librdf_node_get_literal_value_datatype_uri(o) );
        str_lang_t language = librdf_node_get_literal_value_language(o);
        rc = bind_text(stmt, ":o_language", (const unsigned char *)(language ? language : NULL_LANG), -1);
    }

    rc = bind_int(stmt, ":c_wild", context_node == NULL);
    rc = bind_uri( stmt, ":c_uri", node_uri(context_node) );

    return SQLITE_OK;
}


static id_t create_part(instance_t *inst, librdf_node *node, const part_t part, id_t ids[])
{
    if( !node || !isNULL_ID(ids[part]) )
        return ids[part];
    switch( part ) {
    case PART_S_U:
        return create_uri_so(inst, node);
    case PART_S_B:
        return create_blank_so(inst, node);
    case PART_P_U:
        return create_uri_p(inst, node);
    case PART_O_U:
        return create_uri_so(inst, node);
    case PART_O_B:
        return create_blank_so(inst, node);
    case PART_O_L:
        return create_literal_o(inst, node, ids[PART_DATATYPE]);
    case PART_DATATYPE:
        return create_uri_t(inst, node);
    case PART_C:
        return create_uri_c(inst, node);
    case PART_TRIPLE:
        break;
    }
    assert(0 && "Fallthrough");
    return NULL_ID;
}


static librdf_statement *find_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement, boolean_t create)
{
    const sqlite_rc_t begin = transaction_start(storage);
    instance_t *db_ctx = get_instance(storage);

    // const id_t size = pub_size(storage);

    // find already existing parts
    id_t part_ids[PART_TRIPLE + 1] = {
        NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID
    };
    {
        const char *find_parts_sql = // generated via tools/sql2c.sh find_parts.sql
                                     "SELECT :part_s_u AS part, :t_uri AS node_type, id FROM so_uris WHERE (:s_type = :t_uri) AND uri = :s_uri" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_s_b AS part, :t_blank AS node_type, id FROM so_blanks WHERE (:s_type = :t_blank) AND blank = :s_blank" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_p_u AS part, :t_uri AS node_type, id FROM p_uris WHERE (:p_type = :t_uri) AND uri = :p_uri" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_o_u AS part, :t_uri AS node_type, id FROM so_uris WHERE (:o_type = :t_uri) AND uri = :o_uri" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_o_b AS part, :t_blank AS node_type, id FROM so_blanks WHERE (:o_type = :t_blank) AND blank = :o_blank" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_o_l AS part, :t_literal AS node_type, o_literals.id FROM o_literals" "\n" \
                                     "INNER JOIN t_uris ON o_literals.datatype = t_uris.id" "\n" \
                                     "WHERE (:o_type = :t_literal) AND (text = :o_text AND language = :o_language AND t_uris.uri = :o_datatype)" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_datatype AS part, :t_uri AS node_type, id FROM t_uris WHERE (:o_type = :t_literal) AND uri = :o_datatype" "\n" \
                                     "UNION" "\n" \
                                     "SELECT :part_c AS part, :t_uri AS node_type, id FROM c_uris WHERE uri = :c_uri" "\n" \
                                     "ORDER BY part DESC, node_type ASC" "\n" \
        ;
        sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_parts_get), find_parts_sql);
        const sqlite_rc_t rc3 = bind_stmt(stmt, context_node, statement);
        for( sqlite_rc_t rc = sqlite3_step(stmt); rc == SQLITE_ROW; rc = sqlite3_step(stmt) ) {
            switch( rc ) {
            case SQLITE_ROW: {
                const part_t idx = (part_t)sqlite3_column_int(stmt, 0);
                // const triple_node_type type = (triple_node_type)sqlite3_column_int(stmt, 1);
                part_ids[idx] = (id_t)sqlite3_column_int64(stmt, 2);
                break;
            }
            default:
                transaction_rollback(storage, begin);
                return NULL;
            }
        }
    }
    // create missing parts
    if( create ) {
        librdf_node *s = librdf_statement_get_subject(statement);
        librdf_node *p = librdf_statement_get_predicate(statement);
        librdf_node *o = librdf_statement_get_object(statement);
        part_ids[PART_S_U]  = create_part(db_ctx, s, PART_S_U, part_ids);
        part_ids[PART_S_B]  = create_part(db_ctx, s, PART_S_B, part_ids);
        part_ids[PART_P_U]  = create_part(db_ctx, p, PART_P_U, part_ids);
        part_ids[PART_O_U]  = create_part(db_ctx, o, PART_O_U, part_ids);
        part_ids[PART_O_B]  = create_part(db_ctx, o, PART_O_B, part_ids);
        part_ids[PART_DATATYPE] = create_part(db_ctx, o, PART_DATATYPE, part_ids);
        part_ids[PART_O_L]  = create_part(db_ctx, o, PART_O_L, part_ids);
        part_ids[PART_C]    = create_part(db_ctx, context_node, PART_C, part_ids);
    }
    {
        // check if we have such a triple (spocs)
        const char *lookup_triple_sql = "SELECT rowid FROM " TABLE_TRIPLES_NAME "\n" \
                                        "WHERE s_uri=? AND s_blank=? AND p_uri=? AND o_uri=? AND o_blank=? AND o_lit=? AND c_uri=?";
        sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_spocs_get), lookup_triple_sql);
        for( int i = PART_C; i >= PART_S_U; i-- ) {
            const sqlite_rc_t rc_ = sqlite3_bind_int64(stmt, i + 1, part_ids[i]);
            assert(rc_ == SQLITE_OK && "ouch");
        }
        const sqlite_rc_t rc = sqlite3_step(stmt);
        if( rc == SQLITE_ROW ) {
            transaction_rollback(storage, begin);
            return statement;
        }
    }
    if( !create ) {
        transaction_rollback(storage, begin);
        return NULL;
    }
    {
        // create the triple
        const char *insert_triple_sql = "INSERT INTO " TABLE_TRIPLES_NAME "\n" \
                                        "(s_uri,s_blank,p_uri,o_uri,o_blank,o_lit,c_uri) VALUES" "\n" \
                                        "(  ?  ,   ?   ,  ?  ,  ?  ,   ?   ,  ?  ,  ?  )";
        sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_spocs_set), insert_triple_sql);
        for( int i = PART_C; i >= PART_S_U; i-- ) {
            const sqlite_rc_t rc = sqlite3_bind_int64(stmt, i + 1, part_ids[i]);
            assert(rc == SQLITE_OK && "ouch");
        }
        const sqlite_rc_t rc = sqlite3_step(stmt);
        if( rc == SQLITE_DONE ) {
            // assert(size + 1 == pub_size(storage) && "hu?");
            transaction_commit(storage, begin);
            return statement;
        }
    }
    transaction_rollback(storage, begin);
    return NULL;
}


#pragma mark -

#pragma mark Public Interface

#pragma mark Lifecycle & Housekeeping


/** Create a new storage.
 *
 * Setup SQLIte connection instance but don't open yet.
 */
static int pub_init(librdf_storage *storage, const char *name, librdf_hash *options)
{
    if( !name ) {
        free_hash(options);
        return RET_ERROR;
    }

    instance_t *db_ctx = LIBRDF_CALLOC( instance_t *, 1, sizeof(*db_ctx) );
    if( !db_ctx ) {
        free_hash(options);
        return RET_ERROR;
    }

    librdf_storage_set_instance(storage, db_ctx);
    db_ctx->name_len = strlen(name);
    char *name_copy = LIBRDF_MALLOC(char *, db_ctx->name_len + 1);
    if( !name_copy ) {
        free_hash(options);
        return RET_ERROR;
    }

    strncpy(name_copy, name, db_ctx->name_len + 1);
    db_ctx->name = name_copy;

    if( BOOL_NO != librdf_hash_get_as_boolean(options, "new") )
        db_ctx->is_new = BOOL_YES;  /* default is NOT NEW */

    /* Redland default is "PRAGMA synchronous normal" */
    db_ctx->synchronous = SYNC_NORMAL;

    char *synchronous = synchronous = librdf_hash_get(options, "synchronous");
    if( synchronous ) {
        for( int i = 0; synchronous_flags[i]; i++ ) {
            if( !strcmp(synchronous, synchronous_flags[i]) ) {
                db_ctx->synchronous = i;
                break;
            }
        }
        LIBRDF_FREE(char *, synchronous);
    }

    free_hash(options);
    return RET_OK;
}


static void pub_terminate(librdf_storage *storage)
{
    instance_t *db_ctx = get_instance(storage);
    if( db_ctx == NULL )
        return;
    if( db_ctx->name )
        LIBRDF_FREE(char *, (void *)db_ctx->name);
    LIBRDF_FREE(instance_t *, db_ctx);
}


static int pub_close(librdf_storage *storage)
{
    instance_t *db_ctx = get_instance(storage);

    finalize_stmt( &(db_ctx->stmt_txn_start) );
    finalize_stmt( &(db_ctx->stmt_txn_commit) );
    finalize_stmt( &(db_ctx->stmt_txn_rollback) );
    finalize_stmt( &(db_ctx->stmt_uri_so_set) );
    finalize_stmt( &(db_ctx->stmt_uri_p_set) );
    finalize_stmt( &(db_ctx->stmt_uri_t_set) );
    finalize_stmt( &(db_ctx->stmt_uri_c_set) );
    finalize_stmt( &(db_ctx->stmt_literal_set) );
    finalize_stmt( &(db_ctx->stmt_blank_so_set) );
    finalize_stmt( &(db_ctx->stmt_parts_get) );
    finalize_stmt( &(db_ctx->stmt_spocs_get) );
    finalize_stmt( &(db_ctx->stmt_spocs_set) );

    finalize_stmt( &(db_ctx->stmt_size) );
    finalize_stmt( &(db_ctx->stmt_find) );

    const sqlite_rc_t rc = sqlite3_close(db_ctx->db);
    if( rc != SQLITE_OK ) {
        char *errmsg = (char *)sqlite3_errmsg(db_ctx->db);
        librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
                   "SQLite database %s close failed - %s", db_ctx->name, errmsg);
        pub_close(storage);
        return RET_ERROR;
    }
    return RET_OK;
}


static int pub_open(librdf_storage *storage, librdf_model *model)
{
    instance_t *db_ctx = get_instance(storage);
    const boolean_t db_file_exists = 0 == access(db_ctx->name, F_OK);
    if( db_ctx->is_new && db_file_exists )
        unlink(db_ctx->name);

    // open DB
    db_ctx->db = NULL;
    {
        const sqlite_rc_t rc = sqlite3_open(db_ctx->name, &db_ctx->db);
        char *errmsg = NULL;
        if( rc != SQLITE_OK )
            errmsg = (char *)sqlite3_errmsg(db_ctx->db);
        if( rc != SQLITE_OK ) {
            librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
                       "SQLite database %s open failed - %s", db_ctx->name, errmsg);
            pub_close(storage);
            return RET_ERROR;
        }

        // http://stackoverflow.com/a/6618833
        // sqlite3_profile(db_ctx->db, &profile, NULL);
        // sqlite3_profile(db_ctx->db, &trace, NULL);
    }

    // set DB PRAGMA 'synchronous'
    if( db_ctx->synchronous >= SYNC_OFF ) {
        char request[250];
        const size_t len = snprintf(request, sizeof(request) - 1, "PRAGMA synchronous=%s;", synchronous_flags[db_ctx->synchronous]);
        assert(len < sizeof(request) && "buffer too small.");
        const sqlite_rc_t rc = librdf_storage_sqlite_exec(storage, request, NULL, NULL, 0);
        if( rc ) {
            pub_close(storage);
            return RET_ERROR;
        }
    }

    // create tables and indices if DB is new.
    if( db_ctx->is_new ) {
        const sqlite_rc_t begin = transaction_start(storage);

        const char *schema_sql = // generated via tools/sql2c.sh schema.sql
                                 "CREATE TABLE so_uris (" "\n" \
                                 "  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1" "\n" \
                                 "  ,uri TEXT NOT NULL" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX so_uris_index ON so_uris (uri);" "\n" \
                                 "CREATE TABLE so_blanks (" "\n" \
                                 "  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1" "\n" \
                                 "  ,blank TEXT NULL" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX so_blanks_index ON so_blanks (blank);" "\n" \
                                 "CREATE TABLE p_uris (" "\n" \
                                 "  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1" "\n" \
                                 "  ,uri TEXT NOT NULL" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX p_uris_index ON p_uris (uri);" "\n" \
                                 "CREATE TABLE t_uris (" "\n" \
                                 "  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1" "\n" \
                                 "  ,uri TEXT NOT NULL" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX t_uris_index ON t_uris (uri);" "\n" \
                                 "CREATE TABLE o_literals (" "\n" \
                                 "  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1" "\n" \
                                 "  ,datatype INTEGER NOT NULL REFERENCES t_uris (id) ON DELETE NO ACTION" "\n" \
                                 "  ,text TEXT NOT NULL" "\n" \
                                 "  ,language TEXT NOT NULL" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX o_literals_index ON o_literals (text,language,datatype);" "\n" \
                                 "CREATE TABLE c_uris (" "\n" \
                                 "  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1" "\n" \
                                 "  ,uri TEXT NOT NULL" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX c_uris_index ON c_uris (uri);" "\n" \
                                 "INSERT INTO so_uris (id, uri) VALUES (0,'');" "\n" \
                                 "INSERT INTO so_blanks (id, blank) VALUES (0,'');" "\n" \
                                 "INSERT INTO c_uris (id, uri) VALUES (0,'');" "\n" \
                                 "INSERT INTO t_uris (id, uri) VALUES (0,'');" "\n" \
                                 "CREATE TABLE spocs (" "\n" \
                                 "  s_uri INTEGER NOT NULL    REFERENCES so_uris (id) ON DELETE NO ACTION" "\n" \
                                 "  ,s_blank INTEGER NOT NULL REFERENCES so_blanks (id) ON DELETE NO ACTION" "\n" \
                                 "  ,p_uri INTEGER NOT NULL   REFERENCES p_uris (id) ON DELETE NO ACTION" "\n" \
                                 "  ,o_uri INTEGER NOT NULL   REFERENCES so_uris (id) ON DELETE NO ACTION" "\n" \
                                 "  ,o_blank INTEGER NOT NULL REFERENCES so_blanks (id) ON DELETE NO ACTION" "\n" \
                                 "  ,o_lit INTEGER NOT NULL   REFERENCES t_literals (id) ON DELETE NO ACTION" "\n" \
                                 "  ,c_uri INTEGER NOT NULL   REFERENCES c_uris (id) ON DELETE NO ACTION" "\n" \
                                 ");" "\n" \
                                 "CREATE UNIQUE INDEX spocs_index ON spocs (s_uri,s_blank,p_uri,o_uri,o_blank,o_lit,c_uri);" "\n" \
                                 "CREATE VIEW spocs_full AS" "\n" \
                                 "SELECT" "\n" \
                                 "  s_uris.uri       AS s_uri" "\n" \
                                 "  ,s_blanks.blank  AS s_blank" "\n" \
                                 "  ,p_uris.uri      AS p_uri" "\n" \
                                 "  ,o_uris.uri      AS o_uri" "\n" \
                                 "  ,o_blanks.blank  AS o_blank" "\n" \
                                 "  ,o_literals.text AS o_text" "\n" \
                                 "  ,o_literals.language AS o_language" "\n" \
                                 "  ,o_lit_uris.uri  AS o_datatype" "\n" \
                                 "  ,c_uris.uri      AS c_uri" "\n" \
                                 "FROM spocs" "\n" \
                                 "INNER JOIN so_uris    AS s_uris     ON spocs.s_uri      = s_uris.id" "\n" \
                                 "INNER JOIN so_blanks  AS s_blanks   ON spocs.s_blank    = s_blanks.id" "\n" \
                                 "INNER JOIN p_uris     AS p_uris     ON spocs.p_uri      = p_uris.id" "\n" \
                                 "INNER JOIN so_uris    AS o_uris     ON spocs.o_uri      = o_uris.id" "\n" \
                                 "INNER JOIN so_blanks  AS o_blanks   ON spocs.o_blank    = o_blanks.id" "\n" \
                                 "LEFT OUTER JOIN o_literals AS o_literals ON spocs.o_lit = o_literals.id" "\n" \
                                 "LEFT OUTER JOIN t_uris     AS o_lit_uris ON o_literals.datatype   = o_lit_uris.id" "\n" \
                                 "INNER JOIN c_uris     AS c_uris     ON spocs.c_uri      = c_uris.id" "\n" \
                                 ";" "\n" \
                                 "PRAGMA user_version=2;" "\n" \
        ;

        if( RET_OK != librdf_storage_sqlite_exec(storage, schema_sql, NULL, NULL, 0) ) {
            transaction_rollback(storage, begin);
            pub_close(storage);
            return RET_ERROR;
        }

        transaction_commit(storage, begin);
    }
    return RET_OK;
}


/**
 * librdf_storage_sqlite_get_feature:
 * @storage: #librdf_storage object
 * @feature: #librdf_uri feature property
 *
 * Get the value of a storage feature.
 *
 * Return value: #librdf_node feature value or NULL if no such feature
 * exists or the value is empty.
 **/
static librdf_node *pub_get_feature(librdf_storage *storage, librdf_uri *feature)
{
    if( !feature )
        return NULL;
    const unsigned char *uri_string = librdf_uri_as_string(feature);
    if( !uri_string )
        return NULL;
    if( !strncmp( (const char *)uri_string, LIBRDF_MODEL_FEATURE_CONTEXTS, sizeof(LIBRDF_MODEL_FEATURE_CONTEXTS) + 1 ) )
        return librdf_new_node_from_typed_literal(get_world(storage), (const unsigned char *)"1", NULL, NULL);
    return NULL;
}


#pragma mark Transactions


static sqlite_rc_t pub_transaction_start(librdf_storage *storage)
{
    return transaction_start(storage);
}


static sqlite_rc_t pub_transaction_commit(librdf_storage *storage)
{
    return transaction_commit(storage, SQLITE_OK);
}


static sqlite_rc_t pub_transaction_rollback(librdf_storage *storage)
{
    return transaction_rollback(storage, SQLITE_OK);
}


#pragma mark Iterator


typedef struct
{
    librdf_storage *storage;
    librdf_statement *pattern;
    librdf_statement *statement;
    librdf_node *context;

    sqlite3_stmt *stmt;
    sqlite_rc_t txn;
    sqlite_rc_t rc;
    boolean_t dirty;
}
iterator_t;


static int pub_iter_end_of_stream(void *_ctx)
{
    assert(_ctx && "context mustn't be NULL");
    iterator_t *ctx = (iterator_t *)_ctx;
    return ctx->rc != SQLITE_ROW;
}


static int pub_iter_next_statement(void *_ctx)
{
    assert(_ctx && "context mustn't be NULL");
    if( pub_iter_end_of_stream(_ctx) )
        return RET_ERROR;
    iterator_t *ctx = (iterator_t *)_ctx;
    ctx->dirty = BOOL_YES;
    ctx->rc = sqlite3_step(ctx->stmt);
    if( pub_iter_end_of_stream(_ctx) )
        return RET_ERROR;
    return RET_OK;
}


static void *pub_iter_get_statement(void *_ctx, const int _flags)
{
    assert(_ctx && "context mustn't be NULL");
    const librdf_iterator_get_method_flags flags = (librdf_iterator_get_method_flags)_flags;
    iterator_t *ctx = (iterator_t *)_ctx;

    switch( flags ) {
    case LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT: {
        if( ctx->dirty && !pub_iter_end_of_stream(_ctx) ) {
            assert(ctx->statement && "statement mustn't be NULL");
            librdf_world *w = get_world(ctx->storage);
            librdf_statement *st = ctx->statement;
            sqlite3_stmt *stm = ctx->stmt;
            librdf_statement_clear(st);
            // stmt columns refer to find_triples_sql
            {
                /* subject */
                librdf_node *node = NULL;
                const str_uri_t uri = column_uri_string(stm, 0);
                if( uri )
                    node = librdf_new_node_from_uri_string(w, uri);
                if( !node ) {
                    const str_blank_t blank = column_blank_string(stm, 1);
                    if( blank )
                        node = librdf_new_node_from_blank_identifier(w, blank);
                }
                if( !node )
                    return NULL;
                librdf_statement_set_subject(st, node);
            }
            {
                /* predicate */
                librdf_node *node = NULL;
                const str_uri_t uri = column_uri_string(stm, 2);
                if( uri )
                    node = librdf_new_node_from_uri_string(w, uri);
                if( !node )
                    return NULL;
                librdf_statement_set_predicate(st, node);
            }
            {
                /* object */
                librdf_node *node = NULL;
                const str_uri_t uri = column_uri_string(stm, 3);
                if( uri )
                    node = librdf_new_node_from_uri_string(w, uri);
                if( !node ) {
                    const str_blank_t blank = column_blank_string(stm, 4);
                    if( blank )
                        node = librdf_new_node_from_blank_identifier(w, blank);
                }
                if( !node ) {
                    const str_lit_val_t val = (str_lit_val_t)sqlite3_column_text(stm, 5);
                    const str_lang_t lang = (str_lang_t)column_language(stm, 6);
                    const str_uri_t uri = column_uri_string(stm, 7);
                    librdf_uri *t = uri ? librdf_new_uri(w, uri) : NULL;
                    node = librdf_new_node_from_typed_literal(w, val, lang, t);
                    librdf_free_uri(t);
                }
                if( !node )
                    return NULL;
                librdf_statement_set_object(st, node);
            }
            assert(librdf_statement_is_complete(st) && "hu?");
            assert(librdf_statement_match(ctx->statement, ctx->pattern) && "match candidate doesn't match.");
            ctx->dirty = BOOL_NO;
        }
        return ctx->statement;
    }
    case LIBRDF_ITERATOR_GET_METHOD_GET_CONTEXT:
        return ctx->context;
    default:
        librdf_log(get_world(ctx->storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "Unknown iterator method flag %d", flags);
        return NULL;
    }
    return NULL;
}


static void pub_iter_finished(void *_ctx)
{
    assert(_ctx && "context mustn't be NULL");
    iterator_t *ctx = (iterator_t *)_ctx;
    if( ctx->pattern )
        librdf_free_statement(ctx->pattern);
    if( ctx->statement )
        librdf_free_statement(ctx->statement);
    librdf_storage_remove_reference(ctx->storage);
    transaction_rollback(ctx->storage, ctx->txn);
    sqlite3_finalize(ctx->stmt);
    LIBRDF_FREE(iterator_t *, ctx);
}


#pragma mark Query & Iterate


static int pub_size(librdf_storage *storage)
{
    instance_t *db_ctx = get_instance(storage);
    sqlite_rc_t begin = transaction_start(storage);

    sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_size), "SELECT COUNT(rowid) FROM " TABLE_TRIPLES_NAME);
    const sqlite_rc_t rc = sqlite3_step(stmt);
    if( rc == SQLITE_ROW ) {
        const id_t ret = (id_t)sqlite3_column_int64(stmt, 0);
        transaction_rollback(storage, begin);
        return ret;
    }
    transaction_rollback(storage, begin);
    return NULL_ID;
}


static librdf_iterator *pub_get_contexts(librdf_storage *storage)
{
    assert(0 && "not implemented yet.");
    return NULL;
}


static int pub_contains_statement(librdf_storage *storage, librdf_statement *statement)
{
    return NULL != find_statement(storage, NULL, statement, BOOL_NO);
}


static librdf_stream *pub_context_find_statements(librdf_storage *storage, librdf_statement *statement, librdf_node *context_node)
{
    sqlite_rc_t begin = transaction_start(storage);

    instance_t *db_ctx = get_instance(storage);
    const char *find_triples_sql = // generated via tools/sql2c.sh find_triples.sql
                                   "SELECT" "\n" \
                                   "  s_uri" "\n" \
                                   "  ,s_blank" "\n" \
                                   "  ,p_uri" "\n" \
                                   "  ,o_uri" "\n" \
                                   "  ,o_blank" "\n" \
                                   "  ,o_text" "\n" \
                                   "  ,o_language" "\n" \
                                   "  ,o_datatype" "\n" \
                                   "  ,c_uri" "\n" \
                                   "FROM spocs_full" "\n" \
                                   "WHERE" "\n" \
                                   "    ((:s_type != :t_uri)     OR (s_uri = :s_uri))" "\n" \
                                   "AND ((:s_type != :t_blank)   OR (s_blank = :s_blank))" "\n" \
                                   "AND ((:p_type != :t_uri)     OR (p_uri = :p_uri))" "\n" \
                                   "AND ((:o_type != :t_uri)     OR (o_uri = :o_uri))" "\n" \
                                   "AND ((:o_type != :t_blank)   OR (o_blank = :o_blank))" "\n" \
                                   "AND ((:o_type != :t_literal) OR (o_text = :o_text AND o_datatype = :o_datatype AND o_language = :o_language))" "\n" \
                                   "AND ((:c_wild)               OR (c_uri = :c_uri))" "\n" \
    ;

    sqlite3_stmt *stmt = NULL;
    stmt = prep_stmt(db_ctx->db, &stmt, find_triples_sql);

    // librdf_log( librdf_storage_get_world(storage), 0, LIBRDF_LOG_INFO, LIBRDF_FROM_STORAGE, NULL, "%s", librdf_statement_to_string(statement) );

    const sqlite_rc_t rc = bind_stmt(stmt, context_node, statement);
    assert(rc == SQLITE_OK && "foo");

    // printExplainQueryPlan(stmt);

    librdf_world *w = get_world(storage);
    // create iterator
    iterator_t *iter = LIBRDF_CALLOC(iterator_t *, sizeof(iterator_t), 1);
    iter->storage = storage;
    iter->context = context_node;
    iter->pattern = librdf_new_statement_from_statement(statement);
    iter->stmt = stmt;
    iter->txn = begin;
    iter->rc = sqlite3_step(stmt);
    iter->statement = librdf_new_statement(w);
    iter->dirty = BOOL_YES;

    librdf_storage_add_reference(iter->storage);
    librdf_stream *stream = librdf_new_stream(w, iter, &pub_iter_end_of_stream, &pub_iter_next_statement, &pub_iter_get_statement, &pub_iter_finished);

    return stream;
}


static librdf_stream *pub_find_statements(librdf_storage *storage, librdf_statement *statement)
{
    return pub_context_find_statements(storage, statement, NULL);
}


static librdf_stream *pub_context_serialise(librdf_storage *storage, librdf_node *context_node)
{
    return pub_context_find_statements(storage, NULL, context_node);
}


static librdf_stream *pub_serialise(librdf_storage *storage)
{
    return pub_context_serialise(storage, NULL);
}


#pragma mark Add


static int pub_context_add_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement)
{
    // librdf_log( librdf_storage_get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "%s", librdf_statement_to_string(statement) );
    return NULL == find_statement(storage, context_node, statement, BOOL_YES) ? RET_ERROR : RET_OK;
}


static int pub_context_add_statements(librdf_storage *storage, librdf_node *context_node, librdf_stream *statement_stream)
{
    const sqlite_rc_t txn = transaction_start(storage);
    for( librdf_statement *stmt = librdf_stream_get_object(statement_stream); !librdf_stream_end(statement_stream); librdf_stream_next(statement_stream) ) {
        const int rc = pub_context_add_statement(storage, context_node, stmt);
        if( RET_OK != rc ) {
            transaction_rollback(storage, txn);
            return rc;
        }
    }
    return transaction_commit(storage, txn);
}


static int pub_add_statement(librdf_storage *storage, librdf_statement *statement)
{
    return pub_context_add_statement(storage, NULL, statement);
}


static int pub_add_statements(librdf_storage *storage, librdf_stream *statement_stream)
{
    return pub_context_add_statements(storage, NULL, statement_stream);
}


#pragma mark Remove


static int pub_context_remove_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement)
{
    assert(0 && "not implemented yet.");
    return RET_ERROR;
}


static int pub_remove_statement(librdf_storage *storage, librdf_statement *statement)
{
    return pub_context_remove_statement(storage, NULL, statement);
}


static int pub_context_remove_statements(librdf_storage *storage, librdf_node *context_node)
{
    assert(0 && "not implemented yet.");
    return RET_ERROR;
}


#pragma mark Register Storage Factory


static void register_factory(librdf_storage_factory *factory)
{
    assert( !strcmp(factory->name, LIBRDF_STORAGE_SQLITE_MRO) );

    factory->version                    = LIBRDF_STORAGE_INTERFACE_VERSION;
    factory->init                       = pub_init;
    factory->terminate                  = pub_terminate;
    factory->open                       = pub_open;
    factory->close                      = pub_close;
    factory->size                       = pub_size;
    factory->add_statement              = pub_add_statement;
    factory->add_statements             = pub_add_statements;
    factory->remove_statement           = pub_remove_statement;
    factory->contains_statement         = pub_contains_statement;
    factory->serialise                  = pub_serialise;
    factory->find_statements            = pub_find_statements;
    factory->context_add_statement      = pub_context_add_statement;
    factory->context_add_statements     = pub_context_add_statements;
    factory->context_remove_statement   = pub_context_remove_statement;
    factory->context_remove_statements  = pub_context_remove_statements;
    factory->context_serialise          = pub_context_serialise;
    factory->find_statements_in_context = pub_context_find_statements;
    factory->get_contexts               = pub_get_contexts;
    factory->get_feature                = pub_get_feature;
    factory->transaction_start          = pub_transaction_start;
    factory->transaction_commit         = pub_transaction_commit;
    factory->transaction_rollback       = pub_transaction_rollback;
}


void librdf_init_storage_sqlite_mro(librdf_world *world)
{
    librdf_storage_register_factory(world, LIBRDF_STORAGE_SQLITE_MRO, "SQLite", &register_factory);
}
