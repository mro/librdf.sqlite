//
// rdf_storage_sqlite_mro.c
//
// Created by Marcus Rohrmoser on 19.05.14.
//
// Copyright (c) 2014-2015, Marcus Rohrmoser mobile Software, http://mro.name/me
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

const char *LIBRDF_STORAGE_SQLITE_MRO = LIBRDF_STORAGE_SQLITE_MRO_;

#include <stdlib.h>
// #include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <redland.h>
#include <rdf_storage.h>
#include <sqlite3.h>
#include <stdint.h>

#if DEBUG
#undef NDEBUG
#else
#define NDEBUG 1
#endif
#include <assert.h>

#define array_length(a) ( sizeof(a) / sizeof( (a)[0] ) )

#pragma mark Basic Types & Constants

typedef enum {
    BOOL_NO = 0,
    BOOL_YES = 1
} boolean_t;

typedef sqlite3_uint64 hash_t;
static const hash_t NULL_ID = 0;
static inline boolean_t isNULL_ID(const hash_t x)
{
    return NULL_ID == x;
}


#define RET_ERROR 1
#define RET_OK 0

/** C-String type for URIs. */
typedef unsigned char *str_uri_t;
/** C-String type for blank identifiers. */
typedef unsigned char *str_blank_t;
/** C-String type for literal values. */
typedef unsigned char *str_lit_val_t;
/** C-String type for (xml) language ids. */
typedef char *str_lang_t;


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

typedef enum {
    P_S_URI       = 1 << 0,
    P_S_BLANK     = 1 << 1,
    P_P_URI       = 1 << 2,
    P_O_URI       = 1 << 3,
    P_O_BLANK     = 1 << 4,
    P_O_TEXT      = 1 << 5,
    P_O_LANGUAGE  = 1 << 6,
    P_O_DATATYPE  = 1 << 7,
    P_C_URI       = 1 << 8
} sql_find_param_t;

#define ALL_PARAMS (P_C_URI << 1)

typedef struct
{
    sqlite3 *db;
    librdf_digest *digest;

    const char *name;
    boolean_t is_new;
    syncronous_flag_t synchronous;
    boolean_t in_transaction;

    boolean_t do_profile;
    sql_find_param_t sql_cache_mask;

    // compiled statements, lazy init
    sqlite3_stmt *stmt_txn_start;
    sqlite3_stmt *stmt_txn_commit;
    sqlite3_stmt *stmt_txn_rollback;
    sqlite3_stmt *stmt_triple_find; // complete triples
    sqlite3_stmt *stmt_triple_insert;
    sqlite3_stmt *stmt_triple_delete;

    sqlite3_stmt *stmt_size;

    sqlite3_stmt *stmt_triple_finds[ALL_PARAMS]; // sparse triples
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
    return NULL == node ? LIBRDF_NODE_TYPE_UNKNOWN : librdf_node_get_type(node);
}


/* Copy first 8 bytes of digest into 64bit using a method portable across big/little endianness.
 */
static hash_t digest_hash(librdf_digest *digest)
{
    assert(digest && "must be set");
    librdf_digest_final(digest);

    assert(librdf_digest_get_digest_length(digest) >= sizeof(hash_t) && "digest length too small");
    const int byte_count = 8;
    const int bit_per_byte = 8;
    assert(byte_count == sizeof(hash_t) && "made for 8-byte (64-bit) hashes");
    uint8_t *diges = (uint8_t *)librdf_digest_get_digest(digest);
    sqlite3_uint64 ret = 0; // enforce unsigned
    for( int i = byte_count - 1; i >= 0; i-- ) {
        ret <<= bit_per_byte;
        ret += diges[i];
    }
    assert(!isNULL_ID(ret) && "null hash");
    return (hash_t)ret;
}


static hash_t hash_uri(librdf_uri *uri, librdf_digest *digest)
{
    if( !uri )
        return NULL_ID;
    assert(digest && "digest must be set.");
    size_t len = 0;
    const unsigned char *s = librdf_uri_as_counted_string(uri, &len);
    assert(s && "uri NULL");
    assert(len && "uri length 0");
    librdf_digest_init(digest);
    librdf_digest_update(digest, s, len);
    return digest_hash(digest);
}


static hash_t node_hash_uri(librdf_node *node, librdf_digest *digest)
{
    return hash_uri(LIBRDF_NODE_TYPE_RESOURCE == node_type(node) ? librdf_node_get_uri(node) : NULL, digest);
}


static hash_t node_hash_blank(librdf_node *node, librdf_digest *digest)
{
    if( LIBRDF_NODE_TYPE_BLANK != node_type(node) )
        return NULL_ID;
    assert(digest && "digest must be set.");
    size_t len = 0;
    unsigned char *b = librdf_node_get_counted_blank_identifier(node, &len);
    assert(b && "blank NULL");
    assert(len && "blank len 0");
    librdf_digest_init(digest);
    librdf_digest_update(digest, b, len);
    return digest_hash(digest);
}


static hash_t node_hash_literal(librdf_node *node, librdf_digest *digest)
{
    if( LIBRDF_NODE_TYPE_LITERAL != node_type(node) )
        return NULL_ID;
    assert(digest && "digest must be set.");
    librdf_digest_init(digest);
    size_t len = 0;
    unsigned char *s = librdf_node_get_literal_value_as_counted_string(node, &len);
    assert(s && "literal without value");
    librdf_digest_update(digest, s, len);

    librdf_uri *uri = librdf_node_get_literal_value_datatype_uri(node);
    if( uri ) {
        const unsigned char *str = librdf_uri_as_counted_string(uri, &len);
        librdf_digest_update(digest, str, len);
    }

    const char *l = librdf_node_get_literal_value_language(node);
    if( l )
        librdf_digest_update( digest, (unsigned char *)l, strlen(l) );
    return digest_hash(digest);
}


/** https://stackoverflow.com/questions/2590677/how-do-i-combine-hash-values-in-c0x
 */
static inline hash_t hash_combine(const hash_t seed, const hash_t b)
{
    // return seed ^ ( b + 0x9e3779b9 + (seed << 6) + (seed >> 2) );
    // http://stackoverflow.com/a/4948967 and https://stackoverflow.com/questions/4948780/magic-number-in-boosthash-combine#comment35569848_4948967
    // $ python -c "import math; print hex(int(2**64 / ((1 + math.sqrt(5)) / 2)))"
    return seed ^ ( b + 0x9e3779b97f4a7800L + (seed << 6) + (seed >> 2) );
}


static hash_t hash_combine_stmt(const hash_t s_uri_id, const hash_t s_blank_id, const hash_t p_uri_id, const hash_t o_uri_id, const hash_t o_blank_id, const hash_t o_lit_id, const hash_t c_uri_id)
{
    hash_t stmt_id = NULL_ID;
    stmt_id = hash_combine(stmt_id, s_uri_id);
    stmt_id = hash_combine(stmt_id, s_blank_id);
    stmt_id = hash_combine(stmt_id, p_uri_id);
    stmt_id = hash_combine(stmt_id, o_uri_id);
    stmt_id = hash_combine(stmt_id, o_blank_id);
    stmt_id = hash_combine(stmt_id, o_lit_id);
    // stmt_id ^= o_type_id;
    stmt_id = hash_combine(stmt_id, c_uri_id);
    return stmt_id;
}


static hash_t stmt_hash(librdf_statement *stmt, librdf_node *context_node, librdf_digest *digest)
{
    if( !stmt )
        return NULL_ID;

    librdf_node *s = librdf_statement_get_subject(stmt);
    librdf_node *p = librdf_statement_get_predicate(stmt);
    librdf_node *o = librdf_statement_get_object(stmt);

    return hash_combine_stmt( node_hash_uri(s, digest), node_hash_blank(s, digest), node_hash_uri(p, digest), node_hash_uri(o, digest), node_hash_blank(o, digest), node_hash_literal(o, digest), node_hash_uri(context_node, digest) );
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
    if( NULL == zSql ) return SQLITE_ERROR;

    char *zExplain = sqlite3_mprintf("EXPLAIN QUERY PLAN %s", zSql);
    if( NULL == zExplain ) return SQLITE_NOMEM;

    sqlite3_stmt *pExplain; /* Compiled EXPLAIN QUERY PLAN command */
    const int rc = sqlite3_prepare_v2(sqlite3_db_handle(pStmt), zExplain, -1, &pExplain, 0);
    sqlite3_free(zExplain);
    if( SQLITE_OK != rc ) return rc;

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
    fprintf(stderr, "Query SQL: %s\n", sql);
}


// http://stackoverflow.com/a/6618833
static void profile(void *context, const char *sql, const sqlite3_uint64 ns)
{
    fprintf(stderr, "dt=%llu ms Query SQL: %s\n", ns / 1000000, sql);
}


#pragma mark Sqlite Convenience


/** Sqlite Result Code, e.g. SQLITE_OK */
typedef int sqlite_rc_t;

static sqlite_rc_t log_error(sqlite3 *db, const char *sql, const sqlite_rc_t rc)
{
    if( SQLITE_OK != rc ) {
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
        assert(SQLITE_OK == rc0 && "couldn't reset SQL statement");
        const sqlite_rc_t rc1 = sqlite3_clear_bindings(*stmt_p);
        assert(SQLITE_OK == rc1 && "couldn't reset SQL statement");
    } else {
        const char *remainder = NULL;
        const int len_zSql = (int)strlen(zSql) + 1;
        const sqlite_rc_t rc0 = log_error( db, zSql, sqlite3_prepare_v2(db, zSql, len_zSql, stmt_p, &remainder) );
        assert(SQLITE_OK == rc0 && "couldn't compile SQL statement");
        assert('\0' == *remainder && "had remainder");
    }
    assert(*stmt_p && "statement is NULL");
    return *stmt_p;
}


static void finalize_stmt(sqlite3_stmt **pStmt)
{
    if( NULL == *pStmt )
        return;
    sqlite3_reset(*pStmt);
    const sqlite_rc_t rc = sqlite3_finalize(*pStmt);
    assert(SQLITE_OK == rc && "ouch");
    *pStmt = NULL;
}


static sqlite_rc_t exec_stmt(sqlite3 *db, const char *sql)
{
    char *errmsg = NULL;
    const sqlite_rc_t rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
    sqlite3_free(errmsg);
    return rc;
}


static inline sqlite_rc_t bind_int(sqlite3_stmt *stmt, const char *name, const hash_t _id)
{
    assert(stmt && "stmt mandatory");
    assert(name && "name mandatory");
    const int idx = sqlite3_bind_parameter_index(stmt, name);
    return 0 == idx ? SQLITE_OK : sqlite3_bind_int64(stmt, idx, _id);
}


static inline sqlite_rc_t bind_text(sqlite3_stmt *stmt, const char *name, const unsigned char *text, const size_t text_len)
{
    assert(stmt && "stmt mandatory");
    assert(name && "name mandatory");
    const int idx = sqlite3_bind_parameter_index(stmt, name);
    return 0 == idx ? SQLITE_OK : ( NULL == text ? sqlite3_bind_null(stmt, idx) : sqlite3_bind_text(stmt, idx, (const char *)text, (int)text_len, SQLITE_STATIC) );
}


static inline sqlite_rc_t bind_null(sqlite3_stmt *stmt, const char *name)
{
    return bind_text(stmt, name, NULL, 0);
}


static sqlite_rc_t bind_uri_id(sqlite3_stmt *stmt, librdf_digest *digest, const char *name, librdf_uri *uri, hash_t *value)
{
    if( uri ) {
        const hash_t v = hash_uri(uri, digest);
        if( value ) *value = v;
        return bind_int(stmt, name, v);
    }
    return bind_null(stmt, name);
}


static sqlite_rc_t bind_uri(sqlite3_stmt *stmt, const char *name, librdf_uri *uri)
{
    if( uri ) {
        size_t len = 0;
        const unsigned char *str = librdf_uri_as_counted_string(uri, &len);
        return bind_text(stmt, name, str, len);
    }
    return bind_null(stmt, name);
}


static sqlite_rc_t bind_node_uri_id(sqlite3_stmt *stmt, librdf_digest *digest, const char *name, librdf_node *node, hash_t *value)
{
    return bind_uri_id(stmt, digest, name, LIBRDF_NODE_TYPE_RESOURCE == node_type(node) ? librdf_node_get_uri(node) : NULL, value);
}


static sqlite_rc_t bind_node_uri(sqlite3_stmt *stmt, const char *name, librdf_node *node)
{
    return bind_uri(stmt, name, LIBRDF_NODE_TYPE_RESOURCE == node_type(node) ? librdf_node_get_uri(node) : NULL);
}


static sqlite_rc_t bind_node_blank_id(sqlite3_stmt *stmt, librdf_digest *digest, const char *name, librdf_node *node, hash_t *value)
{
    if( LIBRDF_NODE_TYPE_BLANK == node_type(node) ) {
        const hash_t v = node_hash_blank(node, digest);
        if( value ) *value = v;
        return bind_int(stmt, name, v);
    }
    return bind_null(stmt, name);
}


static sqlite_rc_t bind_node_blank(sqlite3_stmt *stmt, const char *name, librdf_node *node)
{
    if( LIBRDF_NODE_TYPE_BLANK == node_type(node) ) {
        size_t len = 0;
        const unsigned char *str = librdf_node_get_counted_blank_identifier(node, &len);
        return bind_text(stmt, name, str, len);
    }
    return bind_null(stmt, name);
}


static sqlite_rc_t bind_node_lit_id(sqlite3_stmt *stmt, librdf_digest *digest, const char *name, librdf_node *node, hash_t *value)
{
    if( LIBRDF_NODE_TYPE_LITERAL == node_type(node) ) {
        const hash_t v = node_hash_literal(node, digest);
        if( value ) *value = v;
        return bind_int(stmt, name, v);
    }
    return bind_null(stmt, name);
}


static sqlite_rc_t bind_stmt(instance_t *db_ctx, librdf_statement *statement, librdf_node *context_node, sqlite3_stmt *stmt)
{
    librdf_node *s = librdf_statement_get_subject(statement);
    librdf_node *p = librdf_statement_get_predicate(statement);
    librdf_node *o = librdf_statement_get_object(statement);

    sqlite_rc_t rc = SQLITE_OK;
    hash_t s_uri_id = NULL_ID;
    hash_t s_blank_id = NULL_ID;
    hash_t p_uri_id = NULL_ID;
    hash_t o_uri_id = NULL_ID;
    hash_t o_blank_id = NULL_ID;
    hash_t o_lit_id = NULL_ID;
    hash_t o_type_id = NULL_ID;
    hash_t c_uri_id = NULL_ID;
    if( SQLITE_OK != ( rc = bind_node_uri_id(stmt, db_ctx->digest, ":s_uri_id", s, &s_uri_id) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_uri(stmt, ":s_uri", s) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_blank_id(stmt, db_ctx->digest, ":s_blank_id", s, &s_blank_id) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_blank(stmt, ":s_blank", s) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_uri_id(stmt, db_ctx->digest, ":p_uri_id", p, &p_uri_id) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_uri(stmt, ":p_uri", p) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_uri_id(stmt, db_ctx->digest, ":o_uri_id", o, &o_uri_id) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_uri(stmt, ":o_uri", o) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_blank_id(stmt, db_ctx->digest, ":o_blank_id", o, &o_blank_id) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_blank(stmt, ":o_blank", o) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_lit_id(stmt, db_ctx->digest, ":o_lit_id", o, &o_lit_id) ) ) return rc;
    if( LIBRDF_NODE_TYPE_LITERAL == node_type(o) ) {
        if( SQLITE_OK != ( rc = bind_uri_id(stmt, db_ctx->digest, ":o_datatype_id", librdf_node_get_literal_value_datatype_uri(o), &o_type_id) ) ) return rc;
        if( SQLITE_OK != ( rc = bind_uri( stmt, ":o_datatype", librdf_node_get_literal_value_datatype_uri(o) ) ) ) return rc;
        char *l = librdf_node_get_literal_value_language(o);
        if( l )
            if( SQLITE_OK != ( rc = bind_text( stmt, ":o_language", (unsigned char *)l, strlen(l) ) ) ) return rc;
        size_t len = 0;
        const unsigned char *str = librdf_node_get_literal_value_as_counted_string(o, &len);
        if( SQLITE_OK != ( rc = bind_text(stmt, ":o_text", str, len) ) ) return rc;
    }

    if( SQLITE_OK != ( rc = bind_node_uri_id(stmt, db_ctx->digest, ":c_uri_id", context_node, &c_uri_id) ) ) return rc;
    if( SQLITE_OK != ( rc = bind_node_uri(stmt, ":c_uri", context_node) ) ) return rc;

    if( !librdf_statement_is_complete(statement) )
        return SQLITE_OK;

    const hash_t stmt_id = hash_combine_stmt(s_uri_id, s_blank_id, p_uri_id, o_uri_id, o_blank_id, o_lit_id, c_uri_id);

    assert(stmt_hash(statement, context_node, db_ctx->digest) == stmt_id && "statement hash compatation mismatch");
    if( SQLITE_OK != ( rc = bind_int(stmt, ":stmt_id", stmt_id) ) ) return rc;

    return SQLITE_OK;
}


static inline const str_uri_t column_uri_string(sqlite3_stmt *stmt, const int iCol)
{
    return (str_uri_t)sqlite3_column_text(stmt, iCol);
}


static inline const str_blank_t column_blank_string(sqlite3_stmt *stmt, const int iCol)
{
    return (str_blank_t)sqlite3_column_text(stmt, iCol);
}


static inline const str_lang_t column_language(sqlite3_stmt *stmt, const int iCol)
{
    return (str_lang_t)sqlite3_column_text(stmt, iCol);
}


static sqlite_rc_t transaction_start(librdf_storage *storage)
{
    instance_t *db_ctx = get_instance(storage);
    if( db_ctx->in_transaction )
        return SQLITE_MISUSE;
    const sqlite_rc_t rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_start), "BEGIN IMMEDIATE TRANSACTION;") );
    db_ctx->in_transaction = SQLITE_DONE == rc;
    assert(db_ctx->in_transaction != BOOL_NO && "ouch");
    return SQLITE_DONE == rc ? SQLITE_OK : rc;
}


static sqlite_rc_t transaction_commit(librdf_storage *storage, const sqlite_rc_t begin)
{
    if( begin != SQLITE_OK )
        return SQLITE_OK;
    instance_t *db_ctx = get_instance(storage);
    if( BOOL_NO == db_ctx->in_transaction )
        return SQLITE_MISUSE;
    const sqlite_rc_t rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_commit), "COMMIT  TRANSACTION;") );
    db_ctx->in_transaction = !(SQLITE_DONE == rc);
    assert(BOOL_NO == db_ctx->in_transaction && "ouch");
    return SQLITE_DONE == rc ? SQLITE_OK : rc;
}


static sqlite_rc_t transaction_rollback(librdf_storage *storage, const sqlite_rc_t begin)
{
    if( begin != SQLITE_OK )
        return SQLITE_OK;
    instance_t *db_ctx = get_instance(storage);
    if( BOOL_NO == db_ctx->in_transaction )
        return SQLITE_MISUSE;
    const sqlite_rc_t rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_rollback), "ROLLBACK TRANSACTION;") );
    db_ctx->in_transaction = !(SQLITE_DONE == rc);
    assert(BOOL_NO == db_ctx->in_transaction && "ouch");
    return SQLITE_DONE == rc ? SQLITE_OK : rc;
}


#pragma mark -


#pragma mark Internal Implementation


/** insert_triple_sql + find_triples_sql
 */
typedef enum {
    IDX_S_URI = 9,
    IDX_S_BLANK,
    IDX_P_URI,
    IDX_O_URI,
    IDX_O_BLANK,
    IDX_O_TEXT,
    IDX_O_LANGUAGE,
    IDX_O_DATATYPE,
    IDX_C_URI
}
idx_triple_column_t;


static librdf_statement *find_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement, const boolean_t create)
{
    assert(statement && "statement must be set.");
    assert(librdf_statement_is_complete(statement) && "statement must be complete.");

    instance_t *db_ctx = get_instance(storage);

    if( !create ) {
        const hash_t stmt_id = stmt_hash(statement, context_node, db_ctx->digest);
        assert(!isNULL_ID(stmt_id) && "mustn't be nil");

        sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_triple_find), "SELECT id FROM triple_relations WHERE id = :stmt_id");

        if( SQLITE_OK != bind_int(stmt, ":stmt_id", stmt_id) )
            return NULL;
        return SQLITE_ROW == sqlite3_step(stmt) ? statement : NULL;
    }
    const char insert_triple_sql[] = // generated via tools/sql2c.sh insert_triple.sql
                                     "INSERT OR IGNORE INTO triples(" "\n" \
                                     "  id," "\n" \
                                     "  s_uri_id, s_uri," "\n" \
                                     "  s_blank_id, s_blank," "\n" \
                                     "  p_uri_id, p_uri," "\n" \
                                     "  o_uri_id, o_uri," "\n" \
                                     "  o_blank_id, o_blank," "\n" \
                                     "  o_lit_id, o_datatype_id, o_datatype, o_language, o_text," "\n" \
                                     "  c_uri_id, c_uri" "\n" \
                                     ") VALUES (" "\n" \
                                     "  :stmt_id," "\n" \
                                     "  :s_uri_id, :s_uri," "\n" \
                                     "  :s_blank_id, :s_blank," "\n" \
                                     "  :p_uri_id, :p_uri," "\n" \
                                     "  :o_uri_id, :o_uri," "\n" \
                                     "  :o_blank_id, :o_blank," "\n" \
                                     "  :o_lit_id, :o_datatype_id, :o_datatype, :o_language, :o_text," "\n" \
                                     "  :c_uri_id, :c_uri" "\n" \
                                     ")" "\n" \
    ;

    sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_triple_insert), insert_triple_sql);
    if( SQLITE_OK != bind_stmt(db_ctx, statement, context_node, stmt) )
        return NULL;

    if( BOOL_NO ) {
        // toggle via "profile" feature?
        printExplainQueryPlan(stmt);
    }

    const sqlite_rc_t rc = sqlite3_step(stmt);
    return SQLITE_DONE == rc ? statement : NULL;
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
    const size_t name_len = strlen(name);
    char *name_copy = LIBRDF_MALLOC(char *, name_len + 1);
    if( !name_copy ) {
        free_hash(options);
        return RET_ERROR;
    }

    strncpy(name_copy, name, name_len + 1);
    db_ctx->name = name_copy;

    if( !( db_ctx->digest = librdf_new_digest(get_world(storage), "MD5") ) ) {
        free_hash(options);
        return RET_ERROR;
    }

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
    if( NULL == db_ctx )
        return;
    if( db_ctx->name )
        LIBRDF_FREE(char *, (void *)db_ctx->name);
    if( db_ctx->digest )
        librdf_free_digest(db_ctx->digest);
    LIBRDF_FREE(instance_t *, db_ctx);
}


static int pub_close(librdf_storage *storage)
{
    instance_t *db_ctx = get_instance(storage);
    if( !db_ctx->db )
        return RET_OK;

    finalize_stmt( &(db_ctx->stmt_txn_start) );
    finalize_stmt( &(db_ctx->stmt_txn_commit) );
    finalize_stmt( &(db_ctx->stmt_txn_rollback) );
    finalize_stmt( &(db_ctx->stmt_triple_find) );
    finalize_stmt( &(db_ctx->stmt_triple_insert) );
    finalize_stmt( &(db_ctx->stmt_triple_delete) );

    finalize_stmt( &(db_ctx->stmt_size) );

    for( int i = ALL_PARAMS - 1; i >= 0; i-- )
        finalize_stmt( &(db_ctx->stmt_triple_finds[i]) );

    const sqlite_rc_t rc = sqlite3_close(db_ctx->db);
    if( SQLITE_OK == rc ) {
        db_ctx->db = NULL;
        return RET_OK;
    }
    char *errmsg = (char *)sqlite3_errmsg(db_ctx->db);
    librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "SQLite database %s close failed - %s", db_ctx->name, errmsg);
    return rc;
}


static int pub_open(librdf_storage *storage, librdf_model *model)
{
    instance_t *db_ctx = get_instance(storage);

    const boolean_t file_exists = ( 0 == access(db_ctx->name, F_OK) );
    if( db_ctx->is_new && file_exists )
        unlink(db_ctx->name);

    // open DB
    db_ctx->db = NULL;
    {
        const sqlite_rc_t rc = sqlite3_open(db_ctx->name, &db_ctx->db);
        if( SQLITE_OK != rc ) {
            const char *errmsg = sqlite3_errmsg(db_ctx->db);
            librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "SQLite database %s open failed - %s", db_ctx->name, errmsg);
            return rc;
        }

        // http://stackoverflow.com/a/6618833
        if( db_ctx->do_profile ) {
            sqlite3_profile(db_ctx->db, &profile, NULL);
            // sqlite3_trace(db_ctx->db, &trace, NULL);
        }
    }

    // set DB session PRAGMAs
    if( SYNC_OFF <= db_ctx->synchronous ) {
        char sql[250];
        const size_t len = snprintf(sql, sizeof(sql) - 1, "PRAGMA synchronous=%s;", synchronous_flags[db_ctx->synchronous]);
        assert(len < sizeof(sql) && "buffer too small.");
        const sqlite_rc_t rc = exec_stmt(db_ctx->db, sql);
        if( SQLITE_OK != rc ) {
            pub_close(storage);
            return rc;
        }
    }
    {
        const char *const sqls[] = {
            "PRAGMA foreign_keys = ON;",
            "PRAGMA recursive_triggers = ON;",
            "PRAGMA encoding = 'UTF-8';",
            NULL
        };
        for( int v = 0; sqls[v]; v++ ) {
            const sqlite_rc_t rc = exec_stmt(db_ctx->db, sqls[v]);
            if( SQLITE_OK != rc ) {
                pub_close(storage);
                return rc;
            }
        }
    }

    // check & update schema (run migrations)
    {
        sqlite_rc_t rc = SQLITE_OK;
        sqlite3_stmt *stmt = NULL;
        prep_stmt(db_ctx->db, &stmt, "PRAGMA user_version;");
        if( SQLITE_ROW != ( rc = sqlite3_step(stmt) ) ) {
            sqlite3_finalize(stmt);
            pub_close(storage);
            return rc;
        }
        const int schema_version = sqlite3_column_int(stmt, 0);
        if( SQLITE_DONE != ( rc = sqlite3_step(stmt) ) ) {
            sqlite3_finalize(stmt);
            pub_close(storage);
            return rc;
        }
        sqlite3_finalize(stmt);

        const char schema_mig_to_1_sql[] = // generated via tools/sql2c.sh schema_mig_to_1.sql
                                           "PRAGMA foreign_keys = ON;" "\n" \
                                           "PRAGMA recursive_triggers = ON;" "\n" \
                                           "PRAGMA encoding = 'UTF-8';" "\n" \
                                           " -- URIs for subjects and objects" "\n" \
                                           "CREATE TABLE so_uris (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space" "\n" \
                                           ");" "\n" \
                                           " -- blank node IDs for subjects and objects" "\n" \
                                           "CREATE TABLE so_blanks (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,blank TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space" "\n" \
                                           ");" "\n" \
                                           " -- URIs for predicates" "\n" \
                                           "CREATE TABLE p_uris (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space" "\n" \
                                           ");" "\n" \
                                           " -- URIs for literal types" "\n" \
                                           "CREATE TABLE t_uris (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space" "\n" \
                                           ");" "\n" \
                                           " -- literal values" "\n" \
                                           "CREATE TABLE o_literals (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,datatype_id INTEGER NULL REFERENCES t_uris(id)" "\n" \
                                           "  ,language TEXT NULL" "\n" \
                                           "  ,text TEXT NOT NULL" "\n" \
                                           ");" "\n" \
                                           " -- CREATE UNIQUE INDEX o_literals_index ON o_literals (text,language,datatype_id); -- redundant constraint (hash should do), could be dropped to save space" "\n" \
                                           " -- URIs for context" "\n" \
                                           "CREATE TABLE c_uris (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space" "\n" \
                                           ");" "\n" \
                                           "CREATE TABLE triple_relations (" "\n" \
                                           "  id INTEGER PRIMARY KEY" "\n" \
                                           "  ,s_uri_id   INTEGER NULL      REFERENCES so_uris(id)" "\n" \
                                           "  ,s_blank_id INTEGER NULL      REFERENCES so_blanks(id)" "\n" \
                                           "  ,p_uri_id   INTEGER NOT NULL  REFERENCES p_uris(id)" "\n" \
                                           "  ,o_uri_id   INTEGER NULL      REFERENCES so_uris(id)" "\n" \
                                           "  ,o_blank_id INTEGER NULL      REFERENCES so_blanks(id)" "\n" \
                                           "  ,o_lit_id   INTEGER NULL      REFERENCES o_literals(id)" "\n" \
                                           "  ,c_uri_id   INTEGER NULL      REFERENCES c_uris(id)" "\n" \
                                           "  , CONSTRAINT null_subject CHECK ( -- ensure uri/blank are mutually exclusive" "\n" \
                                           "    (s_uri_id IS NOT NULL AND s_blank_id IS NULL) OR" "\n" \
                                           "    (s_uri_id IS NULL AND s_blank_id IS NOT NULL)" "\n" \
                                           "  )" "\n" \
                                           "  , CONSTRAINT null_object CHECK ( -- ensure uri/blank/literal are mutually exclusive" "\n" \
                                           "    (o_uri_id IS NOT NULL AND o_blank_id IS NULL AND o_lit_id IS NULL) OR" "\n" \
                                           "    (o_uri_id IS NULL AND o_blank_id IS NOT NULL AND o_lit_id IS NULL) OR" "\n" \
                                           "    (o_uri_id IS NULL AND o_blank_id IS NULL AND o_lit_id IS NOT NULL)" "\n" \
                                           "  )" "\n" \
                                           ");" "\n" \
                                           " -- redundant constraint (hash should do), could be dropped to save space:" "\n" \
                                           " -- CREATE UNIQUE INDEX triple_relations_index     ON triple_relations(s_uri_id,s_blank_id,p_uri_id,o_uri_id,o_blank_id,o_lit_id,c_uri_id);" "\n" \
                                           " -- optional indexes for lookup performance, mostly on DELETE." "\n" \
                                           "CREATE INDEX triple_relations_index_s_uri_id   ON triple_relations(s_uri_id); -- WHERE s_uri_id IS NOT NULL;" "\n" \
                                           "CREATE INDEX triple_relations_index_s_blank_id ON triple_relations(s_blank_id); -- WHERE s_blank_id IS NOT NULL;" "\n" \
                                           "CREATE INDEX triple_relations_index_p_uri_id   ON triple_relations(p_uri_id); -- WHERE p_uri_id IS NOT NULL;" "\n" \
                                           "CREATE INDEX triple_relations_index_o_uri_id   ON triple_relations(o_uri_id); -- WHERE o_uri_id IS NOT NULL;" "\n" \
                                           "CREATE INDEX triple_relations_index_o_blank_id ON triple_relations(o_blank_id); -- WHERE s_blank_id IS NOT NULL;" "\n" \
                                           "CREATE INDEX triple_relations_index_o_lit_id   ON triple_relations(o_lit_id); -- WHERE o_lit_id IS NOT NULL;" "\n" \
                                           "CREATE INDEX o_literals_index_datatype_id      ON o_literals(datatype_id); -- WHERE datatype_id IS NOT NULL;" "\n" \
                                           "CREATE VIEW triples AS" "\n" \
                                           "SELECT" "\n" \
                                           "  -- all *_id (hashes):" "\n" \
                                           "  triple_relations.id AS id" "\n" \
                                           "  ,s_uri_id" "\n" \
                                           "  ,s_blank_id" "\n" \
                                           "  ,p_uri_id" "\n" \
                                           "  ,o_uri_id" "\n" \
                                           "  ,o_blank_id" "\n" \
                                           "  ,o_lit_id" "\n" \
                                           "  ,o_literals.datatype_id AS o_datatype_id" "\n" \
                                           "  ,c_uri_id" "\n" \
                                           "  -- all joined values:" "\n" \
                                           "  ,s_uris.uri      AS s_uri" "\n" \
                                           "  ,s_blanks.blank  AS s_blank" "\n" \
                                           "  ,p_uris.uri      AS p_uri" "\n" \
                                           "  ,o_uris.uri      AS o_uri" "\n" \
                                           "  ,o_blanks.blank  AS o_blank" "\n" \
                                           "  ,o_literals.text AS o_text" "\n" \
                                           "  ,o_literals.language AS o_language" "\n" \
                                           "  ,o_lit_uris.uri  AS o_datatype" "\n" \
                                           "  ,c_uris.uri      AS c_uri" "\n" \
                                           "FROM triple_relations" "\n" \
                                           "LEFT OUTER JOIN so_uris    AS s_uris     ON triple_relations.s_uri_id   = s_uris.id" "\n" \
                                           "LEFT OUTER JOIN so_blanks  AS s_blanks   ON triple_relations.s_blank_id = s_blanks.id" "\n" \
                                           "INNER      JOIN p_uris     AS p_uris     ON triple_relations.p_uri_id   = p_uris.id" "\n" \
                                           "LEFT OUTER JOIN so_uris    AS o_uris     ON triple_relations.o_uri_id   = o_uris.id" "\n" \
                                           "LEFT OUTER JOIN so_blanks  AS o_blanks   ON triple_relations.o_blank_id = o_blanks.id" "\n" \
                                           "LEFT OUTER JOIN o_literals AS o_literals ON triple_relations.o_lit_id   = o_literals.id" "\n" \
                                           "LEFT OUTER JOIN t_uris     AS o_lit_uris ON o_literals.datatype_id      = o_lit_uris.id" "\n" \
                                           "LEFT OUTER JOIN c_uris     AS c_uris     ON triple_relations.c_uri_id   = c_uris.id" "\n" \
                                           ";" "\n" \
                                           "CREATE TRIGGER triples_insert INSTEAD OF INSERT ON triples" "\n" \
                                           "FOR EACH ROW BEGIN" "\n" \
                                           "  -- subject uri/blank" "\n" \
                                           "  INSERT OR IGNORE INTO so_uris   (id,uri)   VALUES (NEW.s_uri_id,      NEW.s_uri);" "\n" \
                                           "  INSERT OR IGNORE INTO so_blanks (id,blank) VALUES (NEW.s_blank_id,    NEW.s_blank);" "\n" \
                                           "  -- predicate uri" "\n" \
                                           "  INSERT OR IGNORE INTO p_uris    (id,uri)   VALUES (NEW.p_uri_id,      NEW.p_uri);" "\n" \
                                           "  -- object uri/blank" "\n" \
                                           "  INSERT OR IGNORE INTO so_uris   (id,uri)   VALUES (NEW.o_uri_id,      NEW.o_uri);" "\n" \
                                           "  INSERT OR IGNORE INTO so_blanks (id,blank) VALUES (NEW.o_blank_id,    NEW.o_blank);" "\n" \
                                           "  -- object literal" "\n" \
                                           "  INSERT OR IGNORE INTO t_uris    (id,uri)   VALUES (NEW.o_datatype_id, NEW.o_datatype);" "\n" \
                                           "  INSERT OR IGNORE INTO o_literals(id,datatype_id,language,text) VALUES (NEW.o_lit_id, NEW.o_datatype_id, NEW.o_language, NEW.o_text);" "\n" \
                                           "  -- context uri" "\n" \
                                           "  INSERT OR IGNORE INTO c_uris    (id,uri)   VALUES (NEW.c_uri_id,      NEW.c_uri);" "\n" \
                                           "  -- triple" "\n" \
                                           "  INSERT INTO triple_relations(id, s_uri_id, s_blank_id, p_uri_id, o_uri_id, o_blank_id, o_lit_id, c_uri_id)" "\n" \
                                           "  VALUES (NEW.id, NEW.s_uri_id, NEW.s_blank_id, NEW.p_uri_id, NEW.o_uri_id, NEW.o_blank_id, NEW.o_lit_id, NEW.c_uri_id);" "\n" \
                                           "END;" "\n" \
                                           "CREATE TRIGGER triples_delete INSTEAD OF DELETE ON triples" "\n" \
                                           "FOR EACH ROW BEGIN" "\n" \
                                           "  -- triple" "\n" \
                                           "  DELETE FROM triple_relations WHERE id = OLD.id;" "\n" \
                                           "  -- subject uri/blank" "\n" \
                                           "  DELETE FROM so_uris    WHERE (OLD.s_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE s_uri_id    = OLD.s_uri_id))      AND (id = OLD.s_uri_id);" "\n" \
                                           "  DELETE FROM so_blanks  WHERE (OLD.s_blank_id    IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE s_blank_id  = OLD.s_blank_id))    AND (id = OLD.s_blank_id);" "\n" \
                                           "  -- predicate uri" "\n" \
                                           "  DELETE FROM p_uris     WHERE (OLD.p_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE p_uri_id    = OLD.p_uri_id))      AND (id = OLD.p_uri_id);" "\n" \
                                           "  -- object uri/blank" "\n" \
                                           "  DELETE FROM so_uris    WHERE (OLD.o_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_uri_id    = OLD.o_uri_id))      AND (id = OLD.o_uri_id);" "\n" \
                                           "  DELETE FROM so_blanks  WHERE (OLD.o_blank_id    IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_blank_id  = OLD.o_blank_id))    AND (id = OLD.o_blank_id);" "\n" \
                                           "  -- object literal" "\n" \
                                           "  DELETE FROM o_literals WHERE (OLD.o_lit_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_lit_id    = OLD.o_lit_id))      AND (id = OLD.o_lit_id);" "\n" \
                                           "  DELETE FROM t_uris     WHERE (OLD.o_datatype_id IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM o_literals       WHERE datatype_id = OLD.o_datatype_id)) AND (id = OLD.o_datatype_id);" "\n" \
                                           "  -- context uri" "\n" \
                                           "  DELETE FROM c_uris     WHERE (OLD.c_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE c_uri_id    = OLD.c_uri_id))      AND (id = OLD.c_uri_id);" "\n" \
                                           "END;" "\n" \
                                           "PRAGMA user_version=1;" "\n" \
        ;

        const char schema_mig_to_2_sql[] = // generated via tools/sql2c.sh schema_mig_to_2.sql
                                           "DROP TRIGGER triples_delete;" "\n" \
                                           "CREATE TRIGGER triples_delete INSTEAD OF DELETE ON triples" "\n" \
                                           "FOR EACH ROW BEGIN" "\n" \
                                           "  -- subject uri/blank" "\n" \
                                           "  DELETE FROM so_uris    WHERE (OLD.s_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE s_uri_id    = OLD.s_uri_id))      AND (id = OLD.s_uri_id);" "\n" \
                                           "  DELETE FROM so_blanks  WHERE (OLD.s_blank_id    IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE s_blank_id  = OLD.s_blank_id))    AND (id = OLD.s_blank_id);" "\n" \
                                           "  -- predicate uri" "\n" \
                                           "  DELETE FROM p_uris     WHERE (OLD.p_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE p_uri_id    = OLD.p_uri_id))      AND (id = OLD.p_uri_id);" "\n" \
                                           "  -- object uri/blank" "\n" \
                                           "  DELETE FROM so_uris    WHERE (OLD.o_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_uri_id    = OLD.o_uri_id))      AND (id = OLD.o_uri_id);" "\n" \
                                           "  DELETE FROM so_blanks  WHERE (OLD.o_blank_id    IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_blank_id  = OLD.o_blank_id))    AND (id = OLD.o_blank_id);" "\n" \
                                           "  -- object literal" "\n" \
                                           "  DELETE FROM o_literals WHERE (OLD.o_lit_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_lit_id    = OLD.o_lit_id))      AND (id = OLD.o_lit_id);" "\n" \
                                           "  DELETE FROM t_uris     WHERE (OLD.o_datatype_id IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM o_literals       WHERE datatype_id = OLD.o_datatype_id)) AND (id = OLD.o_datatype_id);" "\n" \
                                           "  -- context uri" "\n" \
                                           "  DELETE FROM c_uris     WHERE (OLD.c_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE c_uri_id    = OLD.c_uri_id))      AND (id = OLD.c_uri_id);" "\n" \
                                           "  -- triple" "\n" \
                                           "  DELETE FROM triple_relations WHERE id = OLD.id;" "\n" \
                                           "END;" "\n" \
                                           "PRAGMA user_version=2;" "\n" \
        ;

        const char *const migrations[] = {
            schema_mig_to_1_sql,
            schema_mig_to_2_sql,
            NULL
        };
        {
            const size_t mig_count = array_length(migrations) - 1;
            assert(2 == mig_count && "migrations count wrong.");
            assert(!migrations[mig_count] && "migrations must be NULL terminated.");
            if( mig_count < schema_version ) {
                // schema is more recent than this source file knows to handle.
                pub_close(storage);
                return RET_ERROR;
            }
        }
        const sqlite_rc_t begin = transaction_start(storage);
        for( int v = schema_version; migrations[v]; v++ ) {
            if( SQLITE_OK != ( rc = exec_stmt(db_ctx->db, migrations[v]) ) ) {
                transaction_rollback(storage, begin);
                pub_close(storage);
                return rc;
            }
            {
                // assert new schema version
                sqlite3_stmt *stmt = NULL;
                prep_stmt(db_ctx->db, &stmt, "PRAGMA user_version");
                if( SQLITE_ROW != ( rc = sqlite3_step(stmt) ) ) {
                    sqlite3_finalize(stmt);
                    pub_close(storage);
                    return rc;
                }
                const int v_new = sqlite3_column_int(stmt, 0);
                if( SQLITE_DONE != ( rc = sqlite3_step(stmt) ) ) {
                    sqlite3_finalize(stmt);
                    pub_close(storage);
                    return rc;
                }
                sqlite3_finalize(stmt);
                assert(v + 1 == v_new && "invalid schema version after migration.");
            }
        }
        if( SQLITE_OK != ( rc = transaction_commit(storage, begin) ) ) {
            pub_close(storage);
            return rc;
        }
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
    const unsigned char *feat = librdf_uri_as_string(feature);
    if( !feat )
        return NULL;
    librdf_node *ret = NULL;
    librdf_uri *uri_xsd_boolean = librdf_new_uri(get_world(storage), (const unsigned char *)"http://www.w3.org/2000/10/XMLSchema#" "boolean");
    librdf_uri *uri_xsd_unsignedShort = librdf_new_uri(get_world(storage), "http://www.w3.org/2000/10/XMLSchema#" "unsignedShort");
    if( !ret && 0 == strcmp(LIBRDF_MODEL_FEATURE_CONTEXTS, (const char *)feat) )
        ret = librdf_new_node_from_typed_literal(get_world(storage), (const unsigned char *)"0", NULL, uri_xsd_boolean);
    if( !ret && 0 == strcmp(LIBRDF_STORAGE_SQLITE_MRO_ "feature/sql/cache/mask", (const char *)feat) ) {
        ret = librdf_new_node_from_typed_literal(get_world(storage), (const unsigned char *)"1023", NULL, uri_xsd_unsignedShort);
    }
    if( !ret && 0 == strcmp(LIBRDF_STORAGE_SQLITE_MRO_ "feature/profile", (const char *)feat) ) {
        ret = librdf_new_node_from_typed_literal(get_world(storage), (const unsigned char *)"1", NULL, uri_xsd_unsignedShort);
    }
    librdf_free_uri(uri_xsd_boolean);
    librdf_free_uri(uri_xsd_unsignedShort);
    return ret;
}


/**
 * librdf_storage_set_feature:
 * @storage: #librdf_storage object
 * @feature: #librdf_uri feature property
 * @value: #librdf_node feature property value
 *
 * Set the value of a storage feature.
 *
 * Return value: non 0 on failure (negative if no such feature)
 **/
static int pub_set_feature(librdf_storage *storage, librdf_uri *feature, librdf_node *value)
{
    if( !feature )
        return -1;
    const unsigned char *feat = librdf_uri_as_string(feature);
    if( !feat )
        return -1;
    instance_t *db_ctx = get_instance(storage);

    if( 0 == strcmp(LIBRDF_STORAGE_SQLITE_MRO_ "feature/sql/cache/mask", feat) ) {
        const char *val = librdf_node_get_literal_value(value);
        if( 0 == strcmp("0", val) ) {
            db_ctx->sql_cache_mask = 0;
        } else {
            const int i = atoi(val);
            if( 0 >= i ) {
                librdf_log(NULL, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "invalid value: <%s> \"%s\"^^xsd:unsignedShort", feat, val);
                return 3;
            }
            db_ctx->sql_cache_mask = (ALL_PARAMS - 1) & i; // clip range
        }
        librdf_log(NULL, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "good value: <%s> \"%d\"^^xsd:unsignedShort", feat, db_ctx->sql_cache_mask);
        return 0;
    }

    if( 0 == strcmp(LIBRDF_STORAGE_SQLITE_MRO_ "feature/profile", feat) ) {
        const char *val = librdf_node_get_literal_value(value);
        if( 0 == strcmp("1", val) || 0 == strcmp("true", val) )
            db_ctx->do_profile = BOOL_YES;
        else if( 0 == strcmp("0", val) || 0 == strcmp("false", val) )
            db_ctx->do_profile = BOOL_NO;
        else {
            librdf_log(NULL, 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "invalid value: <%s> \"%s\"^^xsd:boolean", feat, val);
            return 2;
        }
        return 0;
    }
    return 1;
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
    return SQLITE_ROW != ctx->rc;
}


static int pub_iter_next_statement(void *_ctx)
{
    assert(_ctx && "context mustn't be NULL");
    iterator_t *ctx = (iterator_t *)_ctx;
    if( pub_iter_end_of_stream(ctx) )
        return RET_ERROR;
    ctx->dirty = BOOL_YES;
    ctx->rc = sqlite3_step(ctx->stmt);
    if( pub_iter_end_of_stream(ctx) )
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
                const str_uri_t uri = column_uri_string(stm, IDX_S_URI);
                if( uri )
                    node = librdf_new_node_from_uri_string(w, uri);
                if( !node ) {
                    const str_blank_t blank = column_blank_string(stm, IDX_S_BLANK);
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
                const str_uri_t uri = column_uri_string(stm, IDX_P_URI);
                if( uri )
                    node = librdf_new_node_from_uri_string(w, uri);
                if( !node )
                    return NULL;
                librdf_statement_set_predicate(st, node);
            }
            {
                /* object */
                librdf_node *node = NULL;
                const str_uri_t uri = column_uri_string(stm, IDX_O_URI);
                if( uri )
                    node = librdf_new_node_from_uri_string(w, uri);
                if( !node ) {
                    const str_blank_t blank = column_blank_string(stm, IDX_O_BLANK);
                    if( blank )
                        node = librdf_new_node_from_blank_identifier(w, blank);
                }
                if( !node ) {
                    const str_lit_val_t val = (str_lit_val_t)sqlite3_column_text(stm, IDX_O_TEXT);
                    const str_lang_t lang = (str_lang_t)column_language(stm, IDX_O_LANGUAGE);
                    const str_uri_t uri = column_uri_string(stm, IDX_O_DATATYPE);
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
    sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_size), "SELECT COUNT(id) FROM triple_relations");
    const sqlite_rc_t rc = sqlite3_step(stmt);
    return SQLITE_ROW == rc ? sqlite3_column_int(stmt, 0) : 0;
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
    const sqlite_rc_t begin = transaction_start(storage);
    instance_t *db_ctx = get_instance(storage);

    librdf_node *s = librdf_statement_get_subject(statement);
    librdf_node *p = librdf_statement_get_predicate(statement);
    librdf_node *o = librdf_statement_get_object(statement);

    // build the bitmask of parameters to set (non-NULL)
    const int params = 0
                       | (LIBRDF_NODE_TYPE_RESOURCE == node_type(s) ? P_S_URI : 0)
                       | (LIBRDF_NODE_TYPE_BLANK == node_type(s) ? P_S_BLANK : 0)
                       | (LIBRDF_NODE_TYPE_RESOURCE == node_type(p) ? P_P_URI : 0)
                       | (LIBRDF_NODE_TYPE_RESOURCE == node_type(o) ? P_O_URI : 0)
                       | (LIBRDF_NODE_TYPE_BLANK == node_type(o) ? P_O_BLANK : 0)
                       | (LIBRDF_NODE_TYPE_LITERAL == node_type(o) ? P_O_TEXT : 0)
                       | (librdf_node_get_literal_value_datatype_uri(o) ? P_O_DATATYPE : 0)
                       | (librdf_node_get_literal_value_language(o) ? P_O_LANGUAGE : 0)
                       | (context_node ? P_C_URI : 0)
    ;
    assert(params <= ALL_PARAMS && "params bitmask overflow");
    assert(params < array_length(db_ctx->stmt_triple_finds) && "statement cache array overflow");

    const int idx = params; // might become more complex to save some memory in db_ctx->stmt_triple_finds - see https://github.com/mro/librdf.sqlite/issues/11#issuecomment-151959176
    sqlite3_stmt *stmt = db_ctx->stmt_triple_finds[idx];
    if( NULL == stmt ) {
        const char find_triples_sql[] = // generated via tools/sql2c.sh find_triples.sql
                                        " -- result columns must match as in enum idx_triple_column_t" "\n" \
                                        "SELECT" "\n" \
                                        " -- all *_id (hashes):" "\n" \
                                        "  id" "\n" \
                                        "  ,s_uri_id" "\n" \
                                        "  ,s_blank_id" "\n" \
                                        "  ,p_uri_id" "\n" \
                                        "  ,o_uri_id" "\n" \
                                        "  ,o_blank_id" "\n" \
                                        "  ,o_lit_id" "\n" \
                                        "  ,o_datatype_id" "\n" \
                                        "  ,c_uri_id" "\n" \
                                        " -- all values:" "\n" \
                                        "  ,s_uri" "\n" \
                                        "  ,s_blank" "\n" \
                                        "  ,p_uri" "\n" \
                                        "  ,o_uri" "\n" \
                                        "  ,o_blank" "\n" \
                                        "  ,o_text" "\n" \
                                        "  ,o_language" "\n" \
                                        "  ,o_datatype" "\n" \
                                        "  ,c_uri" "\n" \
                                        "FROM triples" "\n" \
                                        "WHERE 1" "\n" \
                                        " -- subject" "\n" \
                                        "AND s_uri_id   = :s_uri_id" "\n" \
                                        "AND s_blank_id = :s_blank_id" "\n" \
                                        "AND p_uri_id   = :p_uri_id" "\n" \
                                        " -- object" "\n" \
                                        "AND o_uri_id   = :o_uri_id" "\n" \
                                        "AND o_blank_id = :o_blank_id" "\n" \
                                        "AND o_lit_id   = :o_lit_id" "\n" \
                                        " -- context node" "\n" \
                                        "AND c_uri_id   = :c_uri_id" "\n" \
        ;

        // create a SQL working copy (on stack) to fiddle with
        char sql[sizeof(find_triples_sql)];
        strcpy(sql, find_triples_sql);
        // SQL-comment out the NULL parameter terms
        if( 0 == (P_S_URI & params) )
            strncpy(strstr(sql, "AND s_uri_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND s_uri_id' not found in find_triples.sql");
        if( 0 == (P_S_BLANK & params) )
            strncpy(strstr(sql, "AND s_blank_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND s_blank_id' not found in find_triples.sql");
        if( 0 == (P_P_URI & params) )
            strncpy(strstr(sql, "AND p_uri_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND p_uri_id' not found in find_triples.sql");
        if( 0 == (P_O_URI & params) )
            strncpy(strstr(sql, "AND o_uri_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND o_uri_id' not found in find_triples.sql");
        if( 0 == (P_O_BLANK & params) )
            strncpy(strstr(sql, "AND o_blank_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND o_blank_id' not found in find_triples.sql");
        if( 0 == (P_O_TEXT & params) )
            strncpy(strstr(sql, "AND o_lit_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND o_lit_id' not found in find_triples.sql");
        if( 0 == (P_C_URI & params) )
            strncpy(strstr(sql, "AND c_uri_id"), "-- ", 3);
        assert('-' != sql[0] && "'AND c_uri_id' not found in find_triples.sql");

        librdf_log(librdf_storage_get_world(storage), 0, LIBRDF_LOG_INFO, LIBRDF_FROM_STORAGE, NULL, "Created SQL statement #%d %s", idx, sql);
        stmt = db_ctx->stmt_triple_finds[idx] = prep_stmt(db_ctx->db, &stmt, sql);
    }
    /*
     *  if( BOOL_NO ) {
     *    // toggle via "profile" feature?
     *    librdf_log( librdf_storage_get_world(storage), 0, LIBRDF_LOG_INFO, LIBRDF_FROM_STORAGE, NULL, "%s", librdf_statement_to_string(statement) );
     *  }
     */
    const sqlite_rc_t rc = bind_stmt(db_ctx, statement, context_node, stmt);
    assert(SQLITE_OK == rc && "foo");

    if( BOOL_NO ) {
        // toggle via "profile" feature?
        printExplainQueryPlan(stmt);
    }

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
    if( !storage )
        return RET_ERROR;
    if( !statement )
        return RET_OK;
    // librdf_log( librdf_storage_get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "%s", librdf_statement_to_string(statement) );
    return NULL == find_statement(storage, context_node, statement, BOOL_YES) ? RET_ERROR : RET_OK;
}


static int pub_context_add_statements(librdf_storage *storage, librdf_node *context_node, librdf_stream *statement_stream)
{
    const sqlite_rc_t txn = transaction_start(storage);
    for( ; !librdf_stream_end(statement_stream); librdf_stream_next(statement_stream) ) {
        librdf_statement *stmt = librdf_stream_get_object(statement_stream);
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
    if( !statement )
        return RET_OK;
    if( !librdf_statement_is_complete(statement) )
        return RET_ERROR;
    assert(storage && "must be set");

    instance_t *db_ctx = get_instance(storage);

    const hash_t stmt_id = stmt_hash(statement, context_node, db_ctx->digest);
    assert(!isNULL_ID(stmt_id) && "mustn't be nil");

    sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_triple_delete), "DELETE FROM triples WHERE id = :stmt_id");
    {
        const sqlite_rc_t rc = bind_int(stmt, ":stmt_id", stmt_id);
        if( SQLITE_OK != rc )
            return rc;
    }
    const sqlite_rc_t rc = sqlite3_step(stmt);
    return SQLITE_DONE == rc ? RET_OK : rc;
}


static int pub_remove_statement(librdf_storage *storage, librdf_statement *statement)
{
    return pub_context_remove_statement(storage, NULL, statement);
}


#if 0
static int pub_context_remove_statements(librdf_storage *storage, librdf_node *context_node)
{
    const sqlite_rc_t txn = transaction_start(storage);
    for( librdf_statement *stmt = librdf_stream_get_object(statement_stream); !librdf_stream_end(statement_stream); librdf_stream_next(statement_stream) ) {
        const int rc = pub_context_remove_statement(storage, context_node, stmt);
        if( RET_OK != rc ) {
            transaction_rollback(storage, txn);
            return rc;
        }
    }
    return transaction_commit(storage, txn);
}


#endif


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
    // factory->context_remove_statements  = pub_context_remove_statements; is this a 'clear' ?
    factory->context_serialise          = pub_context_serialise;
    factory->find_statements_in_context = pub_context_find_statements;
    factory->get_contexts               = pub_get_contexts;
    factory->get_feature                = pub_get_feature;
    factory->set_feature                = pub_set_feature;
    factory->transaction_start          = pub_transaction_start;
    factory->transaction_commit         = pub_transaction_commit;
    factory->transaction_rollback       = pub_transaction_rollback;
}


void librdf_init_storage_sqlite_mro(librdf_world *world)
{
    librdf_storage_register_factory(world, LIBRDF_STORAGE_SQLITE_MRO, "SQLite", &register_factory);
}
