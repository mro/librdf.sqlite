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
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include <redland.h>
#include <rdf_storage.h>

#pragma mark Basic Types & Constants

#define RET_ERROR 1
#define RET_OK 0
#define F_OK 0

typedef enum {
	BOOL_NO = 0,
	BOOL_YES = 1
} t_boolean;


#define NULL_ID ( (t_index) - 1 )
#define isNULL_ID(x) ( (x) < 0 )

#define NULL_LANG ""
#define isNULL_LANG(x) ( (x) == NULL || (x)[0] == '\0' )

#define NULL_URI ( (const unsigned char *)"" )
#define isNULL_URI(x) ( (x) == NULL || (x)[0] == '\0' )

#define isNULL_BLANK(x) ( (x) == NULL || (x)[0] == '\0' )


static const char *const sqlite_synchronous_flags[4] = {
	"off", "normal", "full", NULL
};
#define SYNC_OFF 0
#define SYNC_NORMAL 1


typedef struct t_legacy_query t_legacy_query;
struct t_legacy_query
{
	unsigned char *query;
	t_legacy_query *next;
};

typedef struct
{
	librdf_storage *storage;
	sqlite3 *db;
	t_boolean is_new;
	const char *name;
	size_t name_len;
	int synchronous; /* -1 (not set), 0+ index into sqlite_synchronous_flags */
	t_boolean in_stream;
	t_legacy_query *in_stream_queries;
	t_boolean in_transaction;

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
t_instance;


#pragma mark librdf convenience


#define LIBRDF_MALLOC(type, size) (type)malloc(size)
#define LIBRDF_CALLOC(type, size, count) (type)calloc(count, size)
#define LIBRDF_FREE(type, ptr) free(ptr)
// #define LIBRDF_BAD_CAST(t, v) (t)(v)


static inline t_instance *get_instance(librdf_storage *storage)
{
	return (t_instance *)librdf_storage_get_instance(storage);
}


static inline librdf_world *get_world(librdf_storage *storage)
{
	return librdf_storage_get_world(storage);
}


static inline void librdf_free_hash_nullsafe(librdf_hash *hash)
{
	if( hash )
		librdf_free_hash(hash);
}


#pragma mark Sqlite Debug/Profile


/* https://sqlite.org/eqp.html#section_2
** Argument pStmt is a prepared SQL statement. This function compiles
** an EXPLAIN QUERY PLAN command to report on the prepared statement,
** and prints the report to stdout using printf().
*/
static int printExplainQueryPlan(sqlite3_stmt *pStmt)
{
	const char *zSql; /* Input SQL */
	char *zExplain; /* SQL with EXPLAIN QUERY PLAN prepended */
	sqlite3_stmt *pExplain; /* Compiled EXPLAIN QUERY PLAN command */
	int rc; /* Return code from sqlite3_prepare_v2() */

	zSql = sqlite3_sql(pStmt);
	if( zSql == 0 ) return SQLITE_ERROR;

	zExplain = sqlite3_mprintf("EXPLAIN QUERY PLAN %s", zSql);
	if( zExplain == 0 ) return SQLITE_NOMEM;

	rc = sqlite3_prepare_v2(sqlite3_db_handle(pStmt), zExplain, -1, &pExplain, 0);
	sqlite3_free(zExplain);
	if( rc != SQLITE_OK ) return rc;

	while( SQLITE_ROW == sqlite3_step(pExplain) ) {
		int iSelectid = sqlite3_column_int(pExplain, 0);
		int iOrder = sqlite3_column_int(pExplain, 1);
		int iFrom = sqlite3_column_int(pExplain, 2);
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


typedef int t_sqlite_rc;
typedef int32_t t_index;


static sqlite3_stmt *prep_stmt(sqlite3 *db, sqlite3_stmt **stmt_p, const char *zSql)
{
	assert(db && "db handle is NULL");
	assert(stmt_p && "statement is NULL");
	assert(zSql && "SQL is NULL");
	if( *stmt_p ) {
		assert(0 == strcmp(sqlite3_sql(*stmt_p), zSql) && "ouch");
		const t_sqlite_rc rc0 = sqlite3_reset(*stmt_p);
		assert(rc0 == SQLITE_OK && "couldn't reset SQL statement");
	} else {
		const char *remainder = NULL;
		const int len_zSql = (int)strlen(zSql) + 1;
		const t_sqlite_rc rc0 = sqlite3_prepare_v2(db, zSql, len_zSql, stmt_p, &remainder);
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
	const t_sqlite_rc rc = sqlite3_finalize(*pStmt);
	assert(rc == SQLITE_OK && "ouch");
	*pStmt = NULL;
}


static inline t_sqlite_rc bind_int(sqlite3_stmt *stmt, const char *name, const sqlite_int64 _id)
{
	assert(stmt && "stmt mandatory");
	assert(name && "name mandatory");
	const int idx = sqlite3_bind_parameter_index(stmt, name);
	return idx == 0 ? SQLITE_OK : sqlite3_bind_int64(stmt, idx, _id);
}


static inline t_sqlite_rc bind_text(sqlite3_stmt *stmt, const char *name, const unsigned char *text, const size_t text_len)
{
	assert(stmt && "stmt mandatory");
	assert(name && "name mandatory");
	const int idx = sqlite3_bind_parameter_index(stmt, name);
	return idx == 0 ? SQLITE_OK : ( text == NULL ? sqlite3_bind_null(stmt, idx) : sqlite3_bind_text(stmt, idx, (const char *)text, (int)text_len, SQLITE_STATIC) );
}


static inline t_sqlite_rc bind_null(sqlite3_stmt *stmt, const char *name)
{
	return bind_text(stmt, name, NULL, 0);
}


static inline t_sqlite_rc bind_uri(sqlite3_stmt *stmt, const char *name, librdf_uri *uri)
{
	size_t len = 0;
	return bind_text(stmt, name, uri == NULL ? NULL_URI : librdf_uri_as_counted_string(uri, &len), len);
}


static inline const unsigned char *column_uri_string(sqlite3_stmt *stmt, const int iCol)
{
	const unsigned char *ret = sqlite3_column_text(stmt, iCol);
	return isNULL_URI(ret) ? NULL : ret;
}


static inline const unsigned char *column_blank_string(sqlite3_stmt *stmt, const int iCol)
{
	const unsigned char *ret = sqlite3_column_text(stmt, iCol);
	return isNULL_BLANK(ret) ? NULL : ret;
}


static inline const char *column_language(sqlite3_stmt *stmt, const int iCol)
{
	const char *ret = (const char *)sqlite3_column_text(stmt, iCol);
	return isNULL_LANG(ret) ? NULL : ret;
}


static t_index insert(sqlite3 *db, sqlite3_stmt *stmt)
{
	const t_sqlite_rc rc3 = sqlite3_step(stmt);
	if( rc3 == SQLITE_DONE ) {
		const sqlite3_int64 r = sqlite3_last_insert_rowid(db);
		const t_sqlite_rc rc2 = sqlite3_reset(stmt);
		assert(rc2 == SQLITE_OK && "ouch");
		return (t_index)r;
	}
	return -rc3;
}


static t_sqlite_rc transaction_start(librdf_storage *storage)
{
	t_instance *db_ctx = get_instance(storage);
	if( db_ctx->in_transaction )
		return SQLITE_MISUSE;
	const t_sqlite_rc rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_start), "BEGIN IMMEDIATE TRANSACTION;") );
	db_ctx->in_transaction = rc == SQLITE_DONE;
	assert(db_ctx->in_transaction != BOOL_NO && "ouch");
	return rc == SQLITE_DONE ? SQLITE_OK : rc;
}


static t_sqlite_rc transaction_commit(librdf_storage *storage, const t_sqlite_rc begin)
{
	if( begin != SQLITE_OK )
		return SQLITE_OK;
	t_instance *db_ctx = get_instance(storage);
	if( db_ctx->in_transaction == BOOL_NO )
		return RET_ERROR;
	const t_sqlite_rc rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_commit), "COMMIT  TRANSACTION;") );
	db_ctx->in_transaction = !(rc == SQLITE_DONE);
	assert(db_ctx->in_transaction == BOOL_NO && "ouch");
	return rc == SQLITE_DONE ? SQLITE_OK : rc;
}


static t_sqlite_rc transaction_rollback(librdf_storage *storage, const t_sqlite_rc begin)
{
	if( begin != SQLITE_OK )
		return SQLITE_OK;
	t_instance *db_ctx = get_instance(storage);
	if( db_ctx->in_transaction == BOOL_NO )
		return RET_ERROR;
	const t_sqlite_rc rc = sqlite3_step( prep_stmt(db_ctx->db, &(db_ctx->stmt_txn_rollback), "ROLLBACK TRANSACTION;") );
	db_ctx->in_transaction = !(rc == SQLITE_DONE);
	assert(db_ctx->in_transaction == BOOL_NO && "ouch");
	return rc == SQLITE_DONE ? SQLITE_OK : rc;
}


#pragma mark -


#pragma mark Legacy


static int librdf_storage_sqlite_exec(librdf_storage *storage, const char *request, sqlite3_callback callback, void *arg, t_boolean fail_ok)
{
	t_instance *db_ctx = get_instance(storage);

	/* sqlite crashes if passed in a NULL sql string */
	if( !request )
		return RET_ERROR;

#if defined(LIBRDF_DEBUG) && LIBRDF_DEBUG > 2
	LIBRDF_DEBUG2("SQLite exec '%s'\n", request);
#endif

	char *errmsg = NULL;
	t_sqlite_rc status = sqlite3_exec(db_ctx->db, (const char *)request, callback, arg, &errmsg);
	if( fail_ok )
		status = SQLITE_OK;

	if( status != SQLITE_OK ) {
		if( status == SQLITE_LOCKED && !callback && db_ctx->in_stream ) {
			/* error message from sqlite3_exec needs to be freed on both sqlite 2 and 3 */
			if( errmsg )
				sqlite3_free(errmsg);

			t_legacy_query *query = LIBRDF_CALLOC( t_legacy_query *, 1, sizeof(*query) );
			if( !query )
				return RET_ERROR;

			const size_t query_buf = strlen( (char *)request ) + 1;
			query->query = LIBRDF_MALLOC(unsigned char *, query_buf);
			if( !query->query ) {
				LIBRDF_FREE(librdf_storage_sqlite_query, query);
				return RET_ERROR;
			}

			strncpy( (char *)query->query, (char *)request, query_buf );

			if( !db_ctx->in_stream_queries )
				db_ctx->in_stream_queries = query;
			else {
				t_legacy_query *q = db_ctx->in_stream_queries;
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


static t_index create_uri(t_instance *db_ctx, librdf_uri *uri, sqlite3_stmt *stmt)
{
	if( !uri )
		return NULL_ID;
	assert(stmt && "Statement is null");
	size_t len = 0;
	const char *txt = (const char *)librdf_uri_as_counted_string(uri, &len);
	const t_sqlite_rc rc0 = sqlite3_bind_text(stmt, 1, txt, (int)len, SQLITE_STATIC);
	assert(rc0 == SQLITE_OK && "bind fail");
	return insert(db_ctx->db, stmt);
}


static inline t_index create_uri_so(t_instance *db_ctx, librdf_node *node)
{
	assert(node && "node must be set.");
	return create_uri( db_ctx, librdf_node_get_uri(node), prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_so_set), "INSERT INTO " TABLE_URIS_SO_NAME " (uri) VALUES (?)") );
}


static inline t_index create_uri_p(t_instance *db_ctx, librdf_node *node)
{
	assert(node && "node must be set.");
	return create_uri( db_ctx, librdf_node_get_uri(node), prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_p_set), "INSERT INTO " TABLE_URIS_P_NAME " (uri) VALUES (?)") );
}


static inline t_index create_uri_c(t_instance *db_ctx, librdf_node *node)
{
	assert(node && "node must be set.");
	return create_uri( db_ctx, librdf_node_get_uri(node), prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_c_set), "INSERT INTO " TABLE_URIS_C_NAME " (uri) VALUES (?)") );
}


static inline t_index create_uri_t(t_instance *db_ctx, librdf_node *node)
{
	assert(node && "node must be set.");
	librdf_uri *uri = librdf_node_get_literal_value_datatype_uri(node);
	return create_uri( db_ctx, uri, prep_stmt(db_ctx->db, &(db_ctx->stmt_uri_t_set), "INSERT INTO " TABLE_URIS_T_NAME " (uri) VALUES (?)") );
}


static t_index create_blank(t_instance *db_ctx, librdf_node *node, sqlite3_stmt *stmt)
{
	assert(node && "node mustn't be NULL.");
	if( LIBRDF_NODE_TYPE_BLANK != librdf_node_get_type(node) )
		return NULL_ID;
	size_t len = 0;
	unsigned char *val = librdf_node_get_counted_blank_identifier(node, &len);
	if( val == NULL )
		return NULL_ID;
	const t_sqlite_rc rc0 = sqlite3_bind_text(stmt, 1, (const char *)val, (int)len, SQLITE_STATIC);
	assert(rc0 == SQLITE_OK && "bind fail");
	return insert(db_ctx->db, stmt);
}


static inline t_index create_blank_so(t_instance *db_ctx, librdf_node *node)
{
	assert(node && "node must be set.");
	return create_blank( db_ctx, node, prep_stmt(db_ctx->db, &(db_ctx->stmt_blank_so_set), "INSERT INTO " TABLE_BLANKS_SO_NAME " (blank) VALUES (?)") );
}


static t_index create_literal(t_instance *db_ctx, librdf_node *node, const t_index datatype_id, sqlite3_stmt *stmt)
{
	assert(node && "node mustn't be NULL.");
	if( LIBRDF_NODE_TYPE_LITERAL != librdf_node_get_type(node) )
		return NULL_ID;
	// void * p = librdf_node_get_literal_value_datatype_uri(node);
	size_t len = 0;
	unsigned char *txt = librdf_node_get_literal_value_as_counted_string(node, &len);
	char *language = librdf_node_get_literal_value_language(node);
	if( language == NULL )
		language = NULL_LANG;
	const t_sqlite_rc rc0 = sqlite3_bind_text(stmt, 1, (const char *)txt, (int)len, SQLITE_STATIC);
	assert(rc0 == SQLITE_OK && "bind fail");
	const t_sqlite_rc rc1 = sqlite3_bind_int64(stmt, 2, datatype_id);
	assert(rc1 == SQLITE_OK && "bind fail");
	const t_sqlite_rc rc2 = sqlite3_bind_text(stmt, 3, language, -1, SQLITE_STATIC);
	assert(rc2 == SQLITE_OK && "bind fail");
	return insert(db_ctx->db, stmt);
}


static inline t_index create_literal_o(t_instance *db_ctx, librdf_node *node, const t_index datatype_id)
{
	assert(node && "node must be set.");
	return create_literal( db_ctx, node, datatype_id, prep_stmt(db_ctx->db, &(db_ctx->stmt_literal_set), "INSERT INTO " TABLE_LITERALS_NAME " (text,datatype,language) VALUES (?,?,?)") );
}


typedef enum {
	TRIPLE_NODE_URI = 0,
	TRIPLE_NODE_BLANK,
	TRIPLE_NODE_LITERAL,
	TRIPLE_NODE_NONE,
}
triple_node_type; // evtl. map to librdf_node_type ?


static inline triple_node_type triple_node_type_(const librdf_node_type t)
{
	switch( t ) {
	case LIBRDF_NODE_TYPE_RESOURCE: return TRIPLE_NODE_URI;
	case LIBRDF_NODE_TYPE_BLANK: return TRIPLE_NODE_BLANK;
	case LIBRDF_NODE_TYPE_LITERAL: return TRIPLE_NODE_LITERAL;
	case LIBRDF_NODE_TYPE_UNKNOWN: return TRIPLE_NODE_NONE;
	}
	assert(0 && "Fallthrough");
	return -1;
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
t_part;


static t_sqlite_rc bind_stmt(sqlite3_stmt *stmt, librdf_node *context_node, librdf_statement *statement)
{
	t_sqlite_rc rc = SQLITE_OK;
	size_t len = 0;
	triple_node_type t = TRIPLE_NODE_NONE;
	t_part i = PART_S_U;
	// add some constants for reporting results back
	// node type markers
	rc = bind_int(stmt, ":t_uri", t = TRIPLE_NODE_URI);
	rc = bind_int(stmt, ":t_blank", t = TRIPLE_NODE_BLANK);
	rc = bind_int(stmt, ":t_literal", t = TRIPLE_NODE_LITERAL);
	rc = bind_int(stmt, ":t_none", t = TRIPLE_NODE_NONE);
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
	t = s == NULL ? TRIPLE_NODE_NONE : triple_node_type_( librdf_node_get_type(s) );
	rc = bind_int(stmt, ":s_type", t);
	if( t == TRIPLE_NODE_NONE ) {
		rc = bind_null(stmt, ":s_uri");
		rc = bind_null(stmt, ":s_blank");
	} else {
		rc = bind_uri( stmt, ":s_uri", librdf_node_get_uri(s) );
		rc = bind_text(stmt, ":s_blank", librdf_node_get_counted_blank_identifier(s, &len), len);
	}

	librdf_node *p = librdf_statement_get_predicate(statement);
	t = p == NULL ? TRIPLE_NODE_NONE : triple_node_type_( librdf_node_get_type(p) );
	rc = bind_int(stmt, ":p_type", t);
	if( t == TRIPLE_NODE_NONE )
		rc = bind_null(stmt, ":p_uri");
	else
		rc = bind_uri( stmt, ":p_uri", librdf_node_get_uri(p) );

	librdf_node *o = librdf_statement_get_object(statement);
	t = o == NULL ? TRIPLE_NODE_NONE : triple_node_type_( librdf_node_get_type(o) );
	rc = bind_int(stmt, ":o_type", t);
	if( t == TRIPLE_NODE_NONE ) {
		rc = bind_null(stmt, ":o_uri");
		rc = bind_null(stmt, ":o_blank");
		rc = bind_null(stmt, ":o_text");
		rc = bind_null(stmt, ":o_datatype");
		rc = bind_null(stmt, ":o_language");
	} else {
		rc = bind_uri( stmt, ":o_uri", librdf_node_get_uri(o) );
		rc = bind_text(stmt, ":o_blank", librdf_node_get_counted_blank_identifier(o, &len), len);
		rc = bind_text(stmt, ":o_text", librdf_node_get_literal_value_as_counted_string(o, &len), len);
		rc = bind_uri( stmt, ":o_datatype", librdf_node_get_literal_value_datatype_uri(o) );
		char *language = librdf_node_get_literal_value_language(o);
		rc = bind_text(stmt, ":o_language", (const unsigned char *)(language ? language : NULL_LANG), -1);
	}

	rc = bind_int(stmt, ":c_wild", context_node == NULL);
	rc = bind_uri( stmt, ":c_uri", context_node == NULL ? NULL : librdf_node_get_uri(context_node) );

	return SQLITE_OK;
}


static t_index create_part(t_instance *inst, librdf_node *node, const t_part part, t_index ids[])
{
	assert(node && "node must be set.");
	if( !isNULL_ID(ids[part]) )
		return ids[part];
	switch( part ) {
	case PART_S_U:
		return librdf_node_is_resource(node) ? create_uri_so(inst, node) : NULL_ID;
	case PART_S_B:
		return librdf_node_is_blank(node) ? create_blank_so(inst, node) : NULL_ID;
	case PART_P_U:
		return librdf_node_is_resource(node) ? create_uri_p(inst, node) : NULL_ID;
	case PART_O_U:
		return librdf_node_is_resource(node) ? create_uri_so(inst, node) : NULL_ID;
	case PART_O_B:
		return librdf_node_is_blank(node) ? create_blank_so(inst, node) : NULL_ID;
	case PART_O_L:
		return librdf_node_is_literal(node) ? create_literal_o(inst, node, ids[PART_DATATYPE]) : NULL_ID;
	case PART_DATATYPE:
		return librdf_node_is_literal(node) ? create_uri_t(inst, node) : NULL_ID;
	case PART_C:
		return librdf_node_is_resource(node) ? create_uri_c(inst, node) : NULL_ID;
	case PART_TRIPLE:
		break;
	}
	assert(0 && "Fallthrough");
	return NULL_ID;
}


static librdf_statement *find_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement, t_boolean create)
{
	const t_sqlite_rc begin = transaction_start(storage);
	t_instance *db_ctx = get_instance(storage);

	// const t_index size = pub_size(storage);

	// find already existing parts
	t_index part_ids[PART_TRIPLE + 1] = {
		NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID, NULL_ID
	};
	{
		const char *find_parts_sql = // generated via ./tools/sql2c.sh find_parts.sql
					     "" "\n" \
					     "-- find triple parts (nodes)" "\n" \
					     "" "\n" \
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
		const t_sqlite_rc rc3 = bind_stmt(stmt, context_node, statement);
		for( t_sqlite_rc rc = sqlite3_step(stmt); rc == SQLITE_ROW; rc = sqlite3_step(stmt) ) {
			switch( rc ) {
			case SQLITE_ROW: {
				const t_part idx = (t_part)sqlite3_column_int(stmt, 0);
				// const triple_node_type type = (triple_node_type)sqlite3_column_int(stmt, 1);
				part_ids[idx] = sqlite3_column_int(stmt, 2);
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
		part_ids[PART_S_U] = create_part(db_ctx, s, PART_S_U, part_ids);
		part_ids[PART_S_B] = create_part(db_ctx, s, PART_S_B, part_ids);
		part_ids[PART_P_U] = create_part(db_ctx, p, PART_P_U, part_ids);
		part_ids[PART_O_U] = create_part(db_ctx, o, PART_O_U, part_ids);
		part_ids[PART_O_B] = create_part(db_ctx, o, PART_O_B, part_ids);
		part_ids[PART_DATATYPE] = create_part(db_ctx, o, PART_DATATYPE, part_ids);
		part_ids[PART_O_L] = create_part(db_ctx, o, PART_O_L, part_ids);
		if( context_node )
			part_ids[PART_C] = create_part(db_ctx, context_node, PART_C, part_ids);
	}
	{
		// check if we have such a triple (spocs)
		const char *lookup_triple_sql = "SELECT rowid FROM " TABLE_TRIPLES_NAME "\n"	 \
						"WHERE s_uri=? AND s_blank=? AND p_uri=? AND o_uri=? AND o_blank=? AND o_lit=? AND c_uri=?";
		sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_spocs_get), lookup_triple_sql);
		for( int i = PART_C; i >= 0; i-- ) {
			const t_sqlite_rc rc_ = sqlite3_bind_int64(stmt, i + 1, part_ids[i]);
			assert(rc_ == SQLITE_OK && "ouch");
		}
		const t_sqlite_rc rc = sqlite3_step(stmt);
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
		const char *insert_triple_sql = "INSERT INTO " TABLE_TRIPLES_NAME "\n"	 \
						"(s_uri,s_blank,p_uri,o_uri,o_blank,o_lit,c_uri) VALUES" "\n" \
						"(    ?,      ?,    ?,    ?,      ?,    ?,    ?)";
		sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_spocs_set), insert_triple_sql);
		for( int i = PART_C; i >= 0; i-- ) {
			const t_sqlite_rc rc = sqlite3_bind_int64(stmt, i + 1, part_ids[i]);
			assert(rc == SQLITE_OK && "ouch");
		}
		const t_sqlite_rc rc = sqlite3_step(stmt);
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
		librdf_free_hash_nullsafe(options);
		return RET_ERROR;
	}

	t_instance *db_ctx = LIBRDF_CALLOC( t_instance *, 1, sizeof(*db_ctx) );
	if( !db_ctx ) {
		librdf_free_hash_nullsafe(options);
		return RET_ERROR;
	}

	librdf_storage_set_instance(storage, db_ctx);
	db_ctx->storage = storage;
	db_ctx->name_len = strlen(name);
	char *name_copy = LIBRDF_MALLOC(char *, db_ctx->name_len + 1);
	if( !name_copy ) {
		librdf_free_hash_nullsafe(options);
		return RET_ERROR;
	}

	strncpy(name_copy, name, db_ctx->name_len + 1);
	db_ctx->name = name_copy;

	if( librdf_hash_get_as_boolean(options, "new") != BOOL_NO )
		db_ctx->is_new = BOOL_YES;  /* default is NOT NEW */

	/* Redland default is "PRAGMA synchronous normal" */
	db_ctx->synchronous = SYNC_NORMAL;

	char *synchronous = synchronous = librdf_hash_get(options, "synchronous");
	if( synchronous ) {
		for( int i = 0; sqlite_synchronous_flags[i]; i++ ) {
			if( !strcmp(synchronous, sqlite_synchronous_flags[i]) ) {
				db_ctx->synchronous = i;
				break;
			}
		}
		LIBRDF_FREE(char *, synchronous);
	}

	librdf_free_hash_nullsafe(options);
	return RET_OK;
}


static void pub_terminate(librdf_storage *storage)
{
	t_instance *db_ctx = get_instance(storage);
	if( db_ctx == NULL )
		return;
	if( db_ctx->name )
		LIBRDF_FREE(char *, (void *)db_ctx->name);
	LIBRDF_FREE(librdf_storage_sqlite_terminate, db_ctx);
}


static int pub_close(librdf_storage *storage);


static int pub_open(librdf_storage *storage, librdf_model *model)
{
	t_instance *db_ctx = get_instance(storage);
	const t_boolean db_file_exists = 0 == access(db_ctx->name, F_OK);
	if( db_ctx->is_new && db_file_exists )
		unlink(db_ctx->name);

	// open DB
	db_ctx->db = NULL;
	{
		const int rc = sqlite3_open(db_ctx->name, &db_ctx->db);
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

	char request[250];
	// set DB PRAGMA 'synchronous'
	if( db_ctx->synchronous >= SYNC_OFF ) {
		const size_t len = snprintf(request, sizeof(request) - 1, "PRAGMA synchronous=%s;", sqlite_synchronous_flags[db_ctx->synchronous]);
		assert(len < sizeof(request) && "buffer too small.");
		const int rc = librdf_storage_sqlite_exec(storage, request, NULL, NULL, 0);
		if( rc ) {
			pub_close(storage);
			return RET_ERROR;
		}
	}

	// create tables and indices if required.
	if( db_ctx->is_new ) {
		const t_sqlite_rc begin = transaction_start(storage);

		const char *schema_sql = // generated via tools/sql2c.sh schema.sql
					 "" "\n" \
					 "CREATE TABLE so_uris (" "\n" \
					 "  id INTEGER PRIMARY KEY" "\n" \
					 "  ,uri TEXT NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX so_uris_index ON so_uris (uri);" "\n" \
					 "" "\n" \
					 "CREATE TABLE so_blanks (" "\n" \
					 "  id INTEGER PRIMARY KEY" "\n" \
					 "  ,blank TEXT NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX so_blanks_index ON so_blanks (blank);" "\n" \
					 "" "\n" \
					 "CREATE TABLE p_uris (" "\n" \
					 "  id INTEGER PRIMARY KEY" "\n" \
					 "  ,uri TEXT NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX p_uris_index ON p_uris (uri);" "\n" \
					 "" "\n" \
					 "CREATE TABLE o_literals (" "\n" \
					 "  id INTEGER PRIMARY KEY" "\n" \
					 "  ,datatype INTEGER NOT NULL" "\n" \
					 "  ,text TEXT NOT NULL" "\n" \
					 "  ,language TEXT NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX o_literals_index ON o_literals (text,language,datatype);" "\n" \
					 "" "\n" \
					 "CREATE TABLE t_uris (" "\n" \
					 "  id INTEGER PRIMARY KEY" "\n" \
					 "  ,uri TEXT NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX t_uris_index ON t_uris (uri);" "\n" \
					 "" "\n" \
					 "CREATE TABLE c_uris (" "\n" \
					 "  id INTEGER PRIMARY KEY" "\n" \
					 "  ,uri TEXT NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX c_uris_index ON c_uris (uri);" "\n" \
					 "" "\n" \
					 "-- ensure dummies for -1 so as we can relate NOT NULL in spocs" "\n" \
					 "INSERT INTO so_uris (id, uri) VALUES (-1,'');" "\n" \
					 "INSERT INTO so_blanks (id, blank) VALUES (-1,'');" "\n" \
					 "INSERT INTO p_uris (id, uri) VALUES (-1,'');" "\n" \
					 "INSERT INTO c_uris (id, uri) VALUES (-1,'');" "\n" \
					 "INSERT INTO t_uris (id, uri) VALUES (-1,'');" "\n" \
					 "" "\n" \
					 "CREATE TABLE spocs (" "\n" \
					 "  s_uri INTEGER NOT NULL" "\n" \
					 "  ,s_blank INTEGER NOT NULL" "\n" \
					 "  ,p_uri INTEGER NOT NULL" "\n" \
					 "  ,o_uri INTEGER NOT NULL" "\n" \
					 "  ,o_blank INTEGER NOT NULL" "\n" \
					 "  ,o_lit INTEGER NOT NULL" "\n" \
					 "  ,c_uri INTEGER NOT NULL" "\n" \
					 ");" "\n" \
					 "CREATE UNIQUE INDEX spocs_index ON spocs (s_uri,s_blank,p_uri,o_uri,o_blank,o_lit,c_uri);" "\n" \
					 "-- CREATE INDEX spocs_index_so ON spocs (s_uri,o_uri);" "\n" \
					 "-- CREATE INDEX spocs_index_p ON spocs (p_uri);" "\n"	\
					 "-- CREATE INDEX spocs_index_s_blank ON spocs (s_blank,p_uri);" "\n" \
					 "-- CREATE INDEX spocs_index_o_blank ON spocs (o_blank,p_uri);" "\n" \
					 "-- CREATE INDEX spocs_index_o_lit ON spocs (o_lit);" "\n" \
					 "" "\n" \
					 "-----------------------------------------------------------" "\n" \
					 "-- Convenience VIEW" "\n" \
					 "-----------------------------------------------------------" "\n" \
					 "" "\n" \
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
					 "LEFT OUTER JOIN o_literals AS o_literals ON spocs.o_lit = o_literals.id" "\n"	\
					 "LEFT OUTER JOIN t_uris     AS o_lit_uris ON o_literals.datatype   = o_lit_uris.id" "\n" \
					 "INNER JOIN c_uris     AS c_uris     ON spocs.c_uri      = c_uris.id" "\n" \
					 ";" "\n" \
					 "" "\n" \
					 "PRAGMA user_version=1;" "\n" \
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


static int pub_close(librdf_storage *storage)
{
	t_instance *db_ctx = get_instance(storage);

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

	const int rc = sqlite3_close(db_ctx->db);
	if( rc != SQLITE_OK ) {
		char *errmsg = (char *)sqlite3_errmsg(db_ctx->db);
		librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
			   "SQLite database %s close failed - %s", db_ctx->name, errmsg);
		pub_close(storage);
		return RET_ERROR;
	}
	return RET_OK;
}


static librdf_iterator *pub_get_contexts(librdf_storage *storage)
{
	assert(0 && "not implemented yet.");
	return NULL;
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


static t_sqlite_rc pub_transaction_start(librdf_storage *storage)
{
	return transaction_start(storage);
}


static t_sqlite_rc pub_transaction_commit(librdf_storage *storage)
{
	return transaction_commit(storage, SQLITE_OK);
}


static t_sqlite_rc pub_transaction_rollback(librdf_storage *storage)
{
	return transaction_rollback(storage, SQLITE_OK);
}


#pragma mark Iterator



typedef struct
{
	librdf_storage *storage;
	librdf_statement *statement;
	librdf_node *context;

	sqlite3_stmt *stmt;
	t_sqlite_rc txn;
	t_sqlite_rc rc;
	t_boolean dirty;
}
t_stream;


static int pub_stream_end_of_stream(void *_ctx)
{
	assert(_ctx && "context mustn't be NULL");
	t_stream *ctx = (t_stream *)_ctx;
	return ctx->rc != SQLITE_ROW;
}


static int pub_stream_next_statement(void *_ctx)
{
	assert(_ctx && "context mustn't be NULL");
	if( pub_stream_end_of_stream(_ctx) )
		return RET_ERROR;
	t_stream *ctx = (t_stream *)_ctx;
	ctx->dirty = BOOL_YES;
	ctx->rc = sqlite3_step(ctx->stmt);
	if( pub_stream_end_of_stream(_ctx) )
		return RET_ERROR;
	return RET_OK;
}


static void *pub_stream_get_statement(void *_ctx, const int _flags)
{
	assert(_ctx && "context mustn't be NULL");
	const librdf_iterator_get_method_flags flags = (librdf_iterator_get_method_flags)_flags;
	t_stream *ctx = (t_stream *)_ctx;
	librdf_world *w = get_world(ctx->storage);

	switch( flags ) {
	case LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT: {
		if( ctx->dirty && !pub_stream_end_of_stream(_ctx) ) {
			assert(ctx->statement && "statement mustn't be NULL");
			librdf_statement_clear(ctx->statement);
			librdf_statement *st = ctx->statement;
			// stmt columns refer to find_triples_sql
			{
				/* subject */
				librdf_node *node = NULL;
				const unsigned char *uri = column_uri_string(ctx->stmt, 0);
				if( uri )
					node = librdf_new_node_from_uri_string(w, uri);
				if( !node ) {
					const unsigned char *blank = column_blank_string(ctx->stmt, 1);
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
				const unsigned char *uri = column_uri_string(ctx->stmt, 2);
				if( uri )
					node = librdf_new_node_from_uri_string(w, uri);
				if( !node )
					return NULL;
				librdf_statement_set_predicate(st, node);
			}
			{
				/* object */
				librdf_node *node = NULL;
				const unsigned char *uri = column_uri_string(ctx->stmt, 3);
				if( uri )
					node = librdf_new_node_from_uri_string(w, uri);
				if( !node ) {
					const unsigned char *blank = column_blank_string(ctx->stmt, 4);
					if( blank )
						node = librdf_new_node_from_blank_identifier(w, blank);
				}
				if( !node ) {
					const unsigned char *val = sqlite3_column_text(ctx->stmt, 5);
					const char *lang = (const char *)column_language(ctx->stmt, 6);
					const unsigned char *uri = column_uri_string(ctx->stmt, 7);
					librdf_uri *t = uri ? librdf_new_uri(w, uri) : NULL;
					node = librdf_new_node_from_typed_literal(w, val, lang, t);
					librdf_free_uri(t);
				}
				if( !node )
					return NULL;
				librdf_statement_set_object(st, node);
			}
			assert(librdf_statement_is_complete(st) && "hu?");
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


static void pub_stream_finished(void *_ctx)
{
	assert(_ctx && "context mustn't be NULL");
	t_stream *ctx = (t_stream *)_ctx;
	if( ctx->statement )
		librdf_free_statement(ctx->statement);
	librdf_storage_remove_reference(ctx->storage);
	transaction_rollback(ctx->storage, ctx->txn);
	sqlite3_finalize(ctx->stmt);
	LIBRDF_FREE(t_stream *, ctx);
}


#pragma mark Query


static librdf_stream *pub_context_find_statements(librdf_storage *storage, librdf_statement *statement, librdf_node *context_node)
{
	t_sqlite_rc begin = transaction_start(storage);

	t_instance *db_ctx = get_instance(storage);
	const char *find_triples_sql = // generated via tools/sql2c.sh find_triples.sql
				       "-- result columns must match as used in pub_stream_get_statement" "\n" \
				       "SELECT" "\n" \
				       "  s_uri" "\n" \
				       "  ,s_blank" "\n" \
				       "  ,p_uri" "\n" \
				       "  ,o_uri" "\n" \
				       "  ,o_blank" "\n" \
				       "  ,o_text" "\n"	      \
				       "  ,o_language" "\n" \
				       "  ,o_datatype" "\n" \
				       "  ,c_uri" "\n" \
				       "FROM spocs_full" "\n" \
				       "WHERE" "\n" \
				       "-- subject" "\n" \
				       "    ((:s_type != :t_uri)     OR (s_uri = :s_uri))" "\n"	      \
				       "AND ((:s_type != :t_blank)   OR (s_blank = :s_blank))" "\n" \
				       "-- predicate" "\n" \
				       "AND ((:p_type != :t_uri)     OR (p_uri = :p_uri))" "\n"	      \
				       "-- object" "\n"	      \
				       "AND ((:o_type != :t_uri)     OR (o_uri = :o_uri))" "\n"	      \
				       "AND ((:o_type != :t_blank)   OR (o_blank = :o_blank))" "\n" \
				       "AND ((:o_type != :t_literal) OR (o_text = :o_text AND o_datatype = :o_datatype AND o_language = :o_language))" "\n" \
				       "-- context node" "\n" \
				       "AND ((:c_wild)               OR (c_uri = :c_uri))" "\n"	      \
	;

	sqlite3_stmt *stmt = NULL;
	stmt = prep_stmt(db_ctx->db, &stmt, find_triples_sql);

	// librdf_log( librdf_storage_get_world(storage), 0, LIBRDF_LOG_INFO, LIBRDF_FROM_STORAGE, NULL, "%s", librdf_statement_to_string(statement) );

	const t_sqlite_rc rc = bind_stmt(stmt, context_node, statement);
	assert(rc == SQLITE_OK && "foo");

	// printExplainQueryPlan(stmt);

	// create iterator
	t_stream *sctx = LIBRDF_CALLOC(t_stream *, sizeof(t_stream), 1);
	sctx->storage = storage;
	sctx->context = context_node;
	sctx->stmt = stmt;
	sctx->txn = begin;
	sctx->rc = sqlite3_step(stmt);
	sctx->statement = librdf_new_statement( get_world(storage) );
	sctx->dirty = BOOL_YES;

	librdf_storage_add_reference(sctx->storage);
	librdf_stream *stream = librdf_new_stream(get_world(storage), sctx, &pub_stream_end_of_stream, &pub_stream_next_statement, &pub_stream_get_statement, &pub_stream_finished);

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


static int pub_contains_statement(librdf_storage *storage, librdf_statement *statement)
{
	return NULL != find_statement(storage, NULL, statement, BOOL_NO);
}


static int pub_size(librdf_storage *storage)
{
	t_instance *db_ctx = get_instance(storage);
	t_sqlite_rc begin = transaction_start(storage);

	sqlite3_stmt *stmt = prep_stmt(db_ctx->db, &(db_ctx->stmt_size), "SELECT COUNT(rowid) FROM " TABLE_TRIPLES_NAME);
	const t_sqlite_rc rc = sqlite3_step(stmt);
	if( rc == SQLITE_ROW ) {
		const t_index ret = (t_index)sqlite3_column_int64(stmt, 0);
		transaction_rollback(storage, begin);
		return ret;
	}
	transaction_rollback(storage, begin);
	return NULL_ID;
}


#pragma mark Add


/**
 * librdf_storage_sqlite_context_add_statement:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 * @statement: #librdf_statement statement to add
 *
 * Add a statement to a storage db_ctx.
 *
 * Return value: non 0 on failure
 **/
static int pub_context_add_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement)
{
	// librdf_log( librdf_storage_get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL, "%s", librdf_statement_to_string(statement) );
	return NULL == find_statement(storage, context_node, statement, BOOL_YES) ? RET_ERROR : RET_OK;
}


static int pub_add_statement(librdf_storage *storage, librdf_statement *statement)
{
	return pub_context_add_statement(storage, NULL, statement);
}


static int pub_add_statements(librdf_storage *storage, librdf_stream *statement_stream)
{
	// begin txn
	// iterate
	// rollback/commit
	assert(0 && "not implemented yet.");
	return RET_ERROR;
}


#pragma mark Remove


static int pub_remove_statement(librdf_storage *storage, librdf_statement *statement)
{
	assert(0 && "not implemented yet.");
	return RET_ERROR;
}


#pragma mark context functions


static int pub_context_remove_statement(librdf_storage *storage, librdf_node *context_node, librdf_statement *statement)
{
	assert(0 && "not implemented yet.");
	return RET_ERROR;
}


static int pub_context_remove_statements(librdf_storage *storage, librdf_node *context_node)
{
	assert(0 && "not implemented yet.");
	return RET_ERROR;
}


#pragma mark register storage factory


static void librdf_storage_sqlite_register_factory(librdf_storage_factory *factory)
{
	assert( !strcmp(factory->name, LIBRDF_STORAGE_SQLITE_MRO) );

	factory->version            = LIBRDF_STORAGE_INTERFACE_VERSION;
	factory->init               = pub_init;
	factory->terminate          = pub_terminate;
	factory->open               = pub_open;
	factory->close              = pub_close;
	factory->size               = pub_size;
	factory->add_statement      = pub_add_statement;
	factory->add_statements     = pub_add_statements;
	factory->remove_statement   = pub_remove_statement;
	factory->contains_statement = pub_contains_statement;
	factory->serialise          = pub_serialise;
	factory->find_statements    = pub_find_statements;
	factory->context_add_statement    = pub_context_add_statement;
	factory->context_remove_statement = pub_context_remove_statement;
	factory->context_remove_statements = pub_context_remove_statements;
	factory->context_serialise        = pub_context_serialise;
	factory->find_statements_in_context = pub_context_find_statements;
	factory->get_contexts             = pub_get_contexts;
	factory->get_feature              = pub_get_feature;
	factory->transaction_start        = pub_transaction_start;
	factory->transaction_commit       = pub_transaction_commit;
	factory->transaction_rollback     = pub_transaction_rollback;
}


void librdf_init_storage_sqlite_mro(librdf_world *world)
{
	librdf_storage_register_factory(world, LIBRDF_STORAGE_SQLITE_MRO, "SQLite", &librdf_storage_sqlite_register_factory);
}
