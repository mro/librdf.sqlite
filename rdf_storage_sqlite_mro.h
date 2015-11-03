//
// rdf_storage_sqlite_mro.h
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

#ifndef Redland_rdf_storage_sqlite_mro_h
#define Redland_rdf_storage_sqlite_mro_h

#include <librdf.h>

/** Factory name.
 *
 */
extern const char *LIBRDF_STORAGE_SQLITE_MRO;

/** Which triple-find-queries should be cached. Bitmask, http://www.w3.org/2000/10/XMLSchema#unsignedShort. Clipped to 0x1FF.
 */
extern const unsigned char *LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK;

/** Register sqlite3_profile or not. http://www.w3.org/2000/10/XMLSchema#boolean.
 */
extern const unsigned char *LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE;

/** Print (some) sqlite3 'EXPLAIN QUERY PLAN' or not. http://www.w3.org/2000/10/XMLSchema#boolean.
 */
extern const unsigned char *LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_EXPLAIN_QUERY_PLAN;

#ifndef LIBRDF_STORAGE_SQLITE_MRO_CONVENIENCE
#define LIBRDF_STORAGE_SQLITE_MRO_CONVENIENCE 0
#endif

#if LIBRDF_STORAGE_SQLITE_MRO_CONVENIENCE
#include <stdbool.h>
#include <errno.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

void librdf_init_storage_sqlite_mro(librdf_world *world);

#if LIBRDF_STORAGE_SQLITE_MRO_CONVENIENCE
void librdf_storage_set_feature_mro_bool(librdf_storage *storage, const unsigned char *feature, const bool value);
void librdf_storage_set_feature_mro_int(librdf_storage *storage, const unsigned char *feature, const int value);

int librdf_storage_get_feature_mro_bool(librdf_storage *storage, const unsigned char *feature, bool *value);
int librdf_storage_get_feature_mro_int(librdf_storage *storage, const unsigned char *feature, int *value);

#endif

#ifdef __cplusplus
} // extern "C"
#endif

#endif
