//
// test-size.c
//
// Copyright (c) 2015-2015, Marcus Rohrmoser mobile Software, http://mro.name/me
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

#define LIBRDF_STORAGE_SQLITE_MRO_CONVENIENCE 1
#include "../rdf_storage_sqlite_mro.h"

#include "mtest.h"
#include "ansi-colors.h"

#define MUTestRun(test) do { \
    const char *marker = TOSTRING(test); \
    printf("travis_fold:start:%s\r", marker); \
    const long long unsigned int t_start = 5e8 + clock() * 1.0e9 / CLOCKS_PER_SEC; \
    printf("travis_time:start:%s_time\r", marker); \
    char *message = test(); tests_run++; \
    const long long unsigned int t_finish = 7.5e8 + clock() * 1.0e9 / CLOCKS_PER_SEC; \
    printf("travis_time:end:%s_time:start=%llu,finish=%llu,duration=%llu\r", marker, t_start, t_finish, t_finish-t_start); \
    printf("travis_fold:end:%s\r", marker); \
    printf("%s%s %s %s\n", message ? (ANSI_COLOR_F_RED "✗") : (ANSI_COLOR_F_GREEN "✓"), ANSI_COLOR_RESET, marker, message ? message : ""); \
    if( message ) return message; \
} while( 0 )

#include "ansi-colors.h"
#include <unistd.h>
#include <string.h>
// #include <errno.h>
#include <time.h>

int tests_run = 0;

static char *test_bool_defaults()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    // librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-feature.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            bool value = false;
            MUAssert(0 == librdf_storage_get_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, &value), "not set");
            MUAssert(false == value, "wrong value");
            MUAssert(0 == librdf_storage_get_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_EXPLAIN_QUERY_PLAN, &value), "not set");
            MUAssert(false == value, "wrong value");
            MUAssert(4 == librdf_storage_get_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK, &value), "not set");
            MUAssert(false == value, "wrong value");
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}

static char *test_bool_rw_ok()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    // librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-feature.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            MUAssert(0 == librdf_storage_set_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, true), "hu");
            bool value = false;
            MUAssert(0 == librdf_storage_get_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, &value), "hu");
            MUAssert(true == value, "wrong value");
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}

static char *test_bool_rw_fail()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    // librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-feature.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            MUAssert(3 == librdf_storage_set_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK, true), "hu");
            bool value = false;
            MUAssert(4 == librdf_storage_get_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK, &value), "not set");
            MUAssert(false == value, "wrong value");
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}

static char *test_int_defaults()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    // librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-feature.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            int value = -1;
            MUAssert(4 == librdf_storage_get_feature_mro_int(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, &value), "not set");
            MUAssert(false == value, "wrong value");
            MUAssert(0 == librdf_storage_get_feature_mro_int(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK, &value), "not set");
            MUAssert(511 == value, "wrong value");
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}

static char *test_int_rw_ok()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    // librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-feature.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            MUAssert(0 == librdf_storage_set_feature_mro_int(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK, 0xFFFF), "hu");
            int value = -1;
            MUAssert(0 == librdf_storage_get_feature_mro_int(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQL_CACHE_MASK, &value), "not set");
            MUAssert(511 == value, "wrong value");
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}

static char *test_int_rw_fail()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    // librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-feature.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            MUAssert(0 == librdf_storage_set_feature_mro_int(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, true), "hu");
            int value = -1;
            MUAssert(4 == librdf_storage_get_feature_mro_int(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, &value), "not set");
            MUAssert(false == value, "wrong value");
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}


static char *all_tests()
{
    MUTestRun(test_bool_defaults);
    MUTestRun(test_bool_rw_ok);
    MUTestRun(test_bool_rw_fail);
    MUTestRun(test_int_defaults);
    MUTestRun(test_int_rw_ok);
    MUTestRun(test_int_rw_fail);
    return 0;
}


int main(int argc, char **argv)
{
    char *result = all_tests();
    if( result != 0 ) {
        printf("%s\n", result);
    } else {
        printf(ANSI_COLOR_F_GREEN "✓" ANSI_COLOR_RESET " ALL TESTS PASSED\n");
    }
    printf("Tests run: %d\n", tests_run);

    return result != 0;
}
