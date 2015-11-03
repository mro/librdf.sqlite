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
#include <unistd.h>
#include <string.h>

int tests_run = 0;

static char *test_assert()
{
    MUAssert(1, "ok");
    MUAssert(true, "ok");
    MUAssert(true, "ok");
    MUAssert(true, "ok");
    MUAssert(true, "ok");
    MUAssert(true, "ok");
    MUAssert(true, "ok");
    MUAssert(true, "ok");
    MUAssert(1 != 0, "ok");
    return NULL;
}


static char *test_size0()
{
    librdf_world *world = librdf_new_world();
    MUAssert(world, "Failed to create world");
    librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    {
        librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-size0.sqlite",
                                                     "new='on', contexts='no', synchronous='off'");
        MUAssert(storage, "Failed to create storage");
        {
            librdf_model *model = librdf_new_model(world, storage, NULL);
            MUAssert(model, "Failed to create model");
            {
                librdf_storage_set_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, true);
                MUAssert(0 == librdf_model_size(model), "model size has to be 0.");
                {
                    librdf_statement *stmt =
                        librdf_new_statement_from_nodes(
                            world,
                            librdf_new_node_from_uri_string(world, (const unsigned char *)"http://www.dajobe.org/"),
                            librdf_new_node_from_uri_string(world, (const unsigned char *)"http://purl.org/dc/elements/1.1/title"),
                            librdf_new_node_from_literal(world, (const unsigned char *)"My Home Page", NULL, 0)
                            );
                    librdf_model_add_statement(model, stmt);
                    // Free what we just used to add to the model - now it should be stored
                    librdf_free_statement(stmt);
                }
                MUAssert(1 == librdf_model_size(model), "model size has to be 0.");
            }
            librdf_free_model(model);
        }
        librdf_free_storage(storage);
    }
    librdf_free_world(world);
    return NULL;
}


static char *all_tests()
{
    MUTestRun(test_assert);
    MUTestRun(test_size0);
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
