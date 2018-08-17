//
// test-issue17.c
//
// Copyright (c) 2015-2018, Marcus Rohrmoser mobile Software, http://mro.name/~me
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
#define VANILLA
#ifdef VANILLA
#include <librdf.h>
#else
#include "../rdf_storage_sqlite_mro.h"
#endif

#include "mtest.h"
#include <unistd.h>
#include <string.h>

int tests_run = 0;

static char *test_sparql();

static char *all_tests()
{
    MUTestRun(test_sparql);
    return NULL;
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


//
// https://github.com/nlphacker/Audacity/blob/570c5651c5934469c18dad25db03f05076f91225/lib-src/redland/rasqal/src/rasqal_query_test.c

/*
 * test-sparql1.c - run SPARQL query against a librdf.sqlite model
 *
 * based on https://gist.github.com/rtravis/cd32113c0fc543da120d
 *
 * compile:
 * gcc -I/usr/include/raptor2 -I/usr/include/rasqal -o "test-sparql1" "test-sparql1.c"
 */
static char *test_sparql()
{
    const unsigned char query_string[] = ""
                                "SELECT ?author ?title\n"
                                "WHERE {\n"
                                "       ?book a <http://dbpedia.org/ontology/Book> .\n"
                                "       ?book <http://xmlns.com/foaf/0.1/name> ?title .\n"
                                "       ?book <http://dbpedia.org/ontology/author> ?author.\n" "}\n";
    librdf_world *world = librdf_new_world();
    MUAssert(0 != sizeof(query_string), "uhu");

#ifndef VANILLA
    librdf_init_storage_sqlite_mro(world);
#endif
    {
        const char options[] = "new='yes',contexts='yes',synchronous='off'";
#ifdef VANILLA
        librdf_storage *store = librdf_new_storage(world, "sqlite", "tmp/test-issue17.sqlite", options);
#else
        librdf_storage *store = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-issue17.sqlite", options);
#endif
        MUAssert(store, "Failed to create store");
        {
            librdf_model *model = librdf_new_model(world, store, NULL);
            MUAssert(model, "Failed to create model");
            {
                librdf_uri *uri = NULL;
                {
                    char src_uri[1024] = "file://";
                    MUAssert(getcwd( src_uri + strlen(src_uri), sizeof(src_uri) - strlen(src_uri) ),
                             "Failed to get current working dir.");
                    strncat(src_uri, "/test-issue17.ttl", sizeof(src_uri) - strlen(src_uri) - 1);
                    uri = librdf_new_uri(world, (const unsigned char *)src_uri);
                    MUAssert(uri, "Failed to create URI");
                }
                {
                    librdf_parser *parser = librdf_new_parser(world, "turtle", NULL, NULL);
                    MUAssert(parser, "Failed to create parser");
                    fprintf( stdout, "%s: Parsing URI %s\n", __FILE__, librdf_uri_as_string(uri) );
                    if( librdf_parser_parse_into_model(parser, uri, uri, model) ) {
                        // fprintf(stderr, "%s: Failed to parse RDF into model\n", program);
                        MUAssert(0, "Failed to parse RDF");
                    }
                    librdf_free_parser(parser);
                }
                librdf_free_uri(uri);
            }
            {
                librdf_query *rdf_query =
                    librdf_new_query(world, "sparql", NULL, query_string, NULL);
                MUAssert(rdf_query, "Failed to create query");
                {
                    librdf_query_results *res = librdf_query_execute(rdf_query, model);
                    MUAssert(0 == librdf_query_results_get_count(res), "expected 0 so far");

                    {   // 1st result row
                        MUAssert(0 == librdf_query_results_next(res), "oha 0");
                        MUAssert(1 == librdf_query_results_get_count(res), "expected 1 so far");
                        MUAssert(2 == librdf_query_results_get_bindings_count(res), "why has this to be 2?");
                        MUAssert(0 == strcmp("author", librdf_query_results_get_binding_name(res, 0)), "ouch");
                        {
                            librdf_node *nod = librdf_query_results_get_binding_value(res, 0);
                            MUAssert(0 == strcmp("http://dbpedia.org/resource/Donald_Knuth", (char*)librdf_uri_as_string(librdf_node_get_uri(nod))), "uhu");
                            librdf_free_node(nod);
                        }
                        MUAssert(0 == strcmp("title", librdf_query_results_get_binding_name(res, 1)), "ouch");
                        {
                            librdf_node *nod = librdf_query_results_get_binding_value(res, 1);
                            MUAssert(0 == strcmp("The Art of Computer Programming", (char*)librdf_node_get_literal_value(nod)), "uhu");
                            librdf_free_node(nod);
                        }
                        MUAssert(NULL == librdf_query_results_get_binding_name(res, 2), "ouch");
                        // fprintf(stdout, "name: %s\n", librdf_query_results_get_binding_value(res, 2) );
                    }
                    {   // 2nd result row
                        MUAssert(0 == librdf_query_results_next(res), "oha 0");
                        MUAssert(2 == librdf_query_results_get_count(res), "expected 2 so far");
                        MUAssert(2 == librdf_query_results_get_bindings_count(res), "why has this to be 2?");
                        MUAssert(0 == strcmp("author", librdf_query_results_get_binding_name(res, 0)), "ouch");
                        {
                            librdf_node *nod = librdf_query_results_get_binding_value(res, 0);
                            MUAssert(0 == strcmp("http://dbpedia.org/resource/Plato", (char*)librdf_uri_as_string(librdf_node_get_uri(nod))), "uhu");
                            librdf_free_node(nod);
                        }
                        MUAssert(0 == strcmp("title", librdf_query_results_get_binding_name(res, 1)), "ouch");
                        {
                            librdf_node *nod = librdf_query_results_get_binding_value(res, 1);
                            MUAssert(0 == strcmp("Republic", (char*)librdf_node_get_literal_value(nod)), "uhu");
                            librdf_free_node(nod);
                        }
                        MUAssert(NULL == librdf_query_results_get_binding_name(res, 2), "ouch");
                        // fprintf(stdout, "name: %s\n", librdf_query_results_get_binding_value(res, 0) );
                    }
                    {   // 3rd result row
                        MUAssert(0 != librdf_query_results_next(res), "oha 2");
                    }

                    librdf_free_query_results(res);
                }
                librdf_free_query(rdf_query);
            }
            librdf_free_model(model);
        }
        librdf_free_storage(store);
    }
    librdf_free_world(world);

    return NULL;
}
