//
// test-example1.c
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
#include <unistd.h>
#include <string.h>
#include <time.h>

int tests_run = 0;

static char *test_main();

static char *all_tests()
{
    mu_run_test(test_main);
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


static char *_main(int argc, char *argv[])
{
    char *program = argv[0];
    MUAssert(2 <= argc && argc <= 3, "USAGE: %s: <RDF source URI> [rdf parser name]\n");

    librdf_world *world = librdf_new_world();
    librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    raptor_world *raptor_world_ptr = librdf_world_get_raptor(world);

    librdf_uri *uri = librdf_new_uri(world, (const unsigned char *)argv[1]);
    MUAssert(uri, "Failed to create URI");

    librdf_storage *storage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-example1.sqlite",
                                                 "new='on', contexts='no', synchronous='off'");
    MUAssert(storage, "Failed to create storage");
    librdf_storage_set_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_EXPLAIN_QUERY_PLAN, true);

    librdf_model *model = librdf_new_model(world, storage, NULL);
    MUAssert(model, "Failed to create model");

    {
        const clock_t t_start = clock();

        char *parser_name = 3 == argc ? argv[2] : NULL;
        librdf_parser *parser = librdf_new_parser(world, parser_name, NULL, NULL);
        MUAssert(parser, "Failed to create parser");

        fprintf( stdout, "%s: Parsing URI %s\n", program, librdf_uri_as_string(uri) );
        if( librdf_parser_parse_into_model(parser, uri, uri, model) ) {
            fprintf(stderr, "%s: Failed to parse RDF into model\n", program);
            MUAssert(0, "Failed to parse RDF");
        }
        librdf_free_parser(parser);

        const double dt = 1.0 * (clock() - t_start) / CLOCKS_PER_SEC;
        const int triple_count = 1; // librdf_model_size(model);
        fprintf(stderr, "Parsing %d triples took %.6f s\n", triple_count, dt);
    }

    // now as we're done inserting: start profiling..
    librdf_storage_set_feature_mro_bool(storage, LIBRDF_STORAGE_SQLITE_MRO_FEATURE_SQLITE3_PROFILE, true);

    {
        librdf_statement *statement2 =
            librdf_new_statement_from_nodes(world, librdf_new_node_from_uri_string(world, (const unsigned char *)
                                                                                   "http://www.dajobe.org/"),
                                            librdf_new_node_from_uri_string(world, (const unsigned char *)
                                                                            "http://purl.org/dc/elements/1.1/title"),
                                            librdf_new_node_from_literal(world, (const unsigned char *)
                                                                         "My Home Page", NULL, 0)
                                            );
        librdf_model_add_statement(model, statement2);
        /* Free what we just used to add to the model - now it should be stored */
        librdf_free_statement(statement2);
    }
    /* Print out the model */
    if( false ) {
        fprintf(stdout, "%s: Resulting model is:\n", program);
        raptor_iostream *iostr = raptor_new_iostream_to_file_handle(raptor_world_ptr, stdout);
        librdf_model_write(model, iostr);
        raptor_free_iostream(iostr);
    }
    /* Construct the query predicate (arc) and object (target)
     * and partial statement bits
     */
    librdf_node *subject = librdf_new_node_from_uri_string(world, (const unsigned char *)
                                                           "http://www.dajobe.org/");
    librdf_node *predicate = librdf_new_node_from_uri_string(world, (const unsigned char *)
                                                             "http://purl.org/dc/elements/1.1/title");
    MUAssert(subject && predicate, "Failed to create nodes for searching");

    librdf_statement *partial_statement = librdf_new_statement(world);
    librdf_statement_set_subject(partial_statement, subject);
    librdf_statement_set_predicate(partial_statement, predicate);
    {
        /* QUERY TEST 1 - use find_statements to match */
        const clock_t t_start = clock();
        fprintf(stdout, "%s: Trying to find_statements\n", program);
        librdf_stream *stream = librdf_model_find_statements(model, partial_statement);
        MUAssert(stream, "librdf_model_find_statements returned NULL stream");

        int count = 0;
        while( !librdf_stream_end(stream) ) {
            librdf_statement *statement = librdf_stream_get_object(stream);
            if( !statement ) {
                fprintf(stderr, "%s: librdf_stream_next returned NULL\n", program);
                break;
            }
            fputs("  Matched statement: ", stdout);
            librdf_statement_print(statement, stdout);
            fputc('\n', stdout);

            librdf_stream_next(stream);
            count++;
        }
        librdf_free_stream(stream);
        fprintf(stderr, "%s: dt=%.6f sec got %d matching statements\n", program,
                1.0 * (clock() - t_start) / CLOCKS_PER_SEC, count);
    }
    {
        /* QUERY TEST 2 - use get_targets to do match */
        const clock_t t_start = clock();
        fprintf(stdout, "%s: Trying to get targets\n", program);
        librdf_iterator *iterator = librdf_model_get_targets(model, subject, predicate);
        MUAssert(iterator, "librdf_model_get_targets failed to return iterator for searching");
        int count = 0;
        while( !librdf_iterator_end(iterator) ) {
            librdf_node *target = (librdf_node *)librdf_iterator_get_object(iterator);
            if( !target ) {
                fprintf(stderr, "%s: librdf_iterator_get_object returned NULL\n", program);
                break;
            }
            fputs("  Matched target: ", stdout);
            librdf_node_print(target, stdout);
            fputc('\n', stdout);

            count++;
            librdf_iterator_next(iterator);
        }
        librdf_free_iterator(iterator);
        fprintf(stderr, "%s: dt=%.6f sec got %d target nodes\n", program, 1.0 * (clock() - t_start) / CLOCKS_PER_SEC,
                count);
    }
    librdf_free_statement(partial_statement);
    /* the above does this since they are still attached */
    /* librdf_free_node(predicate); */
    /* librdf_free_node(object); */

    librdf_free_model(model);
    librdf_free_storage(storage);
    librdf_free_uri(uri);
    librdf_free_world(world);

#ifdef LIBRDF_MEMORY_DEBUG
    librdf_memory_report(stderr);
#endif

    return NULL;
}


char *test_main()
{
    char src_uri[1024] = "file://";
    MUAssert(getcwd( src_uri + strlen(src_uri), sizeof(src_uri) - strlen(src_uri) ), "Failed to get current working dir.");
    strncat(src_uri, "/test-example1.ttl", sizeof(src_uri) - strlen(src_uri) - 1);
    char *argv[] = {
        "test-example1", src_uri, "turtle", NULL
    };
    return _main(3, argv);
}
