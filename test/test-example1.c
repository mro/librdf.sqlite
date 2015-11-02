#include "../rdf_storage_sqlite_mro.h"

#include "minunit.h"
#include <unistd.h>
#include <string.h>
// #include <errno.h>

int tests_run = 0;

static char *test_main();

static char *all_tests()
{
    mu_run_test(test_main);
    return 0;
}


int main(int argc, char **argv)
{
    char *result = all_tests();
    if( result != 0 ) {
        printf("%s\n", result);
    } else {
        printf("ALL TESTS PASSED\n");
    }
    printf("Tests run: %d\n", tests_run);

    return result != 0;
}


static char *_main(int argc, char *argv[])
{
    char *program = argv[0];
    if( argc < 2 || argc > 3 ) {
        fprintf(stderr, "USAGE: %s: <RDF source URI> [rdf parser name]\n", program);
        mu_assert("wrong parameters", 0);
    }

    librdf_world *world = librdf_new_world();
    librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world);
    raptor_world *raptor_world_ptr = librdf_world_get_raptor(world);

    librdf_uri *uri = librdf_new_uri(world, (const unsigned char *)argv[1]);
    if( !uri ) {
        fprintf(stderr, "%s: Failed to create URI\n", program);
        mu_assert("Failed to create URI", 0);
    }

    librdf_storage *storage =
        librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, "tmp/test-example1.db",
                           "new='on', synchronous='off', contexts='no'");
    if( !storage ) {
        fprintf(stderr, "%s: Failed to create new storage\n", program);
        mu_assert("Failed to create storage", 0);
    }
    {
        librdf_uri *uri_xsd_boolean = librdf_new_uri(world, (const unsigned char *)"http://www.w3.org/2000/10/XMLSchema#" "boolean");
        librdf_uri *f_uri = librdf_new_uri(world, LIBRDF_STORAGE_SQLITE_MRO_ "feature/sqlite3/explain_query_plan");
        librdf_storage_set_feature( storage, f_uri, librdf_new_node_from_typed_literal(world, (const unsigned char *)"1", NULL, uri_xsd_boolean) );
        librdf_free_uri(uri_xsd_boolean);
        librdf_free_uri(f_uri);
    }

    librdf_model *model = librdf_new_model(world, storage, NULL);
    if( !model ) {
        fprintf(stderr, "%s: Failed to create model\n", program);
        mu_assert("Failed to create model", 0);
    }

    char *parser_name = 3 == argc ? argv[2] : NULL;
    librdf_parser *parser = librdf_new_parser(world, parser_name, NULL, NULL);
    if( !parser ) {
        fprintf(stderr, "%s: Failed to create new parser\n", program);
        mu_assert("Failed to create parser", 0);
    }

    /* PARSE the URI as RDF/XML */
    fprintf( stdout, "%s: Parsing URI %s\n", program, librdf_uri_as_string(uri) );
    if( librdf_parser_parse_into_model(parser, uri, uri, model) ) {
        fprintf(stderr, "%s: Failed to parse RDF into model\n", program);
        mu_assert("Failed to parse RDF", 0);
    }
    librdf_free_parser(parser);

    {
        // now as we're done inserting: start profiling..
        librdf_uri *uri_xsd_boolean = librdf_new_uri(world, (const unsigned char *)"http://www.w3.org/2000/10/XMLSchema#" "boolean");
        librdf_uri *f_uri = librdf_new_uri(world, LIBRDF_STORAGE_SQLITE_MRO_ "feature/sqlite3/profile");
        librdf_storage_set_feature( storage, f_uri, librdf_new_node_from_typed_literal(world, (const unsigned char *)"1", NULL, uri_xsd_boolean) );
        librdf_free_uri(uri_xsd_boolean);
        librdf_free_uri(f_uri);
    }

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

    /* Print out the model */
    // fprintf(stdout, "%s: Resulting model is:\n", program);
    // raptor_iostream *iostr = raptor_new_iostream_to_file_handle(raptor_world_ptr, stdout);
    // librdf_model_write(model, iostr);
    // raptor_free_iostream(iostr);

    /* Construct the query predicate (arc) and object (target)
     * and partial statement bits
     */
    librdf_node *subject = librdf_new_node_from_uri_string(world, (const unsigned char *)
                                                           "http://www.dajobe.org/");
    librdf_node *predicate = librdf_new_node_from_uri_string(world, (const unsigned char *)
                                                             "http://purl.org/dc/elements/1.1/title");
    if( !subject || !predicate ) {
        fprintf(stderr, "%s: Failed to create nodes for searching\n", program);
        mu_assert("Failed to create nodes for searching", 0);
    }
    librdf_statement *partial_statement = librdf_new_statement(world);
    librdf_statement_set_subject(partial_statement, subject);
    librdf_statement_set_predicate(partial_statement, predicate);

    /* QUERY TEST 1 - use find_statements to match */

    fprintf(stdout, "%s: Trying to find_statements\n", program);
    librdf_stream *stream = librdf_model_find_statements(model, partial_statement);
    if( !stream ) {
        fprintf(stderr, "%s: librdf_model_find_statements returned NULL stream\n", program);
    } else {
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
        fprintf(stderr, "%s: got %d matching statements\n", program, count);
    }

    /* QUERY TEST 2 - use get_targets to do match */
    fprintf(stdout, "%s: Trying to get targets\n", program);
    librdf_iterator *iterator = librdf_model_get_targets(model, subject, predicate);
    if( !iterator ) {
        fprintf(stderr, "%s: librdf_model_get_targets failed to return iterator for searching\n", program);
        mu_assert("librdf_model_get_targets failed to return iterator for searching", 0);
    }

    int count = 0;
    while( !librdf_iterator_end(iterator) ) {
        librdf_node *target;

        target = (librdf_node *)librdf_iterator_get_object(iterator);
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
    fprintf(stderr, "%s: got %d target nodes\n", program, count);

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
    char src_uri[1024] = "file://localhost";
    mu_assert( "Failed to get current working dir.",
               getcwd( src_uri + strlen(src_uri), sizeof(src_uri) - strlen(src_uri) ) );
    strncat( src_uri, "/test-example1.ttl", sizeof(src_uri) - strlen(src_uri) );
    char *argv[] = {
        "test-example1", src_uri, "turtle", NULL
    };
    return _main(3, argv);
}
