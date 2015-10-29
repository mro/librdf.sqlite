// see https://github.com/mro/librdf.sqlite/issues/10#issuecomment-151000843
//
// Perpare (on debian wheezy):
// $ git clone https://github.com/mro/librdf.sqlite.git
// $ cd librdf.sqlite/demo
// $ sudo apt-get install gcc librdf0-dev librdf-storage-sqlite raptor2-utils
// $ rapper --output turtle --input rdfxml http://tatort.rdf.mro.name/episodes.rdf > loader.ttl
// rapper: Parsing URI http://tatort.rdf.mro.name/episodes.rdf with parser rdfxml
// rapper: Serializing with serializer turtle
// rapper: Parsing returned 100430 triples
//
// Compile:
// $ gcc -std=c99 -D DEBUG=1 -I /usr/include/raptor2 -I /usr/include/rasqal -l rdf -lsqlite3 loader.c
//
// Run:
// $ echo "librdf.sqlite" ; time ./a.out "file://$(pwd)/loader.ttl" "http://purl.mro.name/rdf/sqlite/"
// real 0m9.726s
// user 0m4.564s
// sys  0m4.744s
// $ echo "stock" ; time ./a.out "file://$(pwd)/loader.ttl" "sqlite"
// real 15m2.400s
// user 14m19.086s
// sys  0m42.351s
//

// based on https://gist.github.com/abargnesi/8402baf1a019a2179e70
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <librdf.h>
#include <raptor2.h>

#include "../rdf_storage_sqlite_mro.h"
#include "../rdf_storage_sqlite_mro.c" // unorthodox but ok for now

/* function prototypes */
int main(int argc, char *argv[]);

int main(int argc, char *argv[])
{
    librdf_uri *uri;
    librdf_world *world;
    librdf_storage *storage;
    librdf_parser *parser;
    librdf_stream *stream;
    librdf_model *model;
    char *extfind;
    char *parser_name  = NULL;
    char *storage_name = NULL;

    if( argc != 3 ) {
        fprintf(stderr, "usage: %s <rdf content URI> <name>\n", argv[0]);
        return(1);
    }

    // open world
    world = librdf_new_world();
    librdf_world_open(world);
    librdf_init_storage_sqlite_mro(world); // register storage factory

    // assign uri for content
    uri = librdf_new_uri(world, (const unsigned char *)argv[1]);

    // assign storage name
    storage_name = argv[2];
    if( !storage_name ) {
        fprintf(stderr, "storage name was not set\n");
        return 1;
    }

    extfind = strstr(argv[1], ".ttl");
    if( extfind != NULL ) {
        parser_name = "turtle";
    } else {
        extfind = strstr(argv[1], ".nt");
        if( extfind != NULL ) {
            parser_name = "ntriples";
        }
    }
    if( !parser_name ) {
        fprintf(stderr, "uri was not turtle or ntriples\n");
        return 1;
    }

    fprintf(stdout, "%s\n", extfind);

    // create storage
    storage = librdf_new_storage(world, storage_name, "loader.sqlite", "new='yes',synchronous='off'");
    if( !storage ) {
        fprintf(stderr, "could not create sqlite storage");
        return(1);
    }
    {
        librdf_uri *uri_xsd_unsignedShort = librdf_new_uri(world, "http://www.w3.org/2000/10/XMLSchema#" "unsignedShort");
        librdf_uri *f_uri = librdf_new_uri(world, LIBRDF_STORAGE_SQLITE_MRO_ "feature/sql/cache/mask");
        librdf_storage_set_feature( storage, f_uri, librdf_new_node_from_typed_literal(world, (const unsigned char *)"2047", NULL, uri_xsd_unsignedShort) );
        librdf_free_uri(uri_xsd_unsignedShort);
        librdf_free_uri(f_uri);
    }
    {
        librdf_uri *uri_xsd_boolean = librdf_new_uri(world, (const unsigned char *)"http://www.w3.org/2000/10/XMLSchema#" "boolean");
        librdf_uri *f_uri = librdf_new_uri(world, LIBRDF_STORAGE_SQLITE_MRO_ "feature/profile");
        librdf_storage_set_feature( storage, f_uri, librdf_new_node_from_typed_literal(world, (const unsigned char *)"0", NULL, uri_xsd_boolean) );
        librdf_free_uri(uri_xsd_boolean);
        librdf_free_uri(f_uri);
    }

    // create model
    model = librdf_new_model(world, storage, NULL);

    // create parser
    parser = librdf_new_parser(world, parser_name, NULL, NULL);
    stream = librdf_parser_parse_as_stream(parser, uri, NULL);

    // librdf_model_transaction_start(model);
    librdf_model_add_statements(model, stream);
    // librdf_parser_parse_into_model(parser, uri, NULL, model);
    // librdf_model_transaction_commit(model);

    // free up
    librdf_free_parser(parser);
    librdf_free_model(model);
    librdf_free_uri(uri);
    librdf_free_world(world);

    return 0;
}
