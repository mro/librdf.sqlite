
Improved [SQLite](http://sqlite.org) RDF triple [storage module](http://librdf.org/docs/api/redland-storage-modules.html)
for [librdf](http://librdf.org/).

Inspired by the [official sqlite store](https://github.com/dajobe/librdf/blob/master/src/rdf_storage_sqlite.c).

Considered alpha, *don't use in production*.

## Usage

    ....
    const char* options = "new='yes', contexts='no'";
    librdf_storage *newStorage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, file_path, options);
