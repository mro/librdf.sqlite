
Improved [SQLite](http://sqlite.org) triple [storage module](http://librdf.org/docs/api/redland-storage-module-sqlite.html)
for [librdf](http://librdf.org/).

Inspired by the [official sqlite store](http://librdf.org/docs/api/redland-storage-module-sqlite.html).

Considered alpha, *don't use in production*.

## Usage

    ....
    const char* options = "new='yes', contexts='no'";
    librdf_storage *newStorage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, file_path, options);
