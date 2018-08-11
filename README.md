
[![Build Status](https://travis-ci.org/mro/librdf.sqlite.svg)](https://travis-ci.org/mro/librdf.sqlite)

Improved [SQLite](http://sqlite.org) RDF triple [storage module](http://librdf.org/docs/api/redland-storage-modules.html)
for [librdf](http://librdf.org/).

Cross platform, plain C source file. Comes with a [![Version](https://img.shields.io/cocoapods/v/librdf.sqlite.svg)](https://github.com/CocoaPods/Specs/tree/master/Specs/librdf.sqlite/) for those targeting iOS.

Inspired by the [official sqlite store](https://github.com/dajobe/librdf/blob/master/src/rdf_storage_sqlite.c).

## Usage

```c
#include "rdf_storage_sqlite_mro.h"
....
librdf_world *world = librdf_new_world();
librdf_init_storage_sqlite_mro(world);  // register storage factory
....
const char* options = "new='yes', contexts='no'";
librdf_storage *newStorage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, file_path, options);
```

See e.g. in (my) <http://purl.mro.name/ios/librdf.objc>.

## License

- `test/minunit.h`, Copyright (C) 2002 [John Brewer](http://jera.com), NO WARRANTY,
- *all others*, Copyright (C) 2014-2015 [Marcus Rohrmoser mobile Software](http://mro.name/~me), [Human Rights License](LICENSE)

## Design Goals

| Quality         | very good | good | normal | irrelevant |
|-----------------|:---------:|:----:|:------:|:----------:|
| Functionality   |           |      |    ×   |            |
| Reliability     |           |  ×   |        |            |
| Usability       |           |      |        |     ×      |
| Efficiency      |     ×     |      |        |            |
| Changeability   |           |  ×   |        |            |
| Portability     |           |      |    ×   |            |

Currently 50% code and 99% runtime saving (for 100k triples).

- intense use of [SQLite prepared statements](https://www.sqlite.org/c3ref/stmt.html) and
  [bound values](https://www.sqlite.org/c3ref/bind_blob.html):
  - no stringbuffers
  - no strcpy/memcpy,
  - no SQL escaping,
- re-use compiled statements where possible (at the cost of thread safety),
- as few SQL statements as possible (at the cost of some non-trivial ones),
- SQLite indexes (at the cost of larger DB files).
