#
# Be sure to run `pod lib lint librdf.sqlite.podspec' to ensure this is a
# valid spec and remove all comments before submitting the spec.
#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html
#
Pod::Spec.new do |s|
  s.name            = "librdf.sqlite"
  s.version         = File.read('VERSION')
  s.summary         = "improved SQLite RDF triple store for Redland librdf"
  s.description     = <<-DESC
Improved [SQLite](http://sqlite.org) RDF triple [storage module](http://librdf.org/docs/api/redland-storage-modules.html)
for [librdf](http://librdf.org/).

Cross platform, plain C source file.

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
DESC
  s.homepage        = "http://purl.mro.name/librdf.sqlite/"
  s.license         = 'Human Rights'
  s.author          = { "Marcus Rohrmoser" => "http://mro.name/me" }
  s.source          = { :git => "https://github.com/mro/librdf.sqlite.git", :tag => s.version.to_s }

  s.requires_arc    = false
  s.platform        = :ios, '5.0'

  s.source_files    = 'rdf_storage_sqlite_mro.{c,h}'
  s.public_header_files = 'rdf_storage_sqlite_mro.h'
  s.dependency 'librdf.ios', '~> 1.0'
end
