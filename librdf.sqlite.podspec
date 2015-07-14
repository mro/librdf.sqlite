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

Inspired by the [official sqlite store](https://github.com/dajobe/librdf/blob/master/src/rdf_storage_sqlite.c).

Considered alpha, *don't use in production*.

## Usage

    ....
    const char* options = "new='yes', contexts='no'";
    librdf_storage *newStorage = librdf_new_storage(world, LIBRDF_STORAGE_SQLITE_MRO, file_path, options);
DESC
  s.homepage        = "http://purl.mro.name/rdf/sqlite"
  s.license         = 'Human Rights'
  s.author          = { "Marcus Rohrmoser" => "github@mro.name" }
  s.source          = { :git => "https://github.com/mro/librdf.sqlite.git", :tag => s.version.to_s }

  s.requires_arc 		= false
  s.platform				= :ios, '5.0'

  s.source_files 		= 'rdf_storage_sqlite_mro.{c,h}'
  s.public_header_files = 'rdf_storage_sqlite_mro.h'
  s.dependency 'librdf.ios', '~> 1.0'
end
