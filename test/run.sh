#!/bin/sh
cd "$(dirname "$0")"

TMP=tmp

run_test() {
  test_src="$1"
  test_base="$test_src"
  rm "$TMP"/*
  gcc -pedantic -g -O0 -std=c99 -D DEBUG=1 -I "/usr/include/raptor2" -I "/usr/include/rasqal" -c -o "$TMP/rdf_storage_sqlite_mro.o" "../rdf_storage_sqlite_mro.c" && {
    gcc -pedantic -g -O0 -std=c99 -D DEBUG=1 -I "/usr/include/raptor2" -I "/usr/include/rasqal" -c -o "$TMP/$test_base".o "$test_src" && {
      # http://ubuntuforums.org/showthread.php?t=1936253&p=11742200#post11742200
      gcc -o "$TMP/a.out" "$TMP/rdf_storage_sqlite_mro.o" "$TMP/$test_base".o -lrdf -lsqlite3 && {
        # valgrind --leak-check=full --show-reachable=yes \
        "$TMP/a.out" "file://$(pwd)/$test_base.ttl" "http://purl.mro.name/rdf/sqlite/" 2> /dev/null
      }
    }
  }
}

set -x

run_test test-loader.c
