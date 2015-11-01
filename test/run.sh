#!/bin/sh
cd "$(dirname "$0")"
set -x

TMP=tmp
rm "$TMP"/*

gcc -pedantic -g -O0 -std=c99 -D DEBUG=1 -I /usr/include/raptor2 -I /usr/include/rasqal -c -o "$TMP"/loader.o loader.c && {
  echo "##########################################################################"
  # http://ubuntuforums.org/showthread.php?t=1936253&p=11742200#post11742200
  gcc -o "$TMP"/loader "$TMP"/loader.o -lrdf -lsqlite3 && {
    # valgrind --leak-check=full --show-reachable=yes \
    "$TMP"/loader "file://$(pwd)/loader.ttl" "http://purl.mro.name/rdf/sqlite/" 2> /dev/null
  }
}
