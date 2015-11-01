#!/bin/sh
cd "$(dirname "$0")"
set -x

TMP=tmp
rm "$TMP"/*

gcc -pedantic -g -O0 -std=c99 -D DEBUG=1 -I /usr/include/raptor2 -I /usr/include/rasqal -c -o "$TMP"/loader.o loader.c && {
	gcc -o "$TMP"/loader -lrdf -lsqlite3 "$TMP"/loader.o && {
		# valgrind --leak-check=full --show-reachable=yes \
		"$TMP"/loader "file://$(pwd)/loader.ttl" "http://purl.mro.name/rdf/sqlite/" 2> /dev/null
	}
}
