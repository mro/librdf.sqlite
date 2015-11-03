#!/bin/bash
cd "$(dirname "$0")"

[ "$CC" != "" ] || { echo "environment \$CC must be set, e.g. $ CC=gcc $0 $@" 1>&2 && exit 1 ; }

TMP=tmp
BUILD=build

# terminal colors (require bash)
# http://www.tldp.org/HOWTO/Bash-Prompt-HOWTO/x329.html
# http://wiki.bash-hackers.org/scripting/terminalcodes
FGC_NONE="\033[0m"
FGC_GRAY="\033[1;30m"
FGC_RED="\033[1;31m"
FGC_GREEN="\033[1;32m"
FGC_YELLOW="\033[1;33m"
FGC_BLUE="\033[1;34m"
FGC_PURPLE="\033[1;35m"
FGC_CYAN="\033[1;36m"
FGC_WHITE="\033[1;37m"
BGC_GRAY="\033[7;30m"
BGC_RED="\033[7;31m"
BGC_GREEN="\033[7;32m"
BGC_YELLOW="\033[7;33m"
BGC_BLUE="\033[7;34m"
BGC_PURPLE="\033[7;35m"
BGC_CYAN="\033[7;36m"
BGC_WHITE="\033[7;37m"

status_code=0

run_test() {
  test_src="$1"
  test_name="$test_src"

  echo -n "travis_fold:start:${test_name}\r"
  echo -n "Running $test_name "

  rm "$TMP"/* "$BUILD"/*
  $CC -pedantic -g3 -O0 -std=c99 -D DEBUG=1 -I "/usr/include/raptor2" -I "/usr/include/rasqal" -c -o "$BUILD/rdf_storage_sqlite_mro.o" "../rdf_storage_sqlite_mro.c" && {
    $CC -pedantic -g3 -O0 -std=c99 -D DEBUG=1 -I "/usr/include/raptor2" -I "/usr/include/rasqal" -c -o "$BUILD/$test_name".o "$test_src" && {
      # http://ubuntuforums.org/showthread.php?t=1936253&p=11742200#post11742200
      $CC -g3 -O0 -o "$BUILD/a.out" "$BUILD/rdf_storage_sqlite_mro.o" "$BUILD/$test_name".o -lrdf -lraptor2 -lsqlite3 && {
        # valgrind --leak-check=full --show-reachable=yes \
        "$BUILD/a.out"
      }
    }
  }
  code=$?

  echo -n "travis_fold:end:${test_name}\r"
  if [ "$code" -eq 0 ] ; then
    echo -e "${FGC_GREEN}✓${FGC_NONE} ${test_name}"
  else
    echo -e "${FGC_RED}✗${FGC_NONE} ${test_name} (code: $code)"
    status_code=1
  fi
}

set -x

run_test test-size.c
run_test test-example1.c

exit $status_code
