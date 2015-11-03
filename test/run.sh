#!/bin/bash
#
# Copyright (c) 2015-2015, Marcus Rohrmoser mobile Software, http://mro.name/me
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted
# provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions
# and the following disclaimer.
#
# 2. The software must not be used for military or intelligence or related purposes nor
# anything that's in conflict with human rights as declared in http://www.un.org/en/documents/udhr/ .
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
# THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
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

run_c_test() {
  test_src="$1"
  test_name="$test_src"

  echo -n "travis_fold:start:${test_name}\r"
  echo -n "Running $test_name "

  rm "$TMP"/* "$BUILD"/*
  $CC -Wall -Wno-unknown-pragmas -Werror -g3 -O0 -std=c99 -D DEBUG=1 -I "/usr/include/raptor2" -I "/usr/include/rasqal" -c -o "$BUILD/rdf_storage_sqlite_mro.o" "../rdf_storage_sqlite_mro.c" && {
    $CC -Wall -Wno-unknown-pragmas -Werror -g3 -O0 -std=c99 -D DEBUG=1 -I "/usr/include/raptor2" -I "/usr/include/rasqal" -c -o "$BUILD/$test_name".o "$test_src" && {
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

for tst in test-*.c
do
  run_c_test "$tst"
done

exit $status_code
