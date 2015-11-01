#!/bin/sh
#
# Copyright (c) 2014-2015, Marcus Rohrmoser mobile Software, http://mro.name/me
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted
# provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions
#    and the following disclaimer.
#
# 2. The software must not be used for military or intelligence or related purposes nor
#    anything that's in conflict with human rights as declared in http://www.un.org/en/documents/udhr/ .
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
if [ ! -e "$UNCRUSTIFY" ] ; then
	UNCRUSTIFY=$(which uncrustify | head -n 1)
fi
if [ ! -e "$UNCRUSTIFY" ] ; then
	# use most recent
	UNCRUSTIFY=$(ls -t /Applications/UniversalIndentGUI*/indenters/uncrustify | head -n 1)
fi
if [ ! -e "$UNCRUSTIFY" ] ; then
	echo "I cannot find uncrustify. Please install e.g. from" >&2
	echo "\n    http://sourceforge.net/projects/uncrustify/files/uncrustify/uncrustify-0.59/uncrustify-0.59-osx-64bit.zip/download" >&2
	echo "\n    http://universalindent.sourceforge.net/" >&2
	echo "\ninto 'Applications', or set" >&2
	echo "\n    export UNCRUSTIFY=..." >&2
	echo "\nto point to the location you installed it to." >&2
	exit 1
fi
echo "Found $($UNCRUSTIFY --version) at $UNCRUSTIFY" >&2

cd `dirname $0`/..

if [[ "$@" == "" ]]
then
	echo "Got no files on commandline (which is fine), so I'll format those:"
	# PROJECT_SOURCE=$(find Classes* -name "*.m" -or -name "*.h")
	PROJECT_SOURCE=$(ls *_mro.? */*.c)
else
	PROJECT_SOURCE="$@"
fi

if [[ "$UNCRUSTIFY_OPTS" == "" ]] ; then
	UNCRUSTIFY_OPTS="-l C --replace --no-backup -c tools/uncrustify.cfg"
fi

"$UNCRUSTIFY" $UNCRUSTIFY_OPTS $PROJECT_SOURCE 2>&1

for file2indent in $PROJECT_SOURCE ; do
	# http://code.google.com/p/core-plot/source/browse/scripts/format_core_plot.sh?spec=svn3daea3e540f8571d6e99b2cbfb832a88f0777d79&r=3daea3e540f8571d6e99b2cbfb832a88f0777d79
	# remove spaces before category names to keep Doxygen 1.6.0+ happy
	cp -p "$file2indent" .indent.tmp
	cat .indent.tmp | sed "s|\(@interface .*\) \((.*)\)|\1\2|g" | sed "s|\(@implementation .*\) \((.*)\)|\1\2|g" > "$file2indent"
	touch -r .indent.tmp "$file2indent"
	rm .indent.tmp
done