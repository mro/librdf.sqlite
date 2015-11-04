
# set compiler if unset from outside
CC        ?= gcc

CFLAGS    = -Wall -Wno-unknown-pragmas -Werror -g3 -O0 -std=c99 -D DEBUG=1 -I /usr/include/raptor2 -I /usr/include/rasqal
BUILD     = build
TMP       = tmp

TESTS_C   := $(wildcard test-*.c)
TESTS_O   := $(patsubst %.c,$(BUILD)/%.o,$(TESTS_C))
TESTS     := $(patsubst %.o,%,$(TESTS_O))

test:			$(TESTS)

clean:
	-rm $(BUILD)/* $(TMP)/*

# compile librdf.sqlite
$(BUILD)/rdf_storage_sqlite_mro.o:	../rdf_storage_sqlite_mro.c
	$(CC) $(CFLAGS) -c -o $@ $<

# compile a single test
$(BUILD)/test-%.o:	test-%.c
	$(CC) $(CFLAGS) -c -o $@ $<

# link + run a single test
$(BUILD)/test-%:	$(BUILD)/test-%.o $(BUILD)/rdf_storage_sqlite_mro.o
	$(CC) -o $@ $? -lrdf -lsqlite3
	$@
