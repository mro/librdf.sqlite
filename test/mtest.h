//
// mtest.h
//
// Copyright (c) 2015-2015, Marcus Rohrmoser mobile Software, http://mro.name/me
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted
// provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions
// and the following disclaimer.
//
// 2. The software must not be used for military or intelligence or related purposes nor
// anything that's in conflict with human rights as declared in http://www.un.org/en/documents/udhr/ .
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
// IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

#include "minunit.h"
// parameter order like XCTAssert, STAssert or NSAssert
//
// STAssert from http://www.sente.ch/software/ocunit/
// http://www.decompile.com/cpp/faq/file_and_line_error_string.htm
#define STRINGIFY(x) # x
#define TOSTRING(x) STRINGIFY(x)
#define MUAssert(cond, message) mu_assert(__FILE__ ":" TOSTRING(__LINE__) " " message, cond)

#include "ansi-colors.h"
#include <time.h>

#define MUTestRun(test) do { \
        const char *marker = TOSTRING(test); \
        printf("travis_fold:start:%s\n", marker); \
        const double CLOCKS_PER_NANO_SEC = CLOCKS_PER_SEC * 1e-9; \
        const long long unsigned int t_start = clock() / CLOCKS_PER_NANO_SEC; \
        printf("travis_time:start:%s_time\n", marker); \
        char *message = test(); tests_run++; \
        printf("%s%s %s %s\n", message ? (ANSI_COLOR_F_RED "✗") : (ANSI_COLOR_F_GREEN "✓"), ANSI_COLOR_RESET, marker, message ? message : ""); \
        const long long unsigned int t_finish = clock() / CLOCKS_PER_NANO_SEC; \
        printf("travis_time:end:%s_time:start=%llu,finish=%llu,duration=%llu\n", marker, t_start, t_finish, t_finish - t_start); \
        printf("travis_fold:end:%s\n", marker); \
        if( message ) return message; \
} \
    while( 0 )
