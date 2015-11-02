// https://github.com/jeradesign/MinUnit/blob/master/minunit.h
/* file: minunit.h */
/* Copyright (C) 2002 John Brewer */
/* You may use this code for any purpose, with the understanding that it comes with NO WARRANTY. */

#define mu_assert(message, test) do { if (!(test)) return message; } while (0)
#define mu_run_test(test) do { char *message = test(); tests_run++; \
                            if (message) return message; } while (0)
extern int tests_run;
