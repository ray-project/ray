/*-------------------------------------------------------------------------
 *
 * spt_python.c
 *    A simple function for the module debugging.
 *
 * Copyright (c) 2009-2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *
 * Debug logging is enabled if the environment variable SPT_DEBUG is set to a
 * non-empty value at runtime.
 *
 *-------------------------------------------------------------------------
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "spt_config.h"

HIDDEN void
spt_debug(const char *fmt, ...)
{
    static int enabled = -1;
    va_list ap;

    /* check if debug is enabled */
    if (-1 == enabled) {
        char *d = getenv("SPT_DEBUG");
        enabled = (d && *d) ? 1 : 0;
    }

    /* bail out if debug is not enabled */
    if (0 == enabled) { return; }

    fprintf(stderr, "[SPT]: ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
}
