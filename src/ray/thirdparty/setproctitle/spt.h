/*-------------------------------------------------------------------------
 *
 * spt.h
 *    Definitions useful throughout all the extension.
 *
 * Copyright (c) 2010-2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *
 *-------------------------------------------------------------------------
 */

#ifndef SPT_H
#define SPT_H

#include "spt_config.h"
#include "spt_python.h"

/* expose the debug function to the extension code */
HIDDEN void spt_debug(const char *fmt, ...);

#endif
