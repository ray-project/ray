/*-------------------------------------------------------------------------
 *
 * c.h
 *    A few fundamental C definitions.
 *
 * Copyright (c) 2009-2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *-------------------------------------------------------------------------
 */

#ifndef C_H
#define C_H

#include "spt_config.h"

#if !defined(__cplusplus) && (!defined(__STDC_VERSION__) || __STDC_VERSION__ <= 201710L)
#include <stdbool.h>
#endif

#include <stddef.h>

/* Let's use our version of strlcpy to avoid portability problems */
size_t spt_strlcpy(char *dst, const char *src, size_t siz);

/* VC defines _WIN32, not WIN32 */
#ifdef _WIN32
#ifndef WIN32
#define WIN32 _WIN32
#endif
#endif

#ifdef WIN32
#include <Windows.h>
#endif

#endif /* C_H */
