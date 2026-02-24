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

#if !defined(__cplusplus) && (!defined(__STDC_VERSION__) || __STDC_VERSION__ < 202311L)
/* Define bool, true, false for C before C23. C++ and C23+ have them built-in. */
#ifndef bool
typedef char bool;
#endif

#ifndef true
#define true	((bool) 1)
#endif

#ifndef false
#define false	((bool) 0)
#endif
#endif   /* not C++ */

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
