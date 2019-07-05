#ifndef HEADER_CURL_MULTIBYTE_H
#define HEADER_CURL_MULTIBYTE_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2015, Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.haxx.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************/
#include "curl_setup.h"

#if defined(USE_WIN32_IDN) || ((defined(USE_WINDOWS_SSPI) || \
                                defined(USE_WIN32_LDAP)) && defined(UNICODE))

 /*
  * MultiByte conversions using Windows kernel32 library.
  */

wchar_t *Curl_convert_UTF8_to_wchar(const char *str_utf8);
char *Curl_convert_wchar_to_UTF8(const wchar_t *str_w);

#endif /* USE_WIN32_IDN || ((USE_WINDOWS_SSPI || USE_WIN32_LDAP) && UNICODE) */


#if defined(USE_WIN32_IDN) || defined(USE_WINDOWS_SSPI) || \
    defined(USE_WIN32_LDAP)

/*
 * Macros Curl_convert_UTF8_to_tchar(), Curl_convert_tchar_to_UTF8()
 * and Curl_unicodefree() main purpose is to minimize the number of
 * preprocessor conditional directives needed by code using these
 * to differentiate UNICODE from non-UNICODE builds.
 *
 * When building with UNICODE defined, this two macros
 * Curl_convert_UTF8_to_tchar() and Curl_convert_tchar_to_UTF8()
 * return a pointer to a newly allocated memory area holding result.
 * When the result is no longer needed, allocated memory is intended
 * to be free'ed with Curl_unicodefree().
 *
 * When building without UNICODE defined, this macros
 * Curl_convert_UTF8_to_tchar() and Curl_convert_tchar_to_UTF8()
 * return the pointer received as argument. Curl_unicodefree() does
 * no actual free'ing of this pointer it is simply set to NULL.
 */

#ifdef UNICODE

#define Curl_convert_UTF8_to_tchar(ptr) Curl_convert_UTF8_to_wchar((ptr))
#define Curl_convert_tchar_to_UTF8(ptr) Curl_convert_wchar_to_UTF8((ptr))
#define Curl_unicodefree(ptr) \
  do {if((ptr)) {free((ptr)); (ptr) = NULL;}} WHILE_FALSE

typedef union {
  unsigned short       *tchar_ptr;
  const unsigned short *const_tchar_ptr;
  unsigned short       *tbyte_ptr;
  const unsigned short *const_tbyte_ptr;
} xcharp_u;

#else

#define Curl_convert_UTF8_to_tchar(ptr) (ptr)
#define Curl_convert_tchar_to_UTF8(ptr) (ptr)
#define Curl_unicodefree(ptr) \
  do {(ptr) = NULL;} WHILE_FALSE

typedef union {
  char                *tchar_ptr;
  const char          *const_tchar_ptr;
  unsigned char       *tbyte_ptr;
  const unsigned char *const_tbyte_ptr;
} xcharp_u;

#endif /* UNICODE */

#endif /* USE_WIN32_IDN || USE_WINDOWS_SSPI || USE_WIN32_LDAP */

#endif /* HEADER_CURL_MULTIBYTE_H */
