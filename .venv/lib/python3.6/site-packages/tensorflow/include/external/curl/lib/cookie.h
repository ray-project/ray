#ifndef HEADER_CURL_COOKIE_H
#define HEADER_CURL_COOKIE_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2017, Daniel Stenberg, <daniel@haxx.se>, et al.
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

#include <curl/curl.h>

struct Cookie {
  struct Cookie *next; /* next in the chain */
  char *name;        /* <this> = value */
  char *value;       /* name = <this> */
  char *path;         /* path = <this> which is in Set-Cookie: */
  char *spath;        /* sanitized cookie path */
  char *domain;      /* domain = <this> */
  curl_off_t expires;  /* expires = <this> */
  char *expirestr;   /* the plain text version */
  bool tailmatch;    /* weather we do tail-matchning of the domain name */

  /* RFC 2109 keywords. Version=1 means 2109-compliant cookie sending */
  char *version;     /* Version = <value> */
  char *maxage;      /* Max-Age = <value> */

  bool secure;       /* whether the 'secure' keyword was used */
  bool livecookie;   /* updated from a server, not a stored file */
  bool httponly;     /* true if the httponly directive is present */
};

#define COOKIE_HASH_SIZE 256

struct CookieInfo {
  /* linked list of cookies we know of */
  struct Cookie *cookies[COOKIE_HASH_SIZE];

  char *filename;  /* file we read from/write to */
  bool running;    /* state info, for cookie adding information */
  long numcookies; /* number of cookies in the "jar" */
  bool newsession; /* new session, discard session cookies on load */
};

/* This is the maximum line length we accept for a cookie line. RFC 2109
   section 6.3 says:

   "at least 4096 bytes per cookie (as measured by the size of the characters
   that comprise the cookie non-terminal in the syntax description of the
   Set-Cookie header)"

   We allow max 5000 bytes cookie header. Max 4095 bytes length per cookie
   name and value. Name + value may not exceed 4096 bytes.

*/
#define MAX_COOKIE_LINE 5000

/* This is the maximum length of a cookie name or content we deal with: */
#define MAX_NAME 4096
#define MAX_NAME_TXT "4095"

struct Curl_easy;
/*
 * Add a cookie to the internal list of cookies. The domain and path arguments
 * are only used if the header boolean is TRUE.
 */

struct Cookie *Curl_cookie_add(struct Curl_easy *data,
                               struct CookieInfo *, bool header, bool noexpiry,
                               char *lineptr,
                               const char *domain, const char *path);

struct Cookie *Curl_cookie_getlist(struct CookieInfo *, const char *,
                                   const char *, bool);
void Curl_cookie_freelist(struct Cookie *cookies);
void Curl_cookie_clearall(struct CookieInfo *cookies);
void Curl_cookie_clearsess(struct CookieInfo *cookies);

#if defined(CURL_DISABLE_HTTP) || defined(CURL_DISABLE_COOKIES)
#define Curl_cookie_list(x) NULL
#define Curl_cookie_loadfiles(x) Curl_nop_stmt
#define Curl_cookie_init(x,y,z,w) NULL
#define Curl_cookie_cleanup(x) Curl_nop_stmt
#define Curl_flush_cookies(x,y) Curl_nop_stmt
#else
void Curl_flush_cookies(struct Curl_easy *data, int cleanup);
void Curl_cookie_cleanup(struct CookieInfo *);
struct CookieInfo *Curl_cookie_init(struct Curl_easy *data,
                                    const char *, struct CookieInfo *, bool);
struct curl_slist *Curl_cookie_list(struct Curl_easy *data);
void Curl_cookie_loadfiles(struct Curl_easy *data);
#endif

#endif /* HEADER_CURL_COOKIE_H */
