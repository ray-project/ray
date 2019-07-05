#ifndef HEADER_CURL_MEMDEBUG_H
#define HEADER_CURL_MEMDEBUG_H
#ifdef CURLDEBUG
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

/*
 * CAUTION: this header is designed to work when included by the app-side
 * as well as the library. Do not mix with library internals!
 */

#define CURL_MT_LOGFNAME_BUFSIZE 512

#define logfile curl_debuglogfile

extern FILE *logfile;

/* memory functions */
CURL_EXTERN void *curl_domalloc(size_t size, int line, const char *source);
CURL_EXTERN void *curl_docalloc(size_t elements, size_t size, int line,
                                const char *source);
CURL_EXTERN void *curl_dorealloc(void *ptr, size_t size, int line,
                                 const char *source);
CURL_EXTERN void curl_dofree(void *ptr, int line, const char *source);
CURL_EXTERN char *curl_dostrdup(const char *str, int line, const char *source);
#if defined(WIN32) && defined(UNICODE)
CURL_EXTERN wchar_t *curl_dowcsdup(const wchar_t *str, int line,
                                   const char *source);
#endif

CURL_EXTERN void curl_memdebug(const char *logname);
CURL_EXTERN void curl_memlimit(long limit);
CURL_EXTERN void curl_memlog(const char *format, ...);

/* file descriptor manipulators */
CURL_EXTERN curl_socket_t curl_socket(int domain, int type, int protocol,
                                      int line, const char *source);
CURL_EXTERN void curl_mark_sclose(curl_socket_t sockfd,
                                  int line, const char *source);
CURL_EXTERN int curl_sclose(curl_socket_t sockfd,
                            int line, const char *source);
CURL_EXTERN curl_socket_t curl_accept(curl_socket_t s, void *a, void *alen,
                                      int line, const char *source);
#ifdef HAVE_SOCKETPAIR
CURL_EXTERN int curl_socketpair(int domain, int type, int protocol,
                                curl_socket_t socket_vector[2],
                                int line, const char *source);
#endif

/* send/receive sockets */
CURL_EXTERN SEND_TYPE_RETV curl_dosend(SEND_TYPE_ARG1 sockfd,
                                       SEND_QUAL_ARG2 SEND_TYPE_ARG2 buf,
                                       SEND_TYPE_ARG3 len,
                                       SEND_TYPE_ARG4 flags, int line,
                                       const char *source);
CURL_EXTERN RECV_TYPE_RETV curl_dorecv(RECV_TYPE_ARG1 sockfd,
                                       RECV_TYPE_ARG2 buf, RECV_TYPE_ARG3 len,
                                       RECV_TYPE_ARG4 flags, int line,
                                       const char *source);

/* FILE functions */
CURL_EXTERN FILE *curl_fopen(const char *file, const char *mode, int line,
                             const char *source);
#ifdef HAVE_FDOPEN
CURL_EXTERN FILE *curl_fdopen(int filedes, const char *mode, int line,
                              const char *source);
#endif
CURL_EXTERN int curl_fclose(FILE *file, int line, const char *source);

#ifndef MEMDEBUG_NODEFINES

/* Set this symbol on the command-line, recompile all lib-sources */
#undef strdup
#define strdup(ptr) curl_dostrdup(ptr, __LINE__, __FILE__)
#define malloc(size) curl_domalloc(size, __LINE__, __FILE__)
#define calloc(nbelem,size) curl_docalloc(nbelem, size, __LINE__, __FILE__)
#define realloc(ptr,size) curl_dorealloc(ptr, size, __LINE__, __FILE__)
#define free(ptr) curl_dofree(ptr, __LINE__, __FILE__)
#define send(a,b,c,d) curl_dosend(a,b,c,d, __LINE__, __FILE__)
#define recv(a,b,c,d) curl_dorecv(a,b,c,d, __LINE__, __FILE__)

#ifdef WIN32
#  ifdef UNICODE
#    undef wcsdup
#    define wcsdup(ptr) curl_dowcsdup(ptr, __LINE__, __FILE__)
#    undef _wcsdup
#    define _wcsdup(ptr) curl_dowcsdup(ptr, __LINE__, __FILE__)
#    undef _tcsdup
#    define _tcsdup(ptr) curl_dowcsdup(ptr, __LINE__, __FILE__)
#  else
#    undef _tcsdup
#    define _tcsdup(ptr) curl_dostrdup(ptr, __LINE__, __FILE__)
#  endif
#endif

#undef socket
#define socket(domain,type,protocol)\
 curl_socket(domain, type, protocol, __LINE__, __FILE__)
#undef accept /* for those with accept as a macro */
#define accept(sock,addr,len)\
 curl_accept(sock, addr, len, __LINE__, __FILE__)
#ifdef HAVE_SOCKETPAIR
#define socketpair(domain,type,protocol,socket_vector)\
 curl_socketpair(domain, type, protocol, socket_vector, __LINE__, __FILE__)
#endif

#ifdef HAVE_GETADDRINFO
#if defined(getaddrinfo) && defined(__osf__)
/* OSF/1 and Tru64 have getaddrinfo as a define already, so we cannot define
   our macro as for other platforms. Instead, we redefine the new name they
   define getaddrinfo to become! */
#define ogetaddrinfo(host,serv,hint,res) \
  curl_dogetaddrinfo(host, serv, hint, res, __LINE__, __FILE__)
#else
#undef getaddrinfo
#define getaddrinfo(host,serv,hint,res) \
  curl_dogetaddrinfo(host, serv, hint, res, __LINE__, __FILE__)
#endif
#endif /* HAVE_GETADDRINFO */

#ifdef HAVE_GETNAMEINFO
#undef getnameinfo
#define getnameinfo(sa,salen,host,hostlen,serv,servlen,flags) \
  curl_dogetnameinfo(sa, salen, host, hostlen, serv, servlen, flags, \
                     __LINE__, __FILE__)
#endif /* HAVE_GETNAMEINFO */

#ifdef HAVE_FREEADDRINFO
#undef freeaddrinfo
#define freeaddrinfo(data) \
  curl_dofreeaddrinfo(data, __LINE__, __FILE__)
#endif /* HAVE_FREEADDRINFO */

/* sclose is probably already defined, redefine it! */
#undef sclose
#define sclose(sockfd) curl_sclose(sockfd,__LINE__,__FILE__)

#define fake_sclose(sockfd) curl_mark_sclose(sockfd,__LINE__,__FILE__)

#undef fopen
#define fopen(file,mode) curl_fopen(file,mode,__LINE__,__FILE__)
#undef fdopen
#define fdopen(file,mode) curl_fdopen(file,mode,__LINE__,__FILE__)
#define fclose(file) curl_fclose(file,__LINE__,__FILE__)

#endif /* MEMDEBUG_NODEFINES */

#endif /* CURLDEBUG */

/*
** Following section applies even when CURLDEBUG is not defined.
*/

#ifndef fake_sclose
#define fake_sclose(x)  Curl_nop_stmt
#endif

/*
 * Curl_safefree defined as a macro to allow MemoryTracking feature
 * to log free() calls at same location where Curl_safefree is used.
 * This macro also assigns NULL to given pointer when free'd.
 */

#define Curl_safefree(ptr) \
  do { free((ptr)); (ptr) = NULL;} WHILE_FALSE

#endif /* HEADER_CURL_MEMDEBUG_H */
