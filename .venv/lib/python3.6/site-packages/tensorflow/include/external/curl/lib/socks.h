#ifndef HEADER_CURL_SOCKS_H
#define HEADER_CURL_SOCKS_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2011, Daniel Stenberg, <daniel@haxx.se>, et al.
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

#ifdef CURL_DISABLE_PROXY
#define Curl_SOCKS4(a,b,c,d,e) CURLE_NOT_BUILT_IN
#define Curl_SOCKS5(a,b,c,d,e,f) CURLE_NOT_BUILT_IN
#else
/*
 * Helper read-from-socket functions. Does the same as Curl_read() but it
 * blocks until all bytes amount of buffersize will be read. No more, no less.
 *
 * This is STUPID BLOCKING behaviour which we frown upon, but right now this
 * is what we have...
 */
int Curl_blockread_all(struct connectdata *conn,
                       curl_socket_t sockfd,
                       char *buf,
                       ssize_t buffersize,
                       ssize_t *n);

/*
 * This function logs in to a SOCKS4(a) proxy and sends the specifics to the
 * final destination server.
 */
CURLcode Curl_SOCKS4(const char *proxy_name,
                     const char *hostname,
                     int remote_port,
                     int sockindex,
                     struct connectdata *conn);

/*
 * This function logs in to a SOCKS5 proxy and sends the specifics to the
 * final destination server.
 */
CURLcode Curl_SOCKS5(const char *proxy_name,
                     const char *proxy_password,
                     const char *hostname,
                     int remote_port,
                     int sockindex,
                     struct connectdata *conn);

#if defined(HAVE_GSSAPI) || defined(USE_WINDOWS_SSPI)
/*
 * This function handles the SOCKS5 GSS-API negotiation and initialisation
 */
CURLcode Curl_SOCKS5_gssapi_negotiate(int sockindex,
                                      struct connectdata *conn);
#endif

#endif /* CURL_DISABLE_PROXY */

#endif  /* HEADER_CURL_SOCKS_H */

