#ifndef HEADER_CURL_HTTP_PROXY_H
#define HEADER_CURL_HTTP_PROXY_H
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
#include "urldata.h"

#if !defined(CURL_DISABLE_PROXY) && !defined(CURL_DISABLE_HTTP)
/* ftp can use this as well */
CURLcode Curl_proxyCONNECT(struct connectdata *conn,
                           int tunnelsocket,
                           const char *hostname, int remote_port);

/* Default proxy timeout in milliseconds */
#define PROXY_TIMEOUT (3600*1000)

CURLcode Curl_proxy_connect(struct connectdata *conn, int sockindex);

bool Curl_connect_complete(struct connectdata *conn);
bool Curl_connect_ongoing(struct connectdata *conn);

#else
#define Curl_proxyCONNECT(x,y,z,w) CURLE_NOT_BUILT_IN
#define Curl_proxy_connect(x,y) CURLE_OK
#define Curl_connect_complete(x) CURLE_OK
#define Curl_connect_ongoing(x) FALSE
#endif

void Curl_connect_free(struct Curl_easy *data);

#endif /* HEADER_CURL_HTTP_PROXY_H */
