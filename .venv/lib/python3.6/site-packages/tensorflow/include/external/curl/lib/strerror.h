#ifndef HEADER_CURL_STRERROR_H
#define HEADER_CURL_STRERROR_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2016, Daniel Stenberg, <daniel@haxx.se>, et al.
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

#include "urldata.h"

const char *Curl_strerror (struct connectdata *conn, int err);

#ifdef USE_LIBIDN2
const char *Curl_idn_strerror (struct connectdata *conn, int err);
#endif

#ifdef USE_WINDOWS_SSPI
const char *Curl_sspi_strerror (struct connectdata *conn, int err);
#endif

#endif /* HEADER_CURL_STRERROR_H */
