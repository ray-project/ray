#ifndef HEADER_CURL_PROGRESS_H
#define HEADER_CURL_PROGRESS_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2018, Daniel Stenberg, <daniel@haxx.se>, et al.
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

#include "timeval.h"


typedef enum {
  TIMER_NONE,
  TIMER_STARTOP,
  TIMER_STARTSINGLE,
  TIMER_NAMELOOKUP,
  TIMER_CONNECT,
  TIMER_APPCONNECT,
  TIMER_PRETRANSFER,
  TIMER_STARTTRANSFER,
  TIMER_POSTRANSFER,
  TIMER_STARTACCEPT,
  TIMER_REDIRECT,
  TIMER_LAST /* must be last */
} timerid;

int Curl_pgrsDone(struct connectdata *);
void Curl_pgrsStartNow(struct Curl_easy *data);
void Curl_pgrsSetDownloadSize(struct Curl_easy *data, curl_off_t size);
void Curl_pgrsSetUploadSize(struct Curl_easy *data, curl_off_t size);
void Curl_pgrsSetDownloadCounter(struct Curl_easy *data, curl_off_t size);
void Curl_pgrsSetUploadCounter(struct Curl_easy *data, curl_off_t size);
void Curl_ratelimit(struct Curl_easy *data, struct curltime now);
int Curl_pgrsUpdate(struct connectdata *);
void Curl_pgrsResetTransferSizes(struct Curl_easy *data);
void Curl_pgrsTime(struct Curl_easy *data, timerid timer);
timediff_t Curl_pgrsLimitWaitTime(curl_off_t cursize,
                                  curl_off_t startsize,
                                  curl_off_t limit,
                                  struct curltime start,
                                  struct curltime now);

/* Don't show progress for sizes smaller than: */
#define LEAST_SIZE_PROGRESS BUFSIZE

#define PROGRESS_DOWNLOAD (1<<0)
#define PROGRESS_UPLOAD   (1<<1)
#define PROGRESS_DOWN_AND_UP (PROGRESS_UPLOAD | PROGRESS_DOWNLOAD)

#define PGRS_SHOW_DL (1<<0)
#define PGRS_SHOW_UL (1<<1)
#define PGRS_DONE_DL (1<<2)
#define PGRS_DONE_UL (1<<3)
#define PGRS_HIDE    (1<<4)
#define PGRS_UL_SIZE_KNOWN (1<<5)
#define PGRS_DL_SIZE_KNOWN (1<<6)

#define PGRS_HEADERS_OUT (1<<7) /* set when the headers have been written */


#endif /* HEADER_CURL_PROGRESS_H */

