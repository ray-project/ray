#ifndef HEADER_CURL_CONTENT_ENCODING_H
#define HEADER_CURL_CONTENT_ENCODING_H
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

/* Decoding writer. */
typedef struct contenc_writer_s contenc_writer;
typedef struct content_encoding_s content_encoding;

struct contenc_writer_s {
  const content_encoding *handler;  /* Encoding handler. */
  contenc_writer *downstream;  /* Downstream writer. */
  void *params;  /* Encoding-specific storage (variable length). */
};

/* Content encoding writer. */
struct content_encoding_s {
  const char *name;        /* Encoding name. */
  const char *alias;       /* Encoding name alias. */
  CURLcode (*init_writer)(struct connectdata *conn, contenc_writer *writer);
  CURLcode (*unencode_write)(struct connectdata *conn, contenc_writer *writer,
                             const char *buf, size_t nbytes);
  void (*close_writer)(struct connectdata *conn, contenc_writer *writer);
  size_t paramsize;
};


CURLcode Curl_build_unencoding_stack(struct connectdata *conn,
                                     const char *enclist, int maybechunked);
CURLcode Curl_unencode_write(struct connectdata *conn, contenc_writer *writer,
                             const char *buf, size_t nbytes);
void Curl_unencode_cleanup(struct connectdata *conn);
char *Curl_all_content_encodings(void);

#endif /* HEADER_CURL_CONTENT_ENCODING_H */
