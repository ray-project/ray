#ifndef HEADER_CURL_FORMDATA_H
#define HEADER_CURL_FORMDATA_H
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

/* used by FormAdd for temporary storage */
typedef struct FormInfo {
  char *name;
  bool name_alloc;
  size_t namelength;
  char *value;
  bool value_alloc;
  curl_off_t contentslength;
  char *contenttype;
  bool contenttype_alloc;
  long flags;
  char *buffer;      /* pointer to existing buffer used for file upload */
  size_t bufferlength;
  char *showfilename; /* The file name to show. If not set, the actual
                         file name will be used */
  bool showfilename_alloc;
  char *userp;        /* pointer for the read callback */
  struct curl_slist *contentheader;
  struct FormInfo *more;
} FormInfo;

CURLcode Curl_getformdata(struct Curl_easy *data,
                          curl_mimepart *,
                          struct curl_httppost *post,
                          curl_read_callback fread_func);

#endif /* HEADER_CURL_FORMDATA_H */
