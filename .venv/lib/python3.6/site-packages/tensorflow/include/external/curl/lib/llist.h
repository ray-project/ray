#ifndef HEADER_CURL_LLIST_H
#define HEADER_CURL_LLIST_H
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
#include <stddef.h>

typedef void (*curl_llist_dtor)(void *, void *);

struct curl_llist_element {
  void *ptr;
  struct curl_llist_element *prev;
  struct curl_llist_element *next;
};

struct curl_llist {
  struct curl_llist_element *head;
  struct curl_llist_element *tail;
  curl_llist_dtor dtor;
  size_t size;
};

void Curl_llist_init(struct curl_llist *, curl_llist_dtor);
void Curl_llist_insert_next(struct curl_llist *, struct curl_llist_element *,
                            const void *, struct curl_llist_element *node);
void Curl_llist_remove(struct curl_llist *, struct curl_llist_element *,
                       void *);
size_t Curl_llist_count(struct curl_llist *);
void Curl_llist_destroy(struct curl_llist *, void *);
void Curl_llist_move(struct curl_llist *, struct curl_llist_element *,
                     struct curl_llist *, struct curl_llist_element *);

#endif /* HEADER_CURL_LLIST_H */

