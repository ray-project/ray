#ifndef HEADER_CURL_CONNCACHE_H
#define HEADER_CURL_CONNCACHE_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 2015 - 2017, Daniel Stenberg, <daniel@haxx.se>, et al.
 * Copyright (C) 2012 - 2014, Linus Nielsen Feltzing, <linus@haxx.se>
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
 * All accesses to struct fields and changing of data in the connection cache
 * and connectbundles must be done with the conncache LOCKED. The cache might
 * be shared.
 */

struct conncache {
  struct curl_hash hash;
  size_t num_conn;
  long next_connection_id;
  struct curltime last_cleanup;
  /* handle used for closing cached connections */
  struct Curl_easy *closure_handle;
};

#define BUNDLE_NO_MULTIUSE -1
#define BUNDLE_UNKNOWN     0  /* initial value */
#define BUNDLE_PIPELINING  1
#define BUNDLE_MULTIPLEX   2

struct connectbundle {
  int multiuse;                 /* supports multi-use */
  size_t num_connections;       /* Number of connections in the bundle */
  struct curl_llist conn_list;  /* The connectdata members of the bundle */
};

/* returns 1 on error, 0 is fine */
int Curl_conncache_init(struct conncache *, int size);
void Curl_conncache_destroy(struct conncache *connc);

/* return the correct bundle, to a host or a proxy */
struct connectbundle *Curl_conncache_find_bundle(struct connectdata *conn,
                                                 struct conncache *connc);
void Curl_conncache_unlock(struct connectdata *conn);
/* returns number of connections currently held in the connection cache */
size_t Curl_conncache_size(struct Curl_easy *data);
size_t Curl_conncache_bundle_size(struct connectdata *conn);

bool Curl_conncache_return_conn(struct connectdata *conn);
CURLcode Curl_conncache_add_conn(struct conncache *connc,
                                 struct connectdata *conn);
void Curl_conncache_remove_conn(struct connectdata *conn,
                                bool lock);
bool Curl_conncache_foreach(struct Curl_easy *data,
                            struct conncache *connc,
                            void *param,
                            int (*func)(struct connectdata *conn,
                                        void *param));

struct connectdata *
Curl_conncache_find_first_connection(struct conncache *connc);

struct connectdata *
Curl_conncache_extract_bundle(struct Curl_easy *data,
                              struct connectbundle *bundle);
struct connectdata *
Curl_conncache_extract_oldest(struct Curl_easy *data);
void Curl_conncache_close_all_connections(struct conncache *connc);
void Curl_conncache_print(struct conncache *connc);

#endif /* HEADER_CURL_CONNCACHE_H */
