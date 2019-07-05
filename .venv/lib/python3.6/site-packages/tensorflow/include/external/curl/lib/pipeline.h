#ifndef HEADER_CURL_PIPELINE_H
#define HEADER_CURL_PIPELINE_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 2015 - 2017, Daniel Stenberg, <daniel@haxx.se>, et al.
 * Copyright (C) 2013 - 2014, Linus Nielsen Feltzing, <linus@haxx.se>
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

CURLcode Curl_add_handle_to_pipeline(struct Curl_easy *handle,
                                     struct connectdata *conn);
void Curl_move_handle_from_send_to_recv_pipe(struct Curl_easy *handle,
                                             struct connectdata *conn);
bool Curl_pipeline_penalized(struct Curl_easy *data,
                             struct connectdata *conn);

bool Curl_pipeline_site_blacklisted(struct Curl_easy *handle,
                                    struct connectdata *conn);

CURLMcode Curl_pipeline_set_site_blacklist(char **sites,
                                           struct curl_llist *list_ptr);

bool Curl_pipeline_server_blacklisted(struct Curl_easy *handle,
                                      char *server_name);

CURLMcode Curl_pipeline_set_server_blacklist(char **servers,
                                             struct curl_llist *list_ptr);

bool Curl_pipeline_checkget_write(struct Curl_easy *data,
                                  struct connectdata *conn);
bool Curl_pipeline_checkget_read(struct Curl_easy *data,
                                 struct connectdata *conn);
void Curl_pipeline_leave_write(struct connectdata *conn);
void Curl_pipeline_leave_read(struct connectdata *conn);
bool Curl_recvpipe_head(struct Curl_easy *data,
                        struct connectdata *conn);
bool Curl_sendpipe_head(struct Curl_easy *data,
                        struct connectdata *conn);

#endif /* HEADER_CURL_PIPELINE_H */
