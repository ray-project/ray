#ifndef HEADER_CURL_SASL_H
#define HEADER_CURL_SASL_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 2012 - 2016, Daniel Stenberg, <daniel@haxx.se>, et al.
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

#include <curl/curl.h>

struct Curl_easy;
struct connectdata;

/* Authentication mechanism flags */
#define SASL_MECH_LOGIN             (1 << 0)
#define SASL_MECH_PLAIN             (1 << 1)
#define SASL_MECH_CRAM_MD5          (1 << 2)
#define SASL_MECH_DIGEST_MD5        (1 << 3)
#define SASL_MECH_GSSAPI            (1 << 4)
#define SASL_MECH_EXTERNAL          (1 << 5)
#define SASL_MECH_NTLM              (1 << 6)
#define SASL_MECH_XOAUTH2           (1 << 7)
#define SASL_MECH_OAUTHBEARER       (1 << 8)

/* Authentication mechanism values */
#define SASL_AUTH_NONE          0
#define SASL_AUTH_ANY           ~0U
#define SASL_AUTH_DEFAULT       (SASL_AUTH_ANY & ~SASL_MECH_EXTERNAL)

/* Authentication mechanism strings */
#define SASL_MECH_STRING_LOGIN        "LOGIN"
#define SASL_MECH_STRING_PLAIN        "PLAIN"
#define SASL_MECH_STRING_CRAM_MD5     "CRAM-MD5"
#define SASL_MECH_STRING_DIGEST_MD5   "DIGEST-MD5"
#define SASL_MECH_STRING_GSSAPI       "GSSAPI"
#define SASL_MECH_STRING_EXTERNAL     "EXTERNAL"
#define SASL_MECH_STRING_NTLM         "NTLM"
#define SASL_MECH_STRING_XOAUTH2      "XOAUTH2"
#define SASL_MECH_STRING_OAUTHBEARER  "OAUTHBEARER"

/* SASL machine states */
typedef enum {
  SASL_STOP,
  SASL_PLAIN,
  SASL_LOGIN,
  SASL_LOGIN_PASSWD,
  SASL_EXTERNAL,
  SASL_CRAMMD5,
  SASL_DIGESTMD5,
  SASL_DIGESTMD5_RESP,
  SASL_NTLM,
  SASL_NTLM_TYPE2MSG,
  SASL_GSSAPI,
  SASL_GSSAPI_TOKEN,
  SASL_GSSAPI_NO_DATA,
  SASL_OAUTH2,
  SASL_OAUTH2_RESP,
  SASL_CANCEL,
  SASL_FINAL
} saslstate;

/* Progress indicator */
typedef enum {
  SASL_IDLE,
  SASL_INPROGRESS,
  SASL_DONE
} saslprogress;

/* Protocol dependent SASL parameters */
struct SASLproto {
  const char *service;     /* The service name */
  int contcode;            /* Code to receive when continuation is expected */
  int finalcode;           /* Code to receive upon authentication success */
  size_t maxirlen;         /* Maximum initial response length */
  CURLcode (*sendauth)(struct connectdata *conn,
                       const char *mech, const char *ir);
                           /* Send authentication command */
  CURLcode (*sendcont)(struct connectdata *conn, const char *contauth);
                           /* Send authentication continuation */
  void (*getmessage)(char *buffer, char **outptr);
                           /* Get SASL response message */
};

/* Per-connection parameters */
struct SASL {
  const struct SASLproto *params; /* Protocol dependent parameters */
  saslstate state;         /* Current machine state */
  unsigned int authmechs;  /* Accepted authentication mechanisms */
  unsigned int prefmech;   /* Preferred authentication mechanism */
  unsigned int authused;   /* Auth mechanism used for the connection */
  bool resetprefs;         /* For URL auth option parsing. */
  bool mutual_auth;        /* Mutual authentication enabled (GSSAPI only) */
  bool force_ir;           /* Protocol always supports initial response */
};

/* This is used to test whether the line starts with the given mechanism */
#define sasl_mech_equal(line, wordlen, mech) \
  (wordlen == (sizeof(mech) - 1) / sizeof(char) && \
   !memcmp(line, mech, wordlen))

/* This is used to cleanup any libraries or curl modules used by the sasl
   functions */
void Curl_sasl_cleanup(struct connectdata *conn, unsigned int authused);

/* Convert a mechanism name to a token */
unsigned int Curl_sasl_decode_mech(const char *ptr,
                                   size_t maxlen, size_t *len);

/* Parse the URL login options */
CURLcode Curl_sasl_parse_url_auth_option(struct SASL *sasl,
                                         const char *value, size_t len);

/* Initializes an SASL structure */
void Curl_sasl_init(struct SASL *sasl, const struct SASLproto *params);

/* Check if we have enough auth data and capabilities to authenticate */
bool Curl_sasl_can_authenticate(struct SASL *sasl, struct connectdata *conn);

/* Calculate the required login details for SASL authentication  */
CURLcode Curl_sasl_start(struct SASL *sasl, struct connectdata *conn,
                         bool force_ir, saslprogress *progress);

/* Continue an SASL authentication  */
CURLcode Curl_sasl_continue(struct SASL *sasl, struct connectdata *conn,
                            int code, saslprogress *progress);

#endif /* HEADER_CURL_SASL_H */
