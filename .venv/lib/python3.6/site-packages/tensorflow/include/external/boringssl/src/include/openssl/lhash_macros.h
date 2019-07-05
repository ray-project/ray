/* Copyright (c) 2014, Google Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

#if !defined(IN_LHASH_H)
#error "Don't include this file directly. Include lhash.h"
#endif

// ASN1_OBJECT
#define lh_ASN1_OBJECT_new(hash, comp)                                       \
  ((LHASH_OF(ASN1_OBJECT) *)lh_new(                                          \
      CHECKED_CAST(lhash_hash_func, uint32_t(*)(const ASN1_OBJECT *), hash), \
      CHECKED_CAST(lhash_cmp_func,                                           \
                   int (*)(const ASN1_OBJECT *a, const ASN1_OBJECT *b),      \
                   comp)))

#define lh_ASN1_OBJECT_free(lh) \
  lh_free(CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh));

#define lh_ASN1_OBJECT_num_items(lh) \
  lh_num_items(CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh))

#define lh_ASN1_OBJECT_retrieve(lh, data)                  \
  ((ASN1_OBJECT *)lh_retrieve(                             \
      CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh), \
      CHECKED_CAST(void *, ASN1_OBJECT *, data)))

#define lh_ASN1_OBJECT_retrieve_key(lh, key, key_hash, cmp_key)           \
  ((ASN1_OBJECT *)lh_retrieve_key(                                        \
      CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh), key, key_hash, \
      CHECKED_CAST(int (*)(const void *, const void *),                   \
                   int (*)(const void *, const ASN1_OBJECT *), cmp_key)))

#define lh_ASN1_OBJECT_insert(lh, old_data, data)                \
  lh_insert(CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh), \
            CHECKED_CAST(void **, ASN1_OBJECT **, old_data),     \
            CHECKED_CAST(void *, ASN1_OBJECT *, data))

#define lh_ASN1_OBJECT_delete(lh, data)                    \
  ((ASN1_OBJECT *)lh_delete(                               \
      CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh), \
      CHECKED_CAST(void *, ASN1_OBJECT *, data)))

#define lh_ASN1_OBJECT_doall(lh, func)                          \
  lh_doall(CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh), \
           CHECKED_CAST(void (*)(void *), void (*)(ASN1_OBJECT *), func));

#define lh_ASN1_OBJECT_doall_arg(lh, func, arg)                     \
  lh_doall_arg(CHECKED_CAST(_LHASH *, LHASH_OF(ASN1_OBJECT) *, lh), \
               CHECKED_CAST(void (*)(void *, void *),               \
                            void (*)(ASN1_OBJECT *, void *), func), \
               arg);


// CONF_VALUE
#define lh_CONF_VALUE_new(hash, comp)                                       \
  ((LHASH_OF(CONF_VALUE) *)lh_new(                                          \
      CHECKED_CAST(lhash_hash_func, uint32_t(*)(const CONF_VALUE *), hash), \
      CHECKED_CAST(lhash_cmp_func,                                          \
                   int (*)(const CONF_VALUE *a, const CONF_VALUE *b), comp)))

#define lh_CONF_VALUE_free(lh) \
  lh_free(CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh));

#define lh_CONF_VALUE_num_items(lh) \
  lh_num_items(CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh))

#define lh_CONF_VALUE_retrieve(lh, data)                  \
  ((CONF_VALUE *)lh_retrieve(                             \
      CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh), \
      CHECKED_CAST(void *, CONF_VALUE *, data)))

#define lh_CONF_VALUE_retrieve_key(lh, key, key_hash, cmp_key)           \
  ((CONF_VALUE *)lh_retrieve_key(                                        \
      CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh), key, key_hash, \
      CHECKED_CAST(int (*)(const void *, const void *),                  \
                   int (*)(const void *, const CONF_VALUE *), cmp_key)))

#define lh_CONF_VALUE_insert(lh, old_data, data)                \
  lh_insert(CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh), \
            CHECKED_CAST(void **, CONF_VALUE **, old_data),     \
            CHECKED_CAST(void *, CONF_VALUE *, data))

#define lh_CONF_VALUE_delete(lh, data)                                         \
  ((CONF_VALUE *)lh_delete(CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh), \
                           CHECKED_CAST(void *, CONF_VALUE *, data)))

#define lh_CONF_VALUE_doall(lh, func)                          \
  lh_doall(CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh), \
           CHECKED_CAST(void (*)(void *), void (*)(CONF_VALUE *), func));

#define lh_CONF_VALUE_doall_arg(lh, func, arg)                     \
  lh_doall_arg(CHECKED_CAST(_LHASH *, LHASH_OF(CONF_VALUE) *, lh), \
               CHECKED_CAST(void (*)(void *, void *),              \
                            void (*)(CONF_VALUE *, void *), func), \
               arg);


// CRYPTO_BUFFER
#define lh_CRYPTO_BUFFER_new(hash, comp)                                       \
  ((LHASH_OF(CRYPTO_BUFFER) *)lh_new(                                          \
      CHECKED_CAST(lhash_hash_func, uint32_t(*)(const CRYPTO_BUFFER *), hash), \
      CHECKED_CAST(lhash_cmp_func,                                             \
                   int (*)(const CRYPTO_BUFFER *a, const CRYPTO_BUFFER *b),    \
                   comp)))

#define lh_CRYPTO_BUFFER_free(lh) \
  lh_free(CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh));

#define lh_CRYPTO_BUFFER_num_items(lh) \
  lh_num_items(CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh))

#define lh_CRYPTO_BUFFER_retrieve(lh, data)                  \
  ((CRYPTO_BUFFER *)lh_retrieve(                             \
      CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh), \
      CHECKED_CAST(void *, CRYPTO_BUFFER *, data)))

#define lh_CRYPTO_BUFFER_retrieve_key(lh, key, key_hash, cmp_key)           \
  ((CRYPTO_BUFFER *)lh_retrieve_key(                                        \
      CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh), key, key_hash, \
      CHECKED_CAST(int (*)(const void *, const void *),                     \
                   int (*)(const void *, const CRYPTO_BUFFER *), cmp_key)))

#define lh_CRYPTO_BUFFER_insert(lh, old_data, data)                \
  lh_insert(CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh), \
            CHECKED_CAST(void **, CRYPTO_BUFFER **, old_data),     \
            CHECKED_CAST(void *, CRYPTO_BUFFER *, data))

#define lh_CRYPTO_BUFFER_delete(lh, data)                    \
  ((CRYPTO_BUFFER *)lh_delete(                               \
      CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh), \
      CHECKED_CAST(void *, CRYPTO_BUFFER *, data)))

#define lh_CRYPTO_BUFFER_doall(lh, func)                          \
  lh_doall(CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh), \
           CHECKED_CAST(void (*)(void *), void (*)(CRYPTO_BUFFER *), func));

#define lh_CRYPTO_BUFFER_doall_arg(lh, func, arg)                     \
  lh_doall_arg(CHECKED_CAST(_LHASH *, LHASH_OF(CRYPTO_BUFFER) *, lh), \
               CHECKED_CAST(void (*)(void *, void *),                 \
                            void (*)(CRYPTO_BUFFER *, void *), func), \
               arg);


// SSL_SESSION
#define lh_SSL_SESSION_new(hash, comp)                                       \
  ((LHASH_OF(SSL_SESSION) *)lh_new(                                          \
      CHECKED_CAST(lhash_hash_func, uint32_t(*)(const SSL_SESSION *), hash), \
      CHECKED_CAST(lhash_cmp_func,                                           \
                   int (*)(const SSL_SESSION *a, const SSL_SESSION *b),      \
                   comp)))

#define lh_SSL_SESSION_free(lh) \
  lh_free(CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh));

#define lh_SSL_SESSION_num_items(lh) \
  lh_num_items(CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh))

#define lh_SSL_SESSION_retrieve(lh, data)                  \
  ((SSL_SESSION *)lh_retrieve(                             \
      CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh), \
      CHECKED_CAST(void *, SSL_SESSION *, data)))

#define lh_SSL_SESSION_retrieve_key(lh, key, key_hash, cmp_key)           \
  ((SSL_SESSION *)lh_retrieve_key(                                        \
      CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh), key, key_hash, \
      CHECKED_CAST(int (*)(const void *, const void *),                   \
                   int (*)(const void *, const SSL_SESSION *), cmp_key)))

#define lh_SSL_SESSION_insert(lh, old_data, data)                \
  lh_insert(CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh), \
            CHECKED_CAST(void **, SSL_SESSION **, old_data),     \
            CHECKED_CAST(void *, SSL_SESSION *, data))

#define lh_SSL_SESSION_delete(lh, data)                    \
  ((SSL_SESSION *)lh_delete(                               \
      CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh), \
      CHECKED_CAST(void *, SSL_SESSION *, data)))

#define lh_SSL_SESSION_doall(lh, func)                          \
  lh_doall(CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh), \
           CHECKED_CAST(void (*)(void *), void (*)(SSL_SESSION *), func));

#define lh_SSL_SESSION_doall_arg(lh, func, arg)                     \
  lh_doall_arg(CHECKED_CAST(_LHASH *, LHASH_OF(SSL_SESSION) *, lh), \
               CHECKED_CAST(void (*)(void *, void *),               \
                            void (*)(SSL_SESSION *, void *), func), \
               arg);
