/* Copyright (C) 1995-1998 Eric Young (eay@cryptsoft.com)
 * All rights reserved.
 *
 * This package is an SSL implementation written
 * by Eric Young (eay@cryptsoft.com).
 * The implementation was written so as to conform with Netscapes SSL.
 *
 * This library is free for commercial and non-commercial use as long as
 * the following conditions are aheared to.  The following conditions
 * apply to all code found in this distribution, be it the RC4, RSA,
 * lhash, DES, etc., code; not just the SSL code.  The SSL documentation
 * included with this distribution is covered by the same copyright terms
 * except that the holder is Tim Hudson (tjh@cryptsoft.com).
 *
 * Copyright remains Eric Young's, and as such any Copyright notices in
 * the code are not to be removed.
 * If this package is used in a product, Eric Young should be given attribution
 * as the author of the parts of the library used.
 * This can be in the form of a textual message at program startup or
 * in documentation (online or textual) provided with the package.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    "This product includes cryptographic software written by
 *     Eric Young (eay@cryptsoft.com)"
 *    The word 'cryptographic' can be left out if the rouines from the library
 *    being used are not cryptographic related :-).
 * 4. If you include any Windows specific code (or a derivative thereof) from
 *    the apps directory (application code) you must include an acknowledgement:
 *    "This product includes software written by Tim Hudson (tjh@cryptsoft.com)"
 *
 * THIS SOFTWARE IS PROVIDED BY ERIC YOUNG ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * The licence and distribution terms for any publically available version or
 * derivative of this code cannot be changed.  i.e. this code cannot simply be
 * copied and put under another distribution licence
 * [including the GNU Public Licence.] */

#include <openssl/cipher.h>
#include <openssl/des.h>
#include <openssl/nid.h>

#include "internal.h"
#include "../delocate.h"


typedef struct {
  union {
    double align;
    DES_key_schedule ks;
  } ks;
} EVP_DES_KEY;

static int des_init_key(EVP_CIPHER_CTX *ctx, const uint8_t *key,
                        const uint8_t *iv, int enc) {
  DES_cblock *deskey = (DES_cblock *)key;
  EVP_DES_KEY *dat = (EVP_DES_KEY *)ctx->cipher_data;

  DES_set_key(deskey, &dat->ks.ks);
  return 1;
}

static int des_cbc_cipher(EVP_CIPHER_CTX *ctx, uint8_t *out, const uint8_t *in,
                          size_t in_len) {
  EVP_DES_KEY *dat = (EVP_DES_KEY *)ctx->cipher_data;

  DES_ncbc_encrypt(in, out, in_len, &dat->ks.ks, (DES_cblock *)ctx->iv,
                   ctx->encrypt);

  return 1;
}

DEFINE_METHOD_FUNCTION(EVP_CIPHER, EVP_des_cbc) {
  memset(out, 0, sizeof(EVP_CIPHER));
  out->nid = NID_des_cbc;
  out->block_size = 8;
  out->key_len = 8;
  out->iv_len = 8;
  out->ctx_size = sizeof(EVP_DES_KEY);
  out->flags = EVP_CIPH_CBC_MODE;
  out->init = des_init_key;
  out->cipher = des_cbc_cipher;
}

static int des_ecb_cipher(EVP_CIPHER_CTX *ctx, uint8_t *out, const uint8_t *in,
                          size_t in_len) {
  if (in_len < ctx->cipher->block_size) {
    return 1;
  }
  in_len -= ctx->cipher->block_size;

  EVP_DES_KEY *dat = (EVP_DES_KEY *) ctx->cipher_data;
  for (size_t i = 0; i <= in_len; i += ctx->cipher->block_size) {
    DES_ecb_encrypt((DES_cblock *) (in + i), (DES_cblock *) (out + i),
                    &dat->ks.ks, ctx->encrypt);
  }
  return 1;
}

DEFINE_METHOD_FUNCTION(EVP_CIPHER, EVP_des_ecb) {
  memset(out, 0, sizeof(EVP_CIPHER));
  out->nid = NID_des_ecb;
  out->block_size = 8;
  out->key_len = 8;
  out->iv_len = 0;
  out->ctx_size = sizeof(EVP_DES_KEY);
  out->flags = EVP_CIPH_ECB_MODE;
  out->init = des_init_key;
  out->cipher = des_ecb_cipher;
}

typedef struct {
  union {
    double align;
    DES_key_schedule ks[3];
  } ks;
} DES_EDE_KEY;

static int des_ede3_init_key(EVP_CIPHER_CTX *ctx, const uint8_t *key,
                             const uint8_t *iv, int enc) {
  DES_cblock *deskey = (DES_cblock *)key;
  DES_EDE_KEY *dat = (DES_EDE_KEY*) ctx->cipher_data;

  DES_set_key(&deskey[0], &dat->ks.ks[0]);
  DES_set_key(&deskey[1], &dat->ks.ks[1]);
  DES_set_key(&deskey[2], &dat->ks.ks[2]);

  return 1;
}

static int des_ede3_cbc_cipher(EVP_CIPHER_CTX *ctx, uint8_t *out,
                              const uint8_t *in, size_t in_len) {
  DES_EDE_KEY *dat = (DES_EDE_KEY*) ctx->cipher_data;

  DES_ede3_cbc_encrypt(in, out, in_len, &dat->ks.ks[0], &dat->ks.ks[1],
                       &dat->ks.ks[2], (DES_cblock *)ctx->iv, ctx->encrypt);

  return 1;
}

DEFINE_METHOD_FUNCTION(EVP_CIPHER, EVP_des_ede3_cbc) {
  memset(out, 0, sizeof(EVP_CIPHER));
  out->nid = NID_des_ede3_cbc;
  out->block_size = 8;
  out->key_len = 24;
  out->iv_len = 8;
  out->ctx_size = sizeof(DES_EDE_KEY);
  out->flags = EVP_CIPH_CBC_MODE;
  out->init = des_ede3_init_key;
  out->cipher = des_ede3_cbc_cipher;
}

static int des_ede_init_key(EVP_CIPHER_CTX *ctx, const uint8_t *key,
                             const uint8_t *iv, int enc) {
  DES_cblock *deskey = (DES_cblock *) key;
  DES_EDE_KEY *dat = (DES_EDE_KEY *) ctx->cipher_data;

  DES_set_key(&deskey[0], &dat->ks.ks[0]);
  DES_set_key(&deskey[1], &dat->ks.ks[1]);
  DES_set_key(&deskey[0], &dat->ks.ks[2]);

  return 1;
}

DEFINE_METHOD_FUNCTION(EVP_CIPHER, EVP_des_ede_cbc) {
  memset(out, 0, sizeof(EVP_CIPHER));
  out->nid = NID_des_ede_cbc;
  out->block_size = 8;
  out->key_len = 16;
  out->iv_len = 8;
  out->ctx_size = sizeof(DES_EDE_KEY);
  out->flags = EVP_CIPH_CBC_MODE;
  out->init = des_ede_init_key;
  out->cipher = des_ede3_cbc_cipher;
}

static int des_ede_ecb_cipher(EVP_CIPHER_CTX *ctx, uint8_t *out,
                              const uint8_t *in, size_t in_len) {
  if (in_len < ctx->cipher->block_size) {
    return 1;
  }
  in_len -= ctx->cipher->block_size;

  DES_EDE_KEY *dat = (DES_EDE_KEY *) ctx->cipher_data;
  for (size_t i = 0; i <= in_len; i += ctx->cipher->block_size) {
    DES_ecb3_encrypt((DES_cblock *) (in + i), (DES_cblock *) (out + i),
                     &dat->ks.ks[0], &dat->ks.ks[1], &dat->ks.ks[2],
                     ctx->encrypt);
  }
  return 1;
}

DEFINE_METHOD_FUNCTION(EVP_CIPHER, EVP_des_ede) {
  memset(out, 0, sizeof(EVP_CIPHER));
  out->nid = NID_des_ede_ecb;
  out->block_size = 8;
  out->key_len = 16;
  out->iv_len = 0;
  out->ctx_size = sizeof(DES_EDE_KEY);
  out->flags = EVP_CIPH_ECB_MODE;
  out->init = des_ede_init_key;
  out->cipher = des_ede_ecb_cipher;
}

DEFINE_METHOD_FUNCTION(EVP_CIPHER, EVP_des_ede3) {
  memset(out, 0, sizeof(EVP_CIPHER));
  out->nid = NID_des_ede3_ecb;
  out->block_size = 8;
  out->key_len = 24;
  out->iv_len = 0;
  out->ctx_size = sizeof(DES_EDE_KEY);
  out->flags = EVP_CIPH_ECB_MODE;
  out->init = des_ede3_init_key;
  out->cipher = des_ede_ecb_cipher;
}
