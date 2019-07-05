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

#include <assert.h>
#include <string.h>

#include <openssl/err.h>
#include <openssl/mem.h>
#include <openssl/nid.h>

#include "internal.h"
#include "../../internal.h"


void EVP_CIPHER_CTX_init(EVP_CIPHER_CTX *ctx) {
  OPENSSL_memset(ctx, 0, sizeof(EVP_CIPHER_CTX));
}

EVP_CIPHER_CTX *EVP_CIPHER_CTX_new(void) {
  EVP_CIPHER_CTX *ctx = OPENSSL_malloc(sizeof(EVP_CIPHER_CTX));
  if (ctx) {
    EVP_CIPHER_CTX_init(ctx);
  }
  return ctx;
}

int EVP_CIPHER_CTX_cleanup(EVP_CIPHER_CTX *c) {
  if (c->cipher != NULL && c->cipher->cleanup) {
    c->cipher->cleanup(c);
  }
  OPENSSL_free(c->cipher_data);

  OPENSSL_memset(c, 0, sizeof(EVP_CIPHER_CTX));
  return 1;
}

void EVP_CIPHER_CTX_free(EVP_CIPHER_CTX *ctx) {
  if (ctx) {
    EVP_CIPHER_CTX_cleanup(ctx);
    OPENSSL_free(ctx);
  }
}

int EVP_CIPHER_CTX_copy(EVP_CIPHER_CTX *out, const EVP_CIPHER_CTX *in) {
  if (in == NULL || in->cipher == NULL) {
    OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_INPUT_NOT_INITIALIZED);
    return 0;
  }

  EVP_CIPHER_CTX_cleanup(out);
  OPENSSL_memcpy(out, in, sizeof(EVP_CIPHER_CTX));

  if (in->cipher_data && in->cipher->ctx_size) {
    out->cipher_data = OPENSSL_malloc(in->cipher->ctx_size);
    if (!out->cipher_data) {
      out->cipher = NULL;
      OPENSSL_PUT_ERROR(CIPHER, ERR_R_MALLOC_FAILURE);
      return 0;
    }
    OPENSSL_memcpy(out->cipher_data, in->cipher_data, in->cipher->ctx_size);
  }

  if (in->cipher->flags & EVP_CIPH_CUSTOM_COPY) {
    if (!in->cipher->ctrl((EVP_CIPHER_CTX *)in, EVP_CTRL_COPY, 0, out)) {
      out->cipher = NULL;
      return 0;
    }
  }

  return 1;
}

void EVP_CIPHER_CTX_reset(EVP_CIPHER_CTX *ctx) {
  EVP_CIPHER_CTX_cleanup(ctx);
  EVP_CIPHER_CTX_init(ctx);
}

int EVP_CipherInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                      ENGINE *engine, const uint8_t *key, const uint8_t *iv,
                      int enc) {
  if (enc == -1) {
    enc = ctx->encrypt;
  } else {
    if (enc) {
      enc = 1;
    }
    ctx->encrypt = enc;
  }

  if (cipher) {
    // Ensure a context left from last time is cleared (the previous check
    // attempted to avoid this if the same ENGINE and EVP_CIPHER could be
    // used).
    if (ctx->cipher) {
      EVP_CIPHER_CTX_cleanup(ctx);
      // Restore encrypt and flags
      ctx->encrypt = enc;
    }

    ctx->cipher = cipher;
    if (ctx->cipher->ctx_size) {
      ctx->cipher_data = OPENSSL_malloc(ctx->cipher->ctx_size);
      if (!ctx->cipher_data) {
        ctx->cipher = NULL;
        OPENSSL_PUT_ERROR(CIPHER, ERR_R_MALLOC_FAILURE);
        return 0;
      }
    } else {
      ctx->cipher_data = NULL;
    }

    ctx->key_len = cipher->key_len;
    ctx->flags = 0;

    if (ctx->cipher->flags & EVP_CIPH_CTRL_INIT) {
      if (!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_INIT, 0, NULL)) {
        ctx->cipher = NULL;
        OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_INITIALIZATION_ERROR);
        return 0;
      }
    }
  } else if (!ctx->cipher) {
    OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_NO_CIPHER_SET);
    return 0;
  }

  // we assume block size is a power of 2 in *cryptUpdate
  assert(ctx->cipher->block_size == 1 || ctx->cipher->block_size == 8 ||
         ctx->cipher->block_size == 16);

  if (!(EVP_CIPHER_CTX_flags(ctx) & EVP_CIPH_CUSTOM_IV)) {
    switch (EVP_CIPHER_CTX_mode(ctx)) {
      case EVP_CIPH_STREAM_CIPHER:
      case EVP_CIPH_ECB_MODE:
        break;

      case EVP_CIPH_CFB_MODE:
        ctx->num = 0;
        // fall-through

      case EVP_CIPH_CBC_MODE:
        assert(EVP_CIPHER_CTX_iv_length(ctx) <= sizeof(ctx->iv));
        if (iv) {
          OPENSSL_memcpy(ctx->oiv, iv, EVP_CIPHER_CTX_iv_length(ctx));
        }
        OPENSSL_memcpy(ctx->iv, ctx->oiv, EVP_CIPHER_CTX_iv_length(ctx));
        break;

      case EVP_CIPH_CTR_MODE:
      case EVP_CIPH_OFB_MODE:
        ctx->num = 0;
        // Don't reuse IV for CTR mode
        if (iv) {
          OPENSSL_memcpy(ctx->iv, iv, EVP_CIPHER_CTX_iv_length(ctx));
        }
        break;

      default:
        return 0;
    }
  }

  if (key || (ctx->cipher->flags & EVP_CIPH_ALWAYS_CALL_INIT)) {
    if (!ctx->cipher->init(ctx, key, iv, enc)) {
      return 0;
    }
  }

  ctx->buf_len = 0;
  ctx->final_used = 0;
  ctx->block_mask = ctx->cipher->block_size - 1;
  return 1;
}

int EVP_EncryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                       ENGINE *impl, const uint8_t *key, const uint8_t *iv) {
  return EVP_CipherInit_ex(ctx, cipher, impl, key, iv, 1);
}

int EVP_DecryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                       ENGINE *impl, const uint8_t *key, const uint8_t *iv) {
  return EVP_CipherInit_ex(ctx, cipher, impl, key, iv, 0);
}

int EVP_EncryptUpdate(EVP_CIPHER_CTX *ctx, uint8_t *out, int *out_len,
                      const uint8_t *in, int in_len) {
  int i, j, bl;

  if (ctx->cipher->flags & EVP_CIPH_FLAG_CUSTOM_CIPHER) {
    i = ctx->cipher->cipher(ctx, out, in, in_len);
    if (i < 0) {
      return 0;
    } else {
      *out_len = i;
    }
    return 1;
  }

  if (in_len <= 0) {
    *out_len = 0;
    return in_len == 0;
  }

  if (ctx->buf_len == 0 && (in_len & ctx->block_mask) == 0) {
    if (ctx->cipher->cipher(ctx, out, in, in_len)) {
      *out_len = in_len;
      return 1;
    } else {
      *out_len = 0;
      return 0;
    }
  }

  i = ctx->buf_len;
  bl = ctx->cipher->block_size;
  assert(bl <= (int)sizeof(ctx->buf));
  if (i != 0) {
    if (bl - i > in_len) {
      OPENSSL_memcpy(&ctx->buf[i], in, in_len);
      ctx->buf_len += in_len;
      *out_len = 0;
      return 1;
    } else {
      j = bl - i;
      OPENSSL_memcpy(&ctx->buf[i], in, j);
      if (!ctx->cipher->cipher(ctx, out, ctx->buf, bl)) {
        return 0;
      }
      in_len -= j;
      in += j;
      out += bl;
      *out_len = bl;
    }
  } else {
    *out_len = 0;
  }

  i = in_len & ctx->block_mask;
  in_len -= i;
  if (in_len > 0) {
    if (!ctx->cipher->cipher(ctx, out, in, in_len)) {
      return 0;
    }
    *out_len += in_len;
  }

  if (i != 0) {
    OPENSSL_memcpy(ctx->buf, &in[in_len], i);
  }
  ctx->buf_len = i;
  return 1;
}

int EVP_EncryptFinal_ex(EVP_CIPHER_CTX *ctx, uint8_t *out, int *out_len) {
  int n, ret;
  unsigned int i, b, bl;

  if (ctx->cipher->flags & EVP_CIPH_FLAG_CUSTOM_CIPHER) {
    ret = ctx->cipher->cipher(ctx, out, NULL, 0);
    if (ret < 0) {
      return 0;
    } else {
      *out_len = ret;
    }
    return 1;
  }

  b = ctx->cipher->block_size;
  assert(b <= sizeof(ctx->buf));
  if (b == 1) {
    *out_len = 0;
    return 1;
  }

  bl = ctx->buf_len;
  if (ctx->flags & EVP_CIPH_NO_PADDING) {
    if (bl) {
      OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_DATA_NOT_MULTIPLE_OF_BLOCK_LENGTH);
      return 0;
    }
    *out_len = 0;
    return 1;
  }

  n = b - bl;
  for (i = bl; i < b; i++) {
    ctx->buf[i] = n;
  }
  ret = ctx->cipher->cipher(ctx, out, ctx->buf, b);

  if (ret) {
    *out_len = b;
  }

  return ret;
}

int EVP_DecryptUpdate(EVP_CIPHER_CTX *ctx, uint8_t *out, int *out_len,
                      const uint8_t *in, int in_len) {
  int fix_len;
  unsigned int b;

  if (ctx->cipher->flags & EVP_CIPH_FLAG_CUSTOM_CIPHER) {
    int r = ctx->cipher->cipher(ctx, out, in, in_len);
    if (r < 0) {
      *out_len = 0;
      return 0;
    } else {
      *out_len = r;
    }
    return 1;
  }

  if (in_len <= 0) {
    *out_len = 0;
    return in_len == 0;
  }

  if (ctx->flags & EVP_CIPH_NO_PADDING) {
    return EVP_EncryptUpdate(ctx, out, out_len, in, in_len);
  }

  b = ctx->cipher->block_size;
  assert(b <= sizeof(ctx->final));

  if (ctx->final_used) {
    OPENSSL_memcpy(out, ctx->final, b);
    out += b;
    fix_len = 1;
  } else {
    fix_len = 0;
  }

  if (!EVP_EncryptUpdate(ctx, out, out_len, in, in_len)) {
    return 0;
  }

  // if we have 'decrypted' a multiple of block size, make sure
  // we have a copy of this last block
  if (b > 1 && !ctx->buf_len) {
    *out_len -= b;
    ctx->final_used = 1;
    OPENSSL_memcpy(ctx->final, &out[*out_len], b);
  } else {
    ctx->final_used = 0;
  }

  if (fix_len) {
    *out_len += b;
  }

  return 1;
}

int EVP_DecryptFinal_ex(EVP_CIPHER_CTX *ctx, unsigned char *out, int *out_len) {
  int i, n;
  unsigned int b;
  *out_len = 0;

  if (ctx->cipher->flags & EVP_CIPH_FLAG_CUSTOM_CIPHER) {
    i = ctx->cipher->cipher(ctx, out, NULL, 0);
    if (i < 0) {
      return 0;
    } else {
      *out_len = i;
    }
    return 1;
  }

  b = ctx->cipher->block_size;
  if (ctx->flags & EVP_CIPH_NO_PADDING) {
    if (ctx->buf_len) {
      OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_DATA_NOT_MULTIPLE_OF_BLOCK_LENGTH);
      return 0;
    }
    *out_len = 0;
    return 1;
  }

  if (b > 1) {
    if (ctx->buf_len || !ctx->final_used) {
      OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_WRONG_FINAL_BLOCK_LENGTH);
      return 0;
    }
    assert(b <= sizeof(ctx->final));

    // The following assumes that the ciphertext has been authenticated.
    // Otherwise it provides a padding oracle.
    n = ctx->final[b - 1];
    if (n == 0 || n > (int)b) {
      OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_BAD_DECRYPT);
      return 0;
    }

    for (i = 0; i < n; i++) {
      if (ctx->final[--b] != n) {
        OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_BAD_DECRYPT);
        return 0;
      }
    }

    n = ctx->cipher->block_size - n;
    for (i = 0; i < n; i++) {
      out[i] = ctx->final[i];
    }
    *out_len = n;
  } else {
    *out_len = 0;
  }

  return 1;
}

int EVP_Cipher(EVP_CIPHER_CTX *ctx, uint8_t *out, const uint8_t *in,
               size_t in_len) {
  return ctx->cipher->cipher(ctx, out, in, in_len);
}

int EVP_CipherUpdate(EVP_CIPHER_CTX *ctx, uint8_t *out, int *out_len,
                     const uint8_t *in, int in_len) {
  if (ctx->encrypt) {
    return EVP_EncryptUpdate(ctx, out, out_len, in, in_len);
  } else {
    return EVP_DecryptUpdate(ctx, out, out_len, in, in_len);
  }
}

int EVP_CipherFinal_ex(EVP_CIPHER_CTX *ctx, uint8_t *out, int *out_len) {
  if (ctx->encrypt) {
    return EVP_EncryptFinal_ex(ctx, out, out_len);
  } else {
    return EVP_DecryptFinal_ex(ctx, out, out_len);
  }
}

const EVP_CIPHER *EVP_CIPHER_CTX_cipher(const EVP_CIPHER_CTX *ctx) {
  return ctx->cipher;
}

int EVP_CIPHER_CTX_nid(const EVP_CIPHER_CTX *ctx) {
  return ctx->cipher->nid;
}

int EVP_CIPHER_CTX_encrypting(const EVP_CIPHER_CTX *ctx) {
  return ctx->encrypt;
}

unsigned EVP_CIPHER_CTX_block_size(const EVP_CIPHER_CTX *ctx) {
  return ctx->cipher->block_size;
}

unsigned EVP_CIPHER_CTX_key_length(const EVP_CIPHER_CTX *ctx) {
  return ctx->key_len;
}

unsigned EVP_CIPHER_CTX_iv_length(const EVP_CIPHER_CTX *ctx) {
  return ctx->cipher->iv_len;
}

void *EVP_CIPHER_CTX_get_app_data(const EVP_CIPHER_CTX *ctx) {
  return ctx->app_data;
}

void EVP_CIPHER_CTX_set_app_data(EVP_CIPHER_CTX *ctx, void *data) {
  ctx->app_data = data;
}

uint32_t EVP_CIPHER_CTX_flags(const EVP_CIPHER_CTX *ctx) {
  return ctx->cipher->flags & ~EVP_CIPH_MODE_MASK;
}

uint32_t EVP_CIPHER_CTX_mode(const EVP_CIPHER_CTX *ctx) {
  return ctx->cipher->flags & EVP_CIPH_MODE_MASK;
}

int EVP_CIPHER_CTX_ctrl(EVP_CIPHER_CTX *ctx, int command, int arg, void *ptr) {
  int ret;
  if (!ctx->cipher) {
    OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_NO_CIPHER_SET);
    return 0;
  }

  if (!ctx->cipher->ctrl) {
    OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_CTRL_NOT_IMPLEMENTED);
    return 0;
  }

  ret = ctx->cipher->ctrl(ctx, command, arg, ptr);
  if (ret == -1) {
    OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_CTRL_OPERATION_NOT_IMPLEMENTED);
    return 0;
  }

  return ret;
}

int EVP_CIPHER_CTX_set_padding(EVP_CIPHER_CTX *ctx, int pad) {
  if (pad) {
    ctx->flags &= ~EVP_CIPH_NO_PADDING;
  } else {
    ctx->flags |= EVP_CIPH_NO_PADDING;
  }
  return 1;
}

int EVP_CIPHER_CTX_set_key_length(EVP_CIPHER_CTX *c, unsigned key_len) {
  if (c->key_len == key_len) {
    return 1;
  }

  if (key_len == 0 || !(c->cipher->flags & EVP_CIPH_VARIABLE_LENGTH)) {
    OPENSSL_PUT_ERROR(CIPHER, CIPHER_R_INVALID_KEY_LENGTH);
    return 0;
  }

  c->key_len = key_len;
  return 1;
}

int EVP_CIPHER_nid(const EVP_CIPHER *cipher) { return cipher->nid; }

unsigned EVP_CIPHER_block_size(const EVP_CIPHER *cipher) {
  return cipher->block_size;
}

unsigned EVP_CIPHER_key_length(const EVP_CIPHER *cipher) {
  return cipher->key_len;
}

unsigned EVP_CIPHER_iv_length(const EVP_CIPHER *cipher) {
  return cipher->iv_len;
}

uint32_t EVP_CIPHER_flags(const EVP_CIPHER *cipher) {
  return cipher->flags & ~EVP_CIPH_MODE_MASK;
}

uint32_t EVP_CIPHER_mode(const EVP_CIPHER *cipher) {
  return cipher->flags & EVP_CIPH_MODE_MASK;
}

int EVP_CipherInit(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                   const uint8_t *key, const uint8_t *iv, int enc) {
  if (cipher) {
    EVP_CIPHER_CTX_init(ctx);
  }
  return EVP_CipherInit_ex(ctx, cipher, NULL, key, iv, enc);
}

int EVP_EncryptInit(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                    const uint8_t *key, const uint8_t *iv) {
  return EVP_CipherInit(ctx, cipher, key, iv, 1);
}

int EVP_DecryptInit(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                    const uint8_t *key, const uint8_t *iv) {
  return EVP_CipherInit(ctx, cipher, key, iv, 0);
}

int EVP_add_cipher_alias(const char *a, const char *b) {
  return 1;
}

void EVP_CIPHER_CTX_set_flags(const EVP_CIPHER_CTX *ctx, uint32_t flags) {}
