/* ====================================================================
 * Copyright (c) 2011 The OpenSSL Project.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit. (http://www.openssl.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    openssl-core@openssl.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.openssl.org/)"
 *
 * THIS SOFTWARE IS PROVIDED BY THE OpenSSL PROJECT ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE OpenSSL PROJECT OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 */

#include <assert.h>
#include <string.h>

#include <openssl/cpu.h>
#include <openssl/mem.h>

#include "../../internal.h"
#include "internal.h"


struct ccm128_state {
  union {
    uint64_t u[2];
    uint8_t c[16];
  } nonce, cmac;
};

int CRYPTO_ccm128_init(CCM128_CONTEXT *ctx, const void *key, block128_f block,
                       ctr128_f ctr, unsigned M, unsigned L) {
  if (M < 4 || M > 16 || (M & 1) != 0 || L < 2 || L > 8) {
    return 0;
  }
  ctx->block = block;
  ctx->ctr = ctr;
  ctx->M = M;
  ctx->L = L;
  return 1;
}

size_t CRYPTO_ccm128_max_input(const CCM128_CONTEXT *ctx) {
  return ctx->L >= sizeof(size_t) ? (size_t)-1
                                  : (((size_t)1) << (ctx->L * 8)) - 1;
}

static int ccm128_init_state(const CCM128_CONTEXT *ctx,
                             struct ccm128_state *state, const void *key,
                             const uint8_t *nonce, size_t nonce_len,
                             const uint8_t *aad, size_t aad_len,
                             size_t plaintext_len) {
  const block128_f block = ctx->block;
  const unsigned M = ctx->M;
  const unsigned L = ctx->L;

  // |L| determines the expected |nonce_len| and the limit for |plaintext_len|.
  if (plaintext_len > CRYPTO_ccm128_max_input(ctx) ||
      nonce_len != 15 - L) {
    return 0;
  }

  // Assemble the first block for computing the MAC.
  OPENSSL_memset(state, 0, sizeof(*state));
  state->nonce.c[0] = (uint8_t)((L - 1) | ((M - 2) / 2) << 3);
  if (aad_len != 0) {
    state->nonce.c[0] |= 0x40;  // Set AAD Flag
  }
  OPENSSL_memcpy(&state->nonce.c[1], nonce, nonce_len);
  for (unsigned i = 0; i < L; i++) {
    state->nonce.c[15 - i] = (uint8_t)(plaintext_len >> (8 * i));
  }

  (*block)(state->nonce.c, state->cmac.c, key);
  size_t blocks = 1;

  if (aad_len != 0) {
    unsigned i;
    // Cast to u64 to avoid the compiler complaining about invalid shifts.
    uint64_t aad_len_u64 = aad_len;
    if (aad_len_u64 < 0x10000 - 0x100) {
      state->cmac.c[0] ^= (uint8_t)(aad_len_u64 >> 8);
      state->cmac.c[1] ^= (uint8_t)aad_len_u64;
      i = 2;
    } else if (aad_len_u64 <= 0xffffffff) {
      state->cmac.c[0] ^= 0xff;
      state->cmac.c[1] ^= 0xfe;
      state->cmac.c[2] ^= (uint8_t)(aad_len_u64 >> 24);
      state->cmac.c[3] ^= (uint8_t)(aad_len_u64 >> 16);
      state->cmac.c[4] ^= (uint8_t)(aad_len_u64 >> 8);
      state->cmac.c[5] ^= (uint8_t)aad_len_u64;
      i = 6;
    } else {
      state->cmac.c[0] ^= 0xff;
      state->cmac.c[1] ^= 0xff;
      state->cmac.c[2] ^= (uint8_t)(aad_len_u64 >> 56);
      state->cmac.c[3] ^= (uint8_t)(aad_len_u64 >> 48);
      state->cmac.c[4] ^= (uint8_t)(aad_len_u64 >> 40);
      state->cmac.c[5] ^= (uint8_t)(aad_len_u64 >> 32);
      state->cmac.c[6] ^= (uint8_t)(aad_len_u64 >> 24);
      state->cmac.c[7] ^= (uint8_t)(aad_len_u64 >> 16);
      state->cmac.c[8] ^= (uint8_t)(aad_len_u64 >> 8);
      state->cmac.c[9] ^= (uint8_t)aad_len_u64;
      i = 10;
    }

    do {
      for (; i < 16 && aad_len != 0; i++) {
        state->cmac.c[i] ^= *aad;
        aad++;
        aad_len--;
      }
      (*block)(state->cmac.c, state->cmac.c, key);
      blocks++;
      i = 0;
    } while (aad_len != 0);
  }

  // Per RFC 3610, section 2.6, the total number of block cipher operations done
  // must not exceed 2^61. There are two block cipher operations remaining per
  // message block, plus one block at the end to encrypt the MAC.
  size_t remaining_blocks = 2 * ((plaintext_len + 15) / 16) + 1;
  if (plaintext_len + 15 < plaintext_len ||
      remaining_blocks + blocks < blocks ||
      (uint64_t) remaining_blocks + blocks > UINT64_C(1) << 61) {
    return 0;
  }

  // Assemble the first block for encrypting and decrypting. The bottom |L|
  // bytes are replaced with a counter and all bit the encoding of |L| is
  // cleared in the first byte.
  state->nonce.c[0] &= 7;
  return 1;
}

static int ccm128_encrypt(const CCM128_CONTEXT *ctx, struct ccm128_state *state,
                          const void *key, uint8_t *out, const uint8_t *in,
                          size_t len) {
  // The counter for encryption begins at one.
  for (unsigned i = 0; i < ctx->L; i++) {
    state->nonce.c[15 - i] = 0;
  }
  state->nonce.c[15] = 1;

  uint8_t partial_buf[16];
  unsigned num = 0;
  if (ctx->ctr != NULL) {
    CRYPTO_ctr128_encrypt_ctr32(in, out, len, key, state->nonce.c, partial_buf,
                                &num, ctx->ctr);
  } else {
    CRYPTO_ctr128_encrypt(in, out, len, key, state->nonce.c, partial_buf, &num,
                          ctx->block);
  }
  return 1;
}

static int ccm128_compute_mac(const CCM128_CONTEXT *ctx,
                              struct ccm128_state *state, const void *key,
                              uint8_t *out_tag, size_t tag_len,
                              const uint8_t *in, size_t len) {
  block128_f block = ctx->block;
  if (tag_len != ctx->M) {
    return 0;
  }

  // Incorporate |in| into the MAC.
  union {
    uint64_t u[2];
    uint8_t c[16];
  } tmp;
  while (len >= 16) {
    OPENSSL_memcpy(tmp.c, in, 16);
    state->cmac.u[0] ^= tmp.u[0];
    state->cmac.u[1] ^= tmp.u[1];
    (*block)(state->cmac.c, state->cmac.c, key);
    in += 16;
    len -= 16;
  }
  if (len > 0) {
    for (size_t i = 0; i < len; i++) {
      state->cmac.c[i] ^= in[i];
    }
    (*block)(state->cmac.c, state->cmac.c, key);
  }

  // Encrypt the MAC with counter zero.
  for (unsigned i = 0; i < ctx->L; i++) {
    state->nonce.c[15 - i] = 0;
  }
  (*block)(state->nonce.c, tmp.c, key);
  state->cmac.u[0] ^= tmp.u[0];
  state->cmac.u[1] ^= tmp.u[1];

  OPENSSL_memcpy(out_tag, state->cmac.c, tag_len);
  return 1;
}

int CRYPTO_ccm128_encrypt(const CCM128_CONTEXT *ctx, const void *key,
                          uint8_t *out, uint8_t *out_tag, size_t tag_len,
                          const uint8_t *nonce, size_t nonce_len,
                          const uint8_t *in, size_t len, const uint8_t *aad,
                          size_t aad_len) {
  struct ccm128_state state;
  return ccm128_init_state(ctx, &state, key, nonce, nonce_len, aad, aad_len,
                           len) &&
         ccm128_compute_mac(ctx, &state, key, out_tag, tag_len, in, len) &&
         ccm128_encrypt(ctx, &state, key, out, in, len);
}

int CRYPTO_ccm128_decrypt(const CCM128_CONTEXT *ctx, const void *key,
                          uint8_t *out, uint8_t *out_tag, size_t tag_len,
                          const uint8_t *nonce, size_t nonce_len,
                          const uint8_t *in, size_t len, const uint8_t *aad,
                          size_t aad_len) {
  struct ccm128_state state;
  return ccm128_init_state(ctx, &state, key, nonce, nonce_len, aad, aad_len,
                           len) &&
         ccm128_encrypt(ctx, &state, key, out, in, len) &&
         ccm128_compute_mac(ctx, &state, key, out_tag, tag_len, out, len);
}
