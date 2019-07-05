/* ====================================================================
 * Copyright (c) 2008 The OpenSSL Project.  All rights reserved.
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
 * ==================================================================== */

#ifndef OPENSSL_HEADER_MODES_INTERNAL_H
#define OPENSSL_HEADER_MODES_INTERNAL_H

#include <openssl/base.h>

#include <string.h>

#include "../../internal.h"

#if defined(__cplusplus)
extern "C" {
#endif


#define STRICT_ALIGNMENT 1
#if defined(OPENSSL_X86_64) || defined(OPENSSL_X86) || defined(OPENSSL_AARCH64)
#undef STRICT_ALIGNMENT
#define STRICT_ALIGNMENT 0
#endif

static inline uint32_t GETU32(const void *in) {
  uint32_t v;
  OPENSSL_memcpy(&v, in, sizeof(v));
  return CRYPTO_bswap4(v);
}

static inline void PUTU32(void *out, uint32_t v) {
  v = CRYPTO_bswap4(v);
  OPENSSL_memcpy(out, &v, sizeof(v));
}

static inline size_t load_word_le(const void *in) {
  size_t v;
  OPENSSL_memcpy(&v, in, sizeof(v));
  return v;
}

static inline void store_word_le(void *out, size_t v) {
  OPENSSL_memcpy(out, &v, sizeof(v));
}

// block128_f is the type of a 128-bit, block cipher.
typedef void (*block128_f)(const uint8_t in[16], uint8_t out[16],
                           const void *key);

// GCM definitions
typedef struct { uint64_t hi,lo; } u128;

// gmult_func multiplies |Xi| by the GCM key and writes the result back to
// |Xi|.
typedef void (*gmult_func)(uint64_t Xi[2], const u128 Htable[16]);

// ghash_func repeatedly multiplies |Xi| by the GCM key and adds in blocks from
// |inp|. The result is written back to |Xi| and the |len| argument must be a
// multiple of 16.
typedef void (*ghash_func)(uint64_t Xi[2], const u128 Htable[16],
                           const uint8_t *inp, size_t len);

// This differs from upstream's |gcm128_context| in that it does not have the
// |key| pointer, in order to make it |memcpy|-friendly. Rather the key is
// passed into each call that needs it.
struct gcm128_context {
  // Following 6 names follow names in GCM specification
  union {
    uint64_t u[2];
    uint32_t d[4];
    uint8_t c[16];
    size_t t[16 / sizeof(size_t)];
  } Yi, EKi, EK0, len, Xi;

  // Note that the order of |Xi|, |H| and |Htable| is fixed by the MOVBE-based,
  // x86-64, GHASH assembly.
  u128 H;
  u128 Htable[16];
  gmult_func gmult;
  ghash_func ghash;

  unsigned int mres, ares;
  block128_f block;

  // use_aesni_gcm_crypt is true if this context should use the assembly
  // functions |aesni_gcm_encrypt| and |aesni_gcm_decrypt| to process data.
  unsigned use_aesni_gcm_crypt:1;
};

#if defined(OPENSSL_X86) || defined(OPENSSL_X86_64)
// crypto_gcm_clmul_enabled returns one if the CLMUL implementation of GCM is
// used.
int crypto_gcm_clmul_enabled(void);
#endif


// CTR.

// ctr128_f is the type of a function that performs CTR-mode encryption.
typedef void (*ctr128_f)(const uint8_t *in, uint8_t *out, size_t blocks,
                         const void *key, const uint8_t ivec[16]);

// CRYPTO_ctr128_encrypt encrypts (or decrypts, it's the same in CTR mode)
// |len| bytes from |in| to |out| using |block| in counter mode. There's no
// requirement that |len| be a multiple of any value and any partial blocks are
// stored in |ecount_buf| and |*num|, which must be zeroed before the initial
// call. The counter is a 128-bit, big-endian value in |ivec| and is
// incremented by this function.
void CRYPTO_ctr128_encrypt(const uint8_t *in, uint8_t *out, size_t len,
                           const void *key, uint8_t ivec[16],
                           uint8_t ecount_buf[16], unsigned *num,
                           block128_f block);

// CRYPTO_ctr128_encrypt_ctr32 acts like |CRYPTO_ctr128_encrypt| but takes
// |ctr|, a function that performs CTR mode but only deals with the lower 32
// bits of the counter. This is useful when |ctr| can be an optimised
// function.
void CRYPTO_ctr128_encrypt_ctr32(const uint8_t *in, uint8_t *out, size_t len,
                                 const void *key, uint8_t ivec[16],
                                 uint8_t ecount_buf[16], unsigned *num,
                                 ctr128_f ctr);

#if !defined(OPENSSL_NO_ASM) && \
    (defined(OPENSSL_X86) || defined(OPENSSL_X86_64))
void aesni_ctr32_encrypt_blocks(const uint8_t *in, uint8_t *out, size_t blocks,
                                const void *key, const uint8_t *ivec);
#endif


// GCM.
//
// This API differs from the upstream API slightly. The |GCM128_CONTEXT| does
// not have a |key| pointer that points to the key as upstream's version does.
// Instead, every function takes a |key| parameter. This way |GCM128_CONTEXT|
// can be safely copied.

typedef struct gcm128_context GCM128_CONTEXT;

// CRYPTO_ghash_init writes a precomputed table of powers of |gcm_key| to
// |out_table| and sets |*out_mult| and |*out_hash| to (potentially hardware
// accelerated) functions for performing operations in the GHASH field. If the
// AVX implementation was used |*out_is_avx| will be true.
void CRYPTO_ghash_init(gmult_func *out_mult, ghash_func *out_hash,
                       u128 *out_key, u128 out_table[16], int *out_is_avx,
                       const uint8_t *gcm_key);

// CRYPTO_gcm128_init initialises |ctx| to use |block| (typically AES) with
// the given key. |block_is_hwaes| is one if |block| is |aes_hw_encrypt|.
OPENSSL_EXPORT void CRYPTO_gcm128_init(GCM128_CONTEXT *ctx, const void *key,
                                       block128_f block, int block_is_hwaes);

// CRYPTO_gcm128_setiv sets the IV (nonce) for |ctx|. The |key| must be the
// same key that was passed to |CRYPTO_gcm128_init|.
OPENSSL_EXPORT void CRYPTO_gcm128_setiv(GCM128_CONTEXT *ctx, const void *key,
                                        const uint8_t *iv, size_t iv_len);

// CRYPTO_gcm128_aad sets the authenticated data for an instance of GCM.
// This must be called before and data is encrypted. It returns one on success
// and zero otherwise.
OPENSSL_EXPORT int CRYPTO_gcm128_aad(GCM128_CONTEXT *ctx, const uint8_t *aad,
                                     size_t len);

// CRYPTO_gcm128_encrypt encrypts |len| bytes from |in| to |out|. The |key|
// must be the same key that was passed to |CRYPTO_gcm128_init|. It returns one
// on success and zero otherwise.
OPENSSL_EXPORT int CRYPTO_gcm128_encrypt(GCM128_CONTEXT *ctx, const void *key,
                                         const uint8_t *in, uint8_t *out,
                                         size_t len);

// CRYPTO_gcm128_decrypt decrypts |len| bytes from |in| to |out|. The |key|
// must be the same key that was passed to |CRYPTO_gcm128_init|. It returns one
// on success and zero otherwise.
OPENSSL_EXPORT int CRYPTO_gcm128_decrypt(GCM128_CONTEXT *ctx, const void *key,
                                         const uint8_t *in, uint8_t *out,
                                         size_t len);

// CRYPTO_gcm128_encrypt_ctr32 encrypts |len| bytes from |in| to |out| using
// a CTR function that only handles the bottom 32 bits of the nonce, like
// |CRYPTO_ctr128_encrypt_ctr32|. The |key| must be the same key that was
// passed to |CRYPTO_gcm128_init|. It returns one on success and zero
// otherwise.
OPENSSL_EXPORT int CRYPTO_gcm128_encrypt_ctr32(GCM128_CONTEXT *ctx,
                                               const void *key,
                                               const uint8_t *in, uint8_t *out,
                                               size_t len, ctr128_f stream);

// CRYPTO_gcm128_decrypt_ctr32 decrypts |len| bytes from |in| to |out| using
// a CTR function that only handles the bottom 32 bits of the nonce, like
// |CRYPTO_ctr128_encrypt_ctr32|. The |key| must be the same key that was
// passed to |CRYPTO_gcm128_init|. It returns one on success and zero
// otherwise.
OPENSSL_EXPORT int CRYPTO_gcm128_decrypt_ctr32(GCM128_CONTEXT *ctx,
                                               const void *key,
                                               const uint8_t *in, uint8_t *out,
                                               size_t len, ctr128_f stream);

// CRYPTO_gcm128_finish calculates the authenticator and compares it against
// |len| bytes of |tag|. It returns one on success and zero otherwise.
OPENSSL_EXPORT int CRYPTO_gcm128_finish(GCM128_CONTEXT *ctx, const uint8_t *tag,
                                        size_t len);

// CRYPTO_gcm128_tag calculates the authenticator and copies it into |tag|.
// The minimum of |len| and 16 bytes are copied into |tag|.
OPENSSL_EXPORT void CRYPTO_gcm128_tag(GCM128_CONTEXT *ctx, uint8_t *tag,
                                      size_t len);


// CCM.

typedef struct ccm128_context {
  block128_f block;
  ctr128_f ctr;
  unsigned M, L;
} CCM128_CONTEXT;

// CRYPTO_ccm128_init initialises |ctx| to use |block| (typically AES) with the
// specified |M| and |L| parameters. It returns one on success and zero if |M|
// or |L| is invalid.
int CRYPTO_ccm128_init(CCM128_CONTEXT *ctx, const void *key, block128_f block,
                       ctr128_f ctr, unsigned M, unsigned L);

// CRYPTO_ccm128_max_input returns the maximum input length accepted by |ctx|.
size_t CRYPTO_ccm128_max_input(const CCM128_CONTEXT *ctx);

// CRYPTO_ccm128_encrypt encrypts |len| bytes from |in| to |out| writing the tag
// to |out_tag|. |key| must be the same key that was passed to
// |CRYPTO_ccm128_init|. It returns one on success and zero otherwise.
int CRYPTO_ccm128_encrypt(const CCM128_CONTEXT *ctx, const void *key,
                          uint8_t *out, uint8_t *out_tag, size_t tag_len,
                          const uint8_t *nonce, size_t nonce_len,
                          const uint8_t *in, size_t len, const uint8_t *aad,
                          size_t aad_len);

// CRYPTO_ccm128_decrypt decrypts |len| bytes from |in| to |out|, writing the
// expected tag to |out_tag|. |key| must be the same key that was passed to
// |CRYPTO_ccm128_init|. It returns one on success and zero otherwise.
int CRYPTO_ccm128_decrypt(const CCM128_CONTEXT *ctx, const void *key,
                          uint8_t *out, uint8_t *out_tag, size_t tag_len,
                          const uint8_t *nonce, size_t nonce_len,
                          const uint8_t *in, size_t len, const uint8_t *aad,
                          size_t aad_len);


// CBC.

// cbc128_f is the type of a function that performs CBC-mode encryption.
typedef void (*cbc128_f)(const uint8_t *in, uint8_t *out, size_t len,
                         const void *key, uint8_t ivec[16], int enc);

// CRYPTO_cbc128_encrypt encrypts |len| bytes from |in| to |out| using the
// given IV and block cipher in CBC mode. The input need not be a multiple of
// 128 bits long, but the output will round up to the nearest 128 bit multiple,
// zero padding the input if needed. The IV will be updated on return.
void CRYPTO_cbc128_encrypt(const uint8_t *in, uint8_t *out, size_t len,
                           const void *key, uint8_t ivec[16], block128_f block);

// CRYPTO_cbc128_decrypt decrypts |len| bytes from |in| to |out| using the
// given IV and block cipher in CBC mode. If |len| is not a multiple of 128
// bits then only that many bytes will be written, but a multiple of 128 bits
// is always read from |in|. The IV will be updated on return.
void CRYPTO_cbc128_decrypt(const uint8_t *in, uint8_t *out, size_t len,
                           const void *key, uint8_t ivec[16], block128_f block);


// OFB.

// CRYPTO_ofb128_encrypt encrypts (or decrypts, it's the same with OFB mode)
// |len| bytes from |in| to |out| using |block| in OFB mode. There's no
// requirement that |len| be a multiple of any value and any partial blocks are
// stored in |ivec| and |*num|, the latter must be zero before the initial
// call.
void CRYPTO_ofb128_encrypt(const uint8_t *in, uint8_t *out,
                           size_t len, const void *key, uint8_t ivec[16],
                           unsigned *num, block128_f block);


// CFB.

// CRYPTO_cfb128_encrypt encrypts (or decrypts, if |enc| is zero) |len| bytes
// from |in| to |out| using |block| in CFB mode. There's no requirement that
// |len| be a multiple of any value and any partial blocks are stored in |ivec|
// and |*num|, the latter must be zero before the initial call.
void CRYPTO_cfb128_encrypt(const uint8_t *in, uint8_t *out, size_t len,
                           const void *key, uint8_t ivec[16], unsigned *num,
                           int enc, block128_f block);

// CRYPTO_cfb128_8_encrypt encrypts (or decrypts, if |enc| is zero) |len| bytes
// from |in| to |out| using |block| in CFB-8 mode. Prior to the first call
// |num| should be set to zero.
void CRYPTO_cfb128_8_encrypt(const uint8_t *in, uint8_t *out, size_t len,
                             const void *key, uint8_t ivec[16], unsigned *num,
                             int enc, block128_f block);

// CRYPTO_cfb128_1_encrypt encrypts (or decrypts, if |enc| is zero) |len| bytes
// from |in| to |out| using |block| in CFB-1 mode. Prior to the first call
// |num| should be set to zero.
void CRYPTO_cfb128_1_encrypt(const uint8_t *in, uint8_t *out, size_t bits,
                             const void *key, uint8_t ivec[16], unsigned *num,
                             int enc, block128_f block);

size_t CRYPTO_cts128_encrypt_block(const uint8_t *in, uint8_t *out, size_t len,
                                   const void *key, uint8_t ivec[16],
                                   block128_f block);


// POLYVAL.
//
// POLYVAL is a polynomial authenticator that operates over a field very
// similar to the one that GHASH uses. See
// https://tools.ietf.org/html/draft-irtf-cfrg-gcmsiv-02#section-3.

typedef union {
  uint64_t u[2];
  uint8_t c[16];
} polyval_block;

struct polyval_ctx {
  // Note that the order of |S|, |H| and |Htable| is fixed by the MOVBE-based,
  // x86-64, GHASH assembly.
  polyval_block S;
  u128 H;
  u128 Htable[16];
  gmult_func gmult;
  ghash_func ghash;
};

// CRYPTO_POLYVAL_init initialises |ctx| using |key|.
void CRYPTO_POLYVAL_init(struct polyval_ctx *ctx, const uint8_t key[16]);

// CRYPTO_POLYVAL_update_blocks updates the accumulator in |ctx| given the
// blocks from |in|. Only a whole number of blocks can be processed so |in_len|
// must be a multiple of 16.
void CRYPTO_POLYVAL_update_blocks(struct polyval_ctx *ctx, const uint8_t *in,
                                  size_t in_len);

// CRYPTO_POLYVAL_finish writes the accumulator from |ctx| to |out|.
void CRYPTO_POLYVAL_finish(const struct polyval_ctx *ctx, uint8_t out[16]);


#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_MODES_INTERNAL_H
