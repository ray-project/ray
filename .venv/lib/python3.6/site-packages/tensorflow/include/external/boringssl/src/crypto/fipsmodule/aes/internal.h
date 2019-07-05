/* Copyright (c) 2017, Google Inc.
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

#ifndef OPENSSL_HEADER_AES_INTERNAL_H
#define OPENSSL_HEADER_AES_INTERNAL_H

#include <stdlib.h>

#include <openssl/cpu.h>

#if defined(__cplusplus)
extern "C" {
#endif


#if !defined(OPENSSL_NO_ASM)

#if defined(OPENSSL_X86_64)
#define HWAES
#define HWAES_ECB

static int hwaes_capable(void) {
  return (OPENSSL_ia32cap_P[1] & (1 << (57 - 32))) != 0;
}
#elif defined(OPENSSL_ARM) || defined(OPENSSL_AARCH64)
#define HWAES

static int hwaes_capable(void) {
  return CRYPTO_is_ARMv8_AES_capable();
}
#elif defined(OPENSSL_PPC64LE)
#define HWAES

static int hwaes_capable(void) {
  return CRYPTO_is_PPC64LE_vcrypto_capable();
}
#endif

#endif  // !NO_ASM


#if defined(HWAES)

int aes_hw_set_encrypt_key(const uint8_t *user_key, const int bits,
                           AES_KEY *key);
int aes_hw_set_decrypt_key(const uint8_t *user_key, const int bits,
                           AES_KEY *key);
void aes_hw_encrypt(const uint8_t *in, uint8_t *out, const AES_KEY *key);
void aes_hw_decrypt(const uint8_t *in, uint8_t *out, const AES_KEY *key);
void aes_hw_cbc_encrypt(const uint8_t *in, uint8_t *out, size_t length,
                        const AES_KEY *key, uint8_t *ivec, const int enc);
void aes_hw_ctr32_encrypt_blocks(const uint8_t *in, uint8_t *out, size_t len,
                                 const AES_KEY *key, const uint8_t ivec[16]);

#else

// If HWAES isn't defined then we provide dummy functions for each of the hwaes
// functions.
static int hwaes_capable(void) { return 0; }

static int aes_hw_set_encrypt_key(const uint8_t *user_key, int bits,
                                  AES_KEY *key) {
  abort();
}

static int aes_hw_set_decrypt_key(const uint8_t *user_key, int bits,
                                  AES_KEY *key) {
  abort();
}

static void aes_hw_encrypt(const uint8_t *in, uint8_t *out,
                           const AES_KEY *key) {
  abort();
}

static void aes_hw_decrypt(const uint8_t *in, uint8_t *out,
                           const AES_KEY *key) {
  abort();
}

static void aes_hw_cbc_encrypt(const uint8_t *in, uint8_t *out, size_t length,
                               const AES_KEY *key, uint8_t *ivec, int enc) {
  abort();
}

static void aes_hw_ctr32_encrypt_blocks(const uint8_t *in, uint8_t *out,
                                        size_t len, const AES_KEY *key,
                                        const uint8_t ivec[16]) {
  abort();
}

#endif  // !HWAES


#if defined(HWAES_ECB)
void aes_hw_ecb_encrypt(const uint8_t *in, uint8_t *out, size_t length,
                        const AES_KEY *key, const int enc);
#endif

#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_AES_INTERNAL_H
