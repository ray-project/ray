/* Copyright (c) 2015, Google Inc.
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

#ifndef OPENSSL_HEADER_CRYPTO_RAND_INTERNAL_H
#define OPENSSL_HEADER_CRYPTO_RAND_INTERNAL_H

#include <openssl/aes.h>

#include "../../internal.h"
#include "../modes/internal.h"

#if defined(__cplusplus)
extern "C" {
#endif


// RAND_bytes_with_additional_data samples from the RNG after mixing 32 bytes
// from |user_additional_data| in.
void RAND_bytes_with_additional_data(uint8_t *out, size_t out_len,
                                     const uint8_t user_additional_data[32]);

// CRYPTO_sysrand fills |len| bytes at |buf| with entropy from the operating
// system.
void CRYPTO_sysrand(uint8_t *buf, size_t len);

// rand_fork_unsafe_buffering_enabled returns whether fork-unsafe buffering has
// been enabled via |RAND_enable_fork_unsafe_buffering|.
int rand_fork_unsafe_buffering_enabled(void);

// CTR_DRBG_STATE contains the state of a CTR_DRBG based on AES-256. See SP
// 800-90Ar1.
typedef struct {
  AES_KEY ks;
  block128_f block;
  ctr128_f ctr;
  union {
    uint8_t bytes[16];
    uint32_t words[4];
  } counter;
  uint64_t reseed_counter;
} CTR_DRBG_STATE;

// See SP 800-90Ar1, table 3.
#define CTR_DRBG_ENTROPY_LEN 48
#define CTR_DRBG_MAX_GENERATE_LENGTH 65536

// CTR_DRBG_init initialises |*drbg| given |CTR_DRBG_ENTROPY_LEN| bytes of
// entropy in |entropy| and, optionally, a personalization string up to
// |CTR_DRBG_ENTROPY_LEN| bytes in length. It returns one on success and zero
// on error.
OPENSSL_EXPORT int CTR_DRBG_init(CTR_DRBG_STATE *drbg,
                                 const uint8_t entropy[CTR_DRBG_ENTROPY_LEN],
                                 const uint8_t *personalization,
                                 size_t personalization_len);

// CTR_DRBG_reseed reseeds |drbg| given |CTR_DRBG_ENTROPY_LEN| bytes of entropy
// in |entropy| and, optionally, up to |CTR_DRBG_ENTROPY_LEN| bytes of
// additional data. It returns one on success or zero on error.
OPENSSL_EXPORT int CTR_DRBG_reseed(CTR_DRBG_STATE *drbg,
                                   const uint8_t entropy[CTR_DRBG_ENTROPY_LEN],
                                   const uint8_t *additional_data,
                                   size_t additional_data_len);

// CTR_DRBG_generate processes to up |CTR_DRBG_ENTROPY_LEN| bytes of additional
// data (if any) and then writes |out_len| random bytes to |out|, where
// |out_len| <= |CTR_DRBG_MAX_GENERATE_LENGTH|. It returns one on success or
// zero on error.
OPENSSL_EXPORT int CTR_DRBG_generate(CTR_DRBG_STATE *drbg, uint8_t *out,
                                     size_t out_len,
                                     const uint8_t *additional_data,
                                     size_t additional_data_len);

// CTR_DRBG_clear zeroises the state of |drbg|.
OPENSSL_EXPORT void CTR_DRBG_clear(CTR_DRBG_STATE *drbg);


#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_CRYPTO_RAND_INTERNAL_H
