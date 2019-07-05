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

#ifndef OPENSSL_HEADER_CIPHER_EXTRA_INTERNAL_H
#define OPENSSL_HEADER_CIPHER_EXTRA_INTERNAL_H

#include <openssl/base.h>

#include "../internal.h"

#if defined(__cplusplus)
extern "C" {
#endif


// EVP_tls_cbc_get_padding determines the padding from the decrypted, TLS, CBC
// record in |in|. This decrypted record should not include any "decrypted"
// explicit IV. If the record is publicly invalid, it returns zero. Otherwise,
// it returns one and sets |*out_padding_ok| to all ones (0xfff..f) if the
// padding is valid and zero otherwise. It then sets |*out_len| to the length
// with the padding removed or |in_len| if invalid.
//
// If the function returns one, it runs in time independent of the contents of
// |in|. It is also guaranteed that |*out_len| >= |mac_size|, satisfying
// |EVP_tls_cbc_copy_mac|'s precondition.
int EVP_tls_cbc_remove_padding(crypto_word_t *out_padding_ok, size_t *out_len,
                               const uint8_t *in, size_t in_len,
                               size_t block_size, size_t mac_size);

// EVP_tls_cbc_copy_mac copies |md_size| bytes from the end of the first
// |in_len| bytes of |in| to |out| in constant time (independent of the concrete
// value of |in_len|, which may vary within a 256-byte window). |in| must point
// to a buffer of |orig_len| bytes.
//
// On entry:
//   orig_len >= in_len >= md_size
//   md_size <= EVP_MAX_MD_SIZE
void EVP_tls_cbc_copy_mac(uint8_t *out, size_t md_size, const uint8_t *in,
                          size_t in_len, size_t orig_len);

// EVP_tls_cbc_record_digest_supported returns 1 iff |md| is a hash function
// which EVP_tls_cbc_digest_record supports.
int EVP_tls_cbc_record_digest_supported(const EVP_MD *md);

// EVP_tls_cbc_digest_record computes the MAC of a decrypted, padded TLS
// record.
//
//   md: the hash function used in the HMAC.
//     EVP_tls_cbc_record_digest_supported must return true for this hash.
//   md_out: the digest output. At most EVP_MAX_MD_SIZE bytes will be written.
//   md_out_size: the number of output bytes is written here.
//   header: the 13-byte, TLS record header.
//   data: the record data itself
//   data_plus_mac_size: the secret, reported length of the data and MAC
//     once the padding has been removed.
//   data_plus_mac_plus_padding_size: the public length of the whole
//     record, including padding.
//
// On entry: by virtue of having been through one of the remove_padding
// functions, above, we know that data_plus_mac_size is large enough to contain
// a padding byte and MAC. (If the padding was invalid, it might contain the
// padding too. )
int EVP_tls_cbc_digest_record(const EVP_MD *md, uint8_t *md_out,
                              size_t *md_out_size, const uint8_t header[13],
                              const uint8_t *data, size_t data_plus_mac_size,
                              size_t data_plus_mac_plus_padding_size,
                              const uint8_t *mac_secret,
                              unsigned mac_secret_length);


#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_CIPHER_EXTRA_INTERNAL_H
