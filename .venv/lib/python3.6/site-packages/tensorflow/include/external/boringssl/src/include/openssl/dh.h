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

#ifndef OPENSSL_HEADER_DH_H
#define OPENSSL_HEADER_DH_H

#include <openssl/base.h>

#include <openssl/ex_data.h>
#include <openssl/thread.h>

#if defined(__cplusplus)
extern "C" {
#endif


// DH contains functions for performing Diffie-Hellman key agreement in
// multiplicative groups.


// Allocation and destruction.

// DH_new returns a new, empty DH object or NULL on error.
OPENSSL_EXPORT DH *DH_new(void);

// DH_free decrements the reference count of |dh| and frees it if the reference
// count drops to zero.
OPENSSL_EXPORT void DH_free(DH *dh);

// DH_up_ref increments the reference count of |dh| and returns one.
OPENSSL_EXPORT int DH_up_ref(DH *dh);


// Properties.

// DH_get0_key sets |*out_pub_key| and |*out_priv_key|, if non-NULL, to |dh|'s
// public and private key, respectively. If |dh| is a public key, the private
// key will be set to NULL.
OPENSSL_EXPORT void DH_get0_key(const DH *dh, const BIGNUM **out_pub_key,
                                const BIGNUM **out_priv_key);

// DH_set0_key sets |dh|'s public and private key to the specified values. If
// NULL, the field is left unchanged. On success, it takes ownership of each
// argument and returns one. Otherwise, it returns zero.
OPENSSL_EXPORT int DH_set0_key(DH *dh, BIGNUM *pub_key, BIGNUM *priv_key);

// DH_get0_pqg sets |*out_p|, |*out_q|, and |*out_g|, if non-NULL, to |dh|'s p,
// q, and g parameters, respectively.
OPENSSL_EXPORT void DH_get0_pqg(const DH *dh, const BIGNUM **out_p,
                                const BIGNUM **out_q, const BIGNUM **out_g);

// DH_set0_pqg sets |dh|'s p, q, and g parameters to the specified values.  If
// NULL, the field is left unchanged. On success, it takes ownership of each
// argument and returns one. Otherwise, it returns zero. |q| may be NULL, but
// |p| and |g| must either be specified or already configured on |dh|.
OPENSSL_EXPORT int DH_set0_pqg(DH *dh, BIGNUM *p, BIGNUM *q, BIGNUM *g);


// Standard parameters.

// BN_get_rfc3526_prime_1536 sets |*ret| to the 1536-bit MODP group from RFC
// 3526 and returns |ret|. If |ret| is NULL then a fresh |BIGNUM| is allocated
// and returned. It returns NULL on allocation failure.
OPENSSL_EXPORT BIGNUM *BN_get_rfc3526_prime_1536(BIGNUM *ret);


// Parameter generation.

#define DH_GENERATOR_2 2
#define DH_GENERATOR_5 5

// DH_generate_parameters_ex generates a suitable Diffie-Hellman group with a
// prime that is |prime_bits| long and stores it in |dh|. The generator of the
// group will be |generator|, which should be |DH_GENERATOR_2| unless there's a
// good reason to use a different value. The |cb| argument contains a callback
// function that will be called during the generation. See the documentation in
// |bn.h| about this. In addition to the callback invocations from |BN|, |cb|
// will also be called with |event| equal to three when the generation is
// complete.
OPENSSL_EXPORT int DH_generate_parameters_ex(DH *dh, int prime_bits,
                                             int generator, BN_GENCB *cb);


// Diffie-Hellman operations.

// DH_generate_key generates a new, random, private key and stores it in
// |dh|. It returns one on success and zero on error.
OPENSSL_EXPORT int DH_generate_key(DH *dh);

// DH_compute_key calculates the shared key between |dh| and |peers_key| and
// writes it as a big-endian integer into |out|, which must have |DH_size|
// bytes of space. It returns the number of bytes written, or a negative number
// on error.
OPENSSL_EXPORT int DH_compute_key(uint8_t *out, const BIGNUM *peers_key,
                                  DH *dh);


// Utility functions.

// DH_size returns the number of bytes in the DH group's prime.
OPENSSL_EXPORT int DH_size(const DH *dh);

// DH_num_bits returns the minimum number of bits needed to represent the
// absolute value of the DH group's prime.
OPENSSL_EXPORT unsigned DH_num_bits(const DH *dh);

#define DH_CHECK_P_NOT_PRIME 0x01
#define DH_CHECK_P_NOT_SAFE_PRIME 0x02
#define DH_CHECK_UNABLE_TO_CHECK_GENERATOR 0x04
#define DH_CHECK_NOT_SUITABLE_GENERATOR 0x08
#define DH_CHECK_Q_NOT_PRIME 0x10
#define DH_CHECK_INVALID_Q_VALUE 0x20
#define DH_CHECK_INVALID_J_VALUE 0x40

// These are compatibility defines.
#define DH_NOT_SUITABLE_GENERATOR DH_CHECK_NOT_SUITABLE_GENERATOR
#define DH_UNABLE_TO_CHECK_GENERATOR DH_CHECK_UNABLE_TO_CHECK_GENERATOR

// DH_check checks the suitability of |dh| as a Diffie-Hellman group. and sets
// |DH_CHECK_*| flags in |*out_flags| if it finds any errors. It returns one if
// |*out_flags| was successfully set and zero on error.
//
// Note: these checks may be quite computationally expensive.
OPENSSL_EXPORT int DH_check(const DH *dh, int *out_flags);

#define DH_CHECK_PUBKEY_TOO_SMALL 0x1
#define DH_CHECK_PUBKEY_TOO_LARGE 0x2
#define DH_CHECK_PUBKEY_INVALID 0x4

// DH_check_pub_key checks the suitability of |pub_key| as a public key for the
// DH group in |dh| and sets |DH_CHECK_PUBKEY_*| flags in |*out_flags| if it
// finds any errors. It returns one if |*out_flags| was successfully set and
// zero on error.
OPENSSL_EXPORT int DH_check_pub_key(const DH *dh, const BIGNUM *pub_key,
                                    int *out_flags);

// DHparams_dup allocates a fresh |DH| and copies the parameters from |dh| into
// it. It returns the new |DH| or NULL on error.
OPENSSL_EXPORT DH *DHparams_dup(const DH *dh);


// ASN.1 functions.

// DH_parse_parameters decodes a DER-encoded DHParameter structure (PKCS #3)
// from |cbs| and advances |cbs|. It returns a newly-allocated |DH| or NULL on
// error.
OPENSSL_EXPORT DH *DH_parse_parameters(CBS *cbs);

// DH_marshal_parameters marshals |dh| as a DER-encoded DHParameter structure
// (PKCS #3) and appends the result to |cbb|. It returns one on success and zero
// on error.
OPENSSL_EXPORT int DH_marshal_parameters(CBB *cbb, const DH *dh);


// ex_data functions.
//
// See |ex_data.h| for details.

OPENSSL_EXPORT int DH_get_ex_new_index(long argl, void *argp,
                                       CRYPTO_EX_unused *unused,
                                       CRYPTO_EX_dup *dup_unused,
                                       CRYPTO_EX_free *free_func);
OPENSSL_EXPORT int DH_set_ex_data(DH *d, int idx, void *arg);
OPENSSL_EXPORT void *DH_get_ex_data(DH *d, int idx);


// Deprecated functions.

// DH_generate_parameters behaves like |DH_generate_parameters_ex|, which is
// what you should use instead. It returns NULL on error, or a newly-allocated
// |DH| on success. This function is provided for compatibility only.
OPENSSL_EXPORT DH *DH_generate_parameters(int prime_len, int generator,
                                          void (*callback)(int, int, void *),
                                          void *cb_arg);

// d2i_DHparams parses an ASN.1, DER encoded Diffie-Hellman parameters structure
// from |len| bytes at |*inp|. If |ret| is not NULL then, on exit, a pointer to
// the result is in |*ret|. Note that, even if |*ret| is already non-NULL on
// entry, it will not be written to. Rather, a fresh |DH| is allocated and the
// previous one is freed.
//
// On successful exit, |*inp| is advanced past the DER structure. It
// returns the result or NULL on error.
//
// Use |DH_parse_parameters| instead.
OPENSSL_EXPORT DH *d2i_DHparams(DH **ret, const unsigned char **inp, long len);

// i2d_DHparams marshals |in| to an ASN.1, DER structure. If |outp| is not NULL
// then the result is written to |*outp| and |*outp| is advanced just past the
// output. It returns the number of bytes in the result, whether written or
// not, or a negative value on error.
//
// Use |DH_marshal_parameters| instead.
OPENSSL_EXPORT int i2d_DHparams(const DH *in, unsigned char **outp);


struct dh_st {
  BIGNUM *p;
  BIGNUM *g;
  BIGNUM *pub_key;   // g^x mod p
  BIGNUM *priv_key;  // x

  // priv_length contains the length, in bits, of the private value. If zero,
  // the private value will be the same length as |p|.
  unsigned priv_length;

  CRYPTO_MUTEX method_mont_p_lock;
  BN_MONT_CTX *method_mont_p;

  // Place holders if we want to do X9.42 DH
  BIGNUM *q;
  BIGNUM *j;
  unsigned char *seed;
  int seedlen;
  BIGNUM *counter;

  int flags;
  CRYPTO_refcount_t references;
  CRYPTO_EX_DATA ex_data;
};


#if defined(__cplusplus)
}  // extern C

extern "C++" {

namespace bssl {

BORINGSSL_MAKE_DELETER(DH, DH_free)

}  // namespace bssl

}  // extern C++

#endif

#define DH_R_BAD_GENERATOR 100
#define DH_R_INVALID_PUBKEY 101
#define DH_R_MODULUS_TOO_LARGE 102
#define DH_R_NO_PRIVATE_VALUE 103
#define DH_R_DECODE_ERROR 104
#define DH_R_ENCODE_ERROR 105

#endif  // OPENSSL_HEADER_DH_H
