/* ====================================================================
 * Copyright (c) 1998-2005 The OpenSSL Project.  All rights reserved.
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
 *    for use in the OpenSSL Toolkit. (http://www.OpenSSL.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    openssl-core@OpenSSL.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.OpenSSL.org/)"
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com). */

#include <openssl/ecdsa.h>

#include <assert.h>
#include <string.h>

#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/mem.h>
#include <openssl/sha.h>
#include <openssl/type_check.h>

#include "../bn/internal.h"
#include "../ec/internal.h"
#include "../../internal.h"


// digest_to_scalar interprets |digest_len| bytes from |digest| as a scalar for
// ECDSA. Note this value is not fully reduced modulo the order, only the
// correct number of bits.
static void digest_to_scalar(const EC_GROUP *group, EC_SCALAR *out,
                             const uint8_t *digest, size_t digest_len) {
  const BIGNUM *order = &group->order;
  size_t num_bits = BN_num_bits(order);
  // Need to truncate digest if it is too long: first truncate whole bytes.
  size_t num_bytes = (num_bits + 7) / 8;
  if (digest_len > num_bytes) {
    digest_len = num_bytes;
  }
  OPENSSL_memset(out, 0, sizeof(EC_SCALAR));
  for (size_t i = 0; i < digest_len; i++) {
    out->bytes[i] = digest[digest_len - 1 - i];
  }

  // If it is still too long, truncate remaining bits with a shift.
  if (8 * digest_len > num_bits) {
    bn_rshift_words(out->words, out->words, 8 - (num_bits & 0x7), order->width);
  }

  // |out| now has the same bit width as |order|, but this only bounds by
  // 2*|order|. Subtract the order if out of range.
  //
  // Montgomery multiplication accepts the looser bounds, so this isn't strictly
  // necessary, but it is a cleaner abstraction and has no performance impact.
  BN_ULONG tmp[EC_MAX_SCALAR_WORDS];
  bn_reduce_once_in_place(out->words, 0 /* no carry */, order->d, tmp,
                          order->width);
}

// field_element_to_scalar reduces |r| modulo |group->order|. |r| must
// previously have been reduced modulo |group->field|.
static int field_element_to_scalar(const EC_GROUP *group, BIGNUM *r) {
  // We must have p < 2×order, assuming p is not tiny (p >= 17). Thus rather we
  // can reduce by performing at most one subtraction.
  //
  // Proof: We only work with prime order curves, so the number of points on
  // the curve is the order. Thus Hasse's theorem gives:
  //
  //     |order - (p + 1)| <= 2×sqrt(p)
  //         p + 1 - order <= 2×sqrt(p)
  //     p + 1 - 2×sqrt(p) <= order
  //       p + 1 - 2×(p/4)  < order       (p/4 > sqrt(p) for p >= 17)
  //         p/2 < p/2 + 1  < order
  //                     p  < 2×order
  //
  // Additionally, one can manually check this property for built-in curves. It
  // is enforced for legacy custom curves in |EC_GROUP_set_generator|.
  //
  // TODO(davidben): Introduce |EC_FIELD_ELEMENT|, make this a function from
  // |EC_FIELD_ELEMENT| to |EC_SCALAR|, and cut out the |BIGNUM|. Does this need
  // to be constant-time for signing? |r| is the x-coordinate for kG, which is
  // public unless k was rerolled because |s| was zero.
  assert(!BN_is_negative(r));
  assert(BN_cmp(r, &group->field) < 0);
  if (BN_cmp(r, &group->order) >= 0 &&
      !BN_sub(r, r, &group->order)) {
    return 0;
  }
  assert(!BN_is_negative(r));
  assert(BN_cmp(r, &group->order) < 0);
  return 1;
}

ECDSA_SIG *ECDSA_SIG_new(void) {
  ECDSA_SIG *sig = OPENSSL_malloc(sizeof(ECDSA_SIG));
  if (sig == NULL) {
    return NULL;
  }
  sig->r = BN_new();
  sig->s = BN_new();
  if (sig->r == NULL || sig->s == NULL) {
    ECDSA_SIG_free(sig);
    return NULL;
  }
  return sig;
}

void ECDSA_SIG_free(ECDSA_SIG *sig) {
  if (sig == NULL) {
    return;
  }

  BN_free(sig->r);
  BN_free(sig->s);
  OPENSSL_free(sig);
}

void ECDSA_SIG_get0(const ECDSA_SIG *sig, const BIGNUM **out_r,
                    const BIGNUM **out_s) {
  if (out_r != NULL) {
    *out_r = sig->r;
  }
  if (out_s != NULL) {
    *out_s = sig->s;
  }
}

int ECDSA_SIG_set0(ECDSA_SIG *sig, BIGNUM *r, BIGNUM *s) {
  if (r == NULL || s == NULL) {
    return 0;
  }
  BN_free(sig->r);
  BN_free(sig->s);
  sig->r = r;
  sig->s = s;
  return 1;
}

int ECDSA_do_verify(const uint8_t *digest, size_t digest_len,
                    const ECDSA_SIG *sig, const EC_KEY *eckey) {
  const EC_GROUP *group = EC_KEY_get0_group(eckey);
  const EC_POINT *pub_key = EC_KEY_get0_public_key(eckey);
  if (group == NULL || pub_key == NULL || sig == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ECDSA_R_MISSING_PARAMETERS);
    return 0;
  }

  BN_CTX *ctx = BN_CTX_new();
  if (!ctx) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_MALLOC_FAILURE);
    return 0;
  }
  int ret = 0;
  EC_POINT *point = NULL;
  BN_CTX_start(ctx);
  BIGNUM *X = BN_CTX_get(ctx);
  if (X == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_BN_LIB);
    goto err;
  }

  EC_SCALAR r, s, u1, u2, s_inv_mont, m;
  if (BN_is_zero(sig->r) ||
      !ec_bignum_to_scalar(group, &r, sig->r) ||
      BN_is_zero(sig->s) ||
      !ec_bignum_to_scalar(group, &s, sig->s)) {
    OPENSSL_PUT_ERROR(ECDSA, ECDSA_R_BAD_SIGNATURE);
    goto err;
  }

  // s_inv_mont = s^-1 in the Montgomery domain. This is
  // |ec_scalar_to_montgomery| followed by |ec_scalar_inv_montgomery|, but
  // |ec_scalar_inv_montgomery| followed by |ec_scalar_from_montgomery| is
  // equivalent and slightly more efficient.
  ec_scalar_inv_montgomery(group, &s_inv_mont, &s);
  ec_scalar_from_montgomery(group, &s_inv_mont, &s_inv_mont);

  // u1 = m * s^-1 mod order
  // u2 = r * s^-1 mod order
  //
  // |s_inv_mont| is in Montgomery form while |m| and |r| are not, so |u1| and
  // |u2| will be taken out of Montgomery form, as desired.
  digest_to_scalar(group, &m, digest, digest_len);
  ec_scalar_mul_montgomery(group, &u1, &m, &s_inv_mont);
  ec_scalar_mul_montgomery(group, &u2, &r, &s_inv_mont);

  point = EC_POINT_new(group);
  if (point == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_MALLOC_FAILURE);
    goto err;
  }
  if (!ec_point_mul_scalar_public(group, point, &u1, pub_key, &u2, ctx)) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_EC_LIB);
    goto err;
  }
  if (!EC_POINT_get_affine_coordinates_GFp(group, point, X, NULL, ctx)) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_EC_LIB);
    goto err;
  }
  if (!field_element_to_scalar(group, X)) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_BN_LIB);
    goto err;
  }
  // The signature is correct iff |X| is equal to |sig->r|.
  if (BN_ucmp(X, sig->r) != 0) {
    OPENSSL_PUT_ERROR(ECDSA, ECDSA_R_BAD_SIGNATURE);
    goto err;
  }

  ret = 1;

err:
  BN_CTX_end(ctx);
  BN_CTX_free(ctx);
  EC_POINT_free(point);
  return ret;
}

static int ecdsa_sign_setup(const EC_KEY *eckey, BN_CTX *ctx,
                            EC_SCALAR *out_kinv_mont, BIGNUM **rp,
                            const uint8_t *digest, size_t digest_len,
                            const EC_SCALAR *priv_key) {
  EC_POINT *tmp_point = NULL;
  int ret = 0;
  EC_SCALAR k;
  BIGNUM *r = BN_new();  // this value is later returned in *rp
  if (r == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_MALLOC_FAILURE);
    goto err;
  }
  const EC_GROUP *group = EC_KEY_get0_group(eckey);
  const BIGNUM *order = EC_GROUP_get0_order(group);
  tmp_point = EC_POINT_new(group);
  if (tmp_point == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_EC_LIB);
    goto err;
  }

  // Check that the size of the group order is FIPS compliant (FIPS 186-4
  // B.5.2).
  if (BN_num_bits(order) < 160) {
    OPENSSL_PUT_ERROR(ECDSA, EC_R_INVALID_GROUP_ORDER);
    goto err;
  }

  do {
    // Include the private key and message digest in the k generation.
    if (eckey->fixed_k != NULL) {
      if (!ec_bignum_to_scalar(group, &k, eckey->fixed_k)) {
        goto err;
      }
    } else {
      // Pass a SHA512 hash of the private key and digest as additional data
      // into the RBG. This is a hardening measure against entropy failure.
      OPENSSL_COMPILE_ASSERT(SHA512_DIGEST_LENGTH >= 32,
                             additional_data_is_too_large_for_sha512);
      SHA512_CTX sha;
      uint8_t additional_data[SHA512_DIGEST_LENGTH];
      SHA512_Init(&sha);
      SHA512_Update(&sha, priv_key->words, order->width * sizeof(BN_ULONG));
      SHA512_Update(&sha, digest, digest_len);
      SHA512_Final(additional_data, &sha);
      if (!ec_random_nonzero_scalar(group, &k, additional_data)) {
        goto err;
      }
    }

    // Compute k^-1 in the Montgomery domain. This is |ec_scalar_to_montgomery|
    // followed by |ec_scalar_inv_montgomery|, but |ec_scalar_inv_montgomery|
    // followed by |ec_scalar_from_montgomery| is equivalent and slightly more
    // efficient.
    ec_scalar_inv_montgomery(group, out_kinv_mont, &k);
    ec_scalar_from_montgomery(group, out_kinv_mont, out_kinv_mont);

    // Compute r, the x-coordinate of generator * k.
    if (!ec_point_mul_scalar(group, tmp_point, &k, NULL, NULL, ctx) ||
        !EC_POINT_get_affine_coordinates_GFp(group, tmp_point, r, NULL,
                                             ctx)) {
      goto err;
    }

    if (!field_element_to_scalar(group, r)) {
      goto err;
    }
  } while (BN_is_zero(r));

  BN_clear_free(*rp);
  *rp = r;
  r = NULL;
  ret = 1;

err:
  OPENSSL_cleanse(&k, sizeof(k));
  BN_clear_free(r);
  EC_POINT_free(tmp_point);
  return ret;
}

ECDSA_SIG *ECDSA_do_sign(const uint8_t *digest, size_t digest_len,
                         const EC_KEY *eckey) {
  if (eckey->ecdsa_meth && eckey->ecdsa_meth->sign) {
    OPENSSL_PUT_ERROR(ECDSA, ECDSA_R_NOT_IMPLEMENTED);
    return NULL;
  }

  const EC_GROUP *group = EC_KEY_get0_group(eckey);
  if (group == NULL || eckey->priv_key == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_PASSED_NULL_PARAMETER);
    return NULL;
  }
  const BIGNUM *order = EC_GROUP_get0_order(group);
  const EC_SCALAR *priv_key = &eckey->priv_key->scalar;

  int ok = 0;
  ECDSA_SIG *ret = ECDSA_SIG_new();
  BN_CTX *ctx = BN_CTX_new();
  EC_SCALAR kinv_mont, r_mont, s, m, tmp;
  if (ret == NULL || ctx == NULL) {
    OPENSSL_PUT_ERROR(ECDSA, ERR_R_MALLOC_FAILURE);
    return NULL;
  }

  digest_to_scalar(group, &m, digest, digest_len);
  for (;;) {
    if (!ecdsa_sign_setup(eckey, ctx, &kinv_mont, &ret->r, digest, digest_len,
                          priv_key)) {
      goto err;
    }

    // Compute priv_key * r (mod order). Note if only one parameter is in the
    // Montgomery domain, |scalar_mod_mul_montgomery| will compute the answer in
    // the normal domain.
    if (!ec_bignum_to_scalar(group, &r_mont, ret->r)) {
      goto err;
    }
    ec_scalar_to_montgomery(group, &r_mont, &r_mont);
    ec_scalar_mul_montgomery(group, &s, priv_key, &r_mont);

    // Compute tmp = m + priv_key * r.
    ec_scalar_add(group, &tmp, &m, &s);

    // Finally, multiply s by k^-1. That was retained in Montgomery form, so the
    // same technique as the previous multiplication works.
    ec_scalar_mul_montgomery(group, &s, &tmp, &kinv_mont);
    if (!bn_set_words(ret->s, s.words, order->width)) {
      goto err;
    }
    if (!BN_is_zero(ret->s)) {
      // s != 0 => we have a valid signature
      break;
    }
  }

  ok = 1;

err:
  if (!ok) {
    ECDSA_SIG_free(ret);
    ret = NULL;
  }
  BN_CTX_free(ctx);
  OPENSSL_cleanse(&kinv_mont, sizeof(kinv_mont));
  OPENSSL_cleanse(&r_mont, sizeof(r_mont));
  OPENSSL_cleanse(&s, sizeof(s));
  OPENSSL_cleanse(&tmp, sizeof(tmp));
  OPENSSL_cleanse(&m, sizeof(m));
  return ret;
}
