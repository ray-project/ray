/* Originally written by Bodo Moeller and Nils Larsch for the OpenSSL project.
 * ====================================================================
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */
/* ====================================================================
 * Copyright 2002 Sun Microsystems, Inc. ALL RIGHTS RESERVED.
 *
 * Portions of the attached software ("Contribution") are developed by
 * SUN MICROSYSTEMS, INC., and are contributed to the OpenSSL project.
 *
 * The Contribution is licensed pursuant to the OpenSSL open source
 * license provided above.
 *
 * The elliptic curve binary polynomial software is originally written by
 * Sheueling Chang Shantz and Douglas Stebila of Sun Microsystems
 * Laboratories. */

#include <openssl/ec.h>

#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/mem.h>

#include "../bn/internal.h"
#include "../delocate.h"
#include "internal.h"


int ec_GFp_mont_group_init(EC_GROUP *group) {
  int ok;

  ok = ec_GFp_simple_group_init(group);
  group->mont = NULL;
  return ok;
}

void ec_GFp_mont_group_finish(EC_GROUP *group) {
  BN_MONT_CTX_free(group->mont);
  group->mont = NULL;
  ec_GFp_simple_group_finish(group);
}

int ec_GFp_mont_group_set_curve(EC_GROUP *group, const BIGNUM *p,
                                const BIGNUM *a, const BIGNUM *b, BN_CTX *ctx) {
  BN_CTX *new_ctx = NULL;
  int ret = 0;

  BN_MONT_CTX_free(group->mont);
  group->mont = NULL;

  if (ctx == NULL) {
    ctx = new_ctx = BN_CTX_new();
    if (ctx == NULL) {
      return 0;
    }
  }

  group->mont = BN_MONT_CTX_new_for_modulus(p, ctx);
  if (group->mont == NULL) {
    OPENSSL_PUT_ERROR(EC, ERR_R_BN_LIB);
    goto err;
  }

  ret = ec_GFp_simple_group_set_curve(group, p, a, b, ctx);

  if (!ret) {
    BN_MONT_CTX_free(group->mont);
    group->mont = NULL;
  }

err:
  BN_CTX_free(new_ctx);
  return ret;
}

static void ec_GFp_mont_felem_to_montgomery(const EC_GROUP *group,
                                            EC_FELEM *out, const EC_FELEM *in) {
  bn_to_montgomery_small(out->words, in->words, group->field.width,
                         group->mont);
}

static void ec_GFp_mont_felem_from_montgomery(const EC_GROUP *group,
                                              EC_FELEM *out,
                                              const EC_FELEM *in) {
  bn_from_montgomery_small(out->words, in->words, group->field.width,
                           group->mont);
}

static void ec_GFp_mont_felem_inv(const EC_GROUP *group, EC_FELEM *out,
                                  const EC_FELEM *a) {
  bn_mod_inverse_prime_mont_small(out->words, a->words, group->field.width,
                                  group->mont);
}

void ec_GFp_mont_felem_mul(const EC_GROUP *group, EC_FELEM *r,
                           const EC_FELEM *a, const EC_FELEM *b) {
  bn_mod_mul_montgomery_small(r->words, a->words, b->words, group->field.width,
                              group->mont);
}

void ec_GFp_mont_felem_sqr(const EC_GROUP *group, EC_FELEM *r,
                           const EC_FELEM *a) {
  bn_mod_mul_montgomery_small(r->words, a->words, a->words, group->field.width,
                              group->mont);
}

int ec_GFp_mont_bignum_to_felem(const EC_GROUP *group, EC_FELEM *out,
                                const BIGNUM *in) {
  if (group->mont == NULL) {
    OPENSSL_PUT_ERROR(EC, EC_R_NOT_INITIALIZED);
    return 0;
  }

  if (!bn_copy_words(out->words, group->field.width, in)) {
    return 0;
  }
  ec_GFp_mont_felem_to_montgomery(group, out, out);
  return 1;
}

int ec_GFp_mont_felem_to_bignum(const EC_GROUP *group, BIGNUM *out,
                                const EC_FELEM *in) {
  if (group->mont == NULL) {
    OPENSSL_PUT_ERROR(EC, EC_R_NOT_INITIALIZED);
    return 0;
  }

  EC_FELEM tmp;
  ec_GFp_mont_felem_from_montgomery(group, &tmp, in);
  return bn_set_words(out, tmp.words, group->field.width);
}

static int ec_GFp_mont_point_get_affine_coordinates(const EC_GROUP *group,
                                                    const EC_RAW_POINT *point,
                                                    BIGNUM *x, BIGNUM *y) {
  if (ec_GFp_simple_is_at_infinity(group, point)) {
    OPENSSL_PUT_ERROR(EC, EC_R_POINT_AT_INFINITY);
    return 0;
  }

  // Transform  (X, Y, Z)  into  (x, y) := (X/Z^2, Y/Z^3).

  EC_FELEM z1, z2;
  ec_GFp_mont_felem_inv(group, &z2, &point->Z);
  ec_GFp_mont_felem_sqr(group, &z1, &z2);

  // Instead of using |ec_GFp_mont_felem_from_montgomery| to convert the |x|
  // coordinate and then calling |ec_GFp_mont_felem_from_montgomery| again to
  // convert the |y| coordinate below, convert the common factor |z1| once now,
  // saving one reduction.
  ec_GFp_mont_felem_from_montgomery(group, &z1, &z1);

  if (x != NULL) {
    EC_FELEM tmp;
    ec_GFp_mont_felem_mul(group, &tmp, &point->X, &z1);
    if (!bn_set_words(x, tmp.words, group->field.width)) {
      return 0;
    }
  }

  if (y != NULL) {
    EC_FELEM tmp;
    ec_GFp_mont_felem_mul(group, &z1, &z1, &z2);
    ec_GFp_mont_felem_mul(group, &tmp, &point->Y, &z1);
    if (!bn_set_words(y, tmp.words, group->field.width)) {
      return 0;
    }
  }

  return 1;
}

DEFINE_METHOD_FUNCTION(EC_METHOD, EC_GFp_mont_method) {
  out->group_init = ec_GFp_mont_group_init;
  out->group_finish = ec_GFp_mont_group_finish;
  out->group_set_curve = ec_GFp_mont_group_set_curve;
  out->point_get_affine_coordinates = ec_GFp_mont_point_get_affine_coordinates;
  out->mul = ec_GFp_simple_mul;
  out->mul_public = ec_GFp_simple_mul_public;
  out->felem_mul = ec_GFp_mont_felem_mul;
  out->felem_sqr = ec_GFp_mont_felem_sqr;
  out->bignum_to_felem = ec_GFp_mont_bignum_to_felem;
  out->felem_to_bignum = ec_GFp_mont_felem_to_bignum;
  out->scalar_inv_montgomery = ec_simple_scalar_inv_montgomery;
}
