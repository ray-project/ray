/* Originally written by Bodo Moeller for the OpenSSL project.
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

#include <openssl/ec_key.h>

#include <string.h>

#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/ex_data.h>
#include <openssl/mem.h>
#include <openssl/thread.h>

#include "internal.h"
#include "../delocate.h"
#include "../../internal.h"


DEFINE_STATIC_EX_DATA_CLASS(g_ec_ex_data_class);

static EC_WRAPPED_SCALAR *ec_wrapped_scalar_new(const EC_GROUP *group) {
  EC_WRAPPED_SCALAR *wrapped = OPENSSL_malloc(sizeof(EC_WRAPPED_SCALAR));
  if (wrapped == NULL) {
    OPENSSL_PUT_ERROR(EC, ERR_R_MALLOC_FAILURE);
    return NULL;
  }

  OPENSSL_memset(wrapped, 0, sizeof(EC_WRAPPED_SCALAR));
  wrapped->bignum.d = wrapped->scalar.words;
  wrapped->bignum.width = group->order.width;
  wrapped->bignum.dmax = group->order.width;
  wrapped->bignum.flags = BN_FLG_STATIC_DATA;
  return wrapped;
}

static void ec_wrapped_scalar_free(EC_WRAPPED_SCALAR *scalar) {
  OPENSSL_free(scalar);
}

EC_KEY *EC_KEY_new(void) { return EC_KEY_new_method(NULL); }

EC_KEY *EC_KEY_new_method(const ENGINE *engine) {
  EC_KEY *ret = OPENSSL_malloc(sizeof(EC_KEY));
  if (ret == NULL) {
    OPENSSL_PUT_ERROR(EC, ERR_R_MALLOC_FAILURE);
    return NULL;
  }

  OPENSSL_memset(ret, 0, sizeof(EC_KEY));

  if (engine) {
    ret->ecdsa_meth = ENGINE_get_ECDSA_method(engine);
  }
  if (ret->ecdsa_meth) {
    METHOD_ref(ret->ecdsa_meth);
  }

  ret->conv_form = POINT_CONVERSION_UNCOMPRESSED;
  ret->references = 1;

  CRYPTO_new_ex_data(&ret->ex_data);

  if (ret->ecdsa_meth && ret->ecdsa_meth->init && !ret->ecdsa_meth->init(ret)) {
    CRYPTO_free_ex_data(g_ec_ex_data_class_bss_get(), ret, &ret->ex_data);
    if (ret->ecdsa_meth) {
      METHOD_unref(ret->ecdsa_meth);
    }
    OPENSSL_free(ret);
    return NULL;
  }

  return ret;
}

EC_KEY *EC_KEY_new_by_curve_name(int nid) {
  EC_KEY *ret = EC_KEY_new();
  if (ret == NULL) {
    OPENSSL_PUT_ERROR(EC, ERR_R_MALLOC_FAILURE);
    return NULL;
  }
  ret->group = EC_GROUP_new_by_curve_name(nid);
  if (ret->group == NULL) {
    EC_KEY_free(ret);
    return NULL;
  }
  return ret;
}

void EC_KEY_free(EC_KEY *r) {
  if (r == NULL) {
    return;
  }

  if (!CRYPTO_refcount_dec_and_test_zero(&r->references)) {
    return;
  }

  if (r->ecdsa_meth) {
    if (r->ecdsa_meth->finish) {
      r->ecdsa_meth->finish(r);
    }
    METHOD_unref(r->ecdsa_meth);
  }

  EC_GROUP_free(r->group);
  EC_POINT_free(r->pub_key);
  ec_wrapped_scalar_free(r->priv_key);
  BN_free(r->fixed_k);

  CRYPTO_free_ex_data(g_ec_ex_data_class_bss_get(), r, &r->ex_data);

  OPENSSL_free(r);
}

EC_KEY *EC_KEY_dup(const EC_KEY *src) {
  if (src == NULL) {
    OPENSSL_PUT_ERROR(EC, ERR_R_PASSED_NULL_PARAMETER);
    return NULL;
  }

  EC_KEY *ret = EC_KEY_new();
  if (ret == NULL) {
    return NULL;
  }

  if ((src->group != NULL &&
       !EC_KEY_set_group(ret, src->group)) ||
      (src->pub_key != NULL &&
       !EC_KEY_set_public_key(ret, src->pub_key)) ||
      (src->priv_key != NULL &&
       !EC_KEY_set_private_key(ret, EC_KEY_get0_private_key(src)))) {
    EC_KEY_free(ret);
    return NULL;
  }

  ret->enc_flag = src->enc_flag;
  ret->conv_form = src->conv_form;
  return ret;
}

int EC_KEY_up_ref(EC_KEY *r) {
  CRYPTO_refcount_inc(&r->references);
  return 1;
}

int EC_KEY_is_opaque(const EC_KEY *key) {
  return key->ecdsa_meth && (key->ecdsa_meth->flags & ECDSA_FLAG_OPAQUE);
}

const EC_GROUP *EC_KEY_get0_group(const EC_KEY *key) { return key->group; }

int EC_KEY_set_group(EC_KEY *key, const EC_GROUP *group) {
  // If |key| already has a group, it is an error to switch to another one.
  if (key->group != NULL) {
    if (EC_GROUP_cmp(key->group, group, NULL) != 0) {
      OPENSSL_PUT_ERROR(EC, EC_R_GROUP_MISMATCH);
      return 0;
    }
    return 1;
  }

  assert(key->priv_key == NULL);
  assert(key->pub_key == NULL);

  EC_GROUP_free(key->group);
  key->group = EC_GROUP_dup(group);
  return key->group != NULL;
}

const BIGNUM *EC_KEY_get0_private_key(const EC_KEY *key) {
  return key->priv_key != NULL ? &key->priv_key->bignum : NULL;
}

int EC_KEY_set_private_key(EC_KEY *key, const BIGNUM *priv_key) {
  if (key->group == NULL) {
    OPENSSL_PUT_ERROR(EC, EC_R_MISSING_PARAMETERS);
    return 0;
  }

  EC_WRAPPED_SCALAR *scalar = ec_wrapped_scalar_new(key->group);
  if (scalar == NULL) {
    return 0;
  }
  if (!ec_bignum_to_scalar(key->group, &scalar->scalar, priv_key)) {
    OPENSSL_PUT_ERROR(EC, EC_R_WRONG_ORDER);
    ec_wrapped_scalar_free(scalar);
    return 0;
  }
  ec_wrapped_scalar_free(key->priv_key);
  key->priv_key = scalar;
  return 1;
}

const EC_POINT *EC_KEY_get0_public_key(const EC_KEY *key) {
  return key->pub_key;
}

int EC_KEY_set_public_key(EC_KEY *key, const EC_POINT *pub_key) {
  if (key->group == NULL) {
    OPENSSL_PUT_ERROR(EC, EC_R_MISSING_PARAMETERS);
    return 0;
  }

  if (EC_GROUP_cmp(key->group, pub_key->group, NULL) != 0) {
    OPENSSL_PUT_ERROR(EC, EC_R_GROUP_MISMATCH);
    return 0;
  }

  EC_POINT_free(key->pub_key);
  key->pub_key = EC_POINT_dup(pub_key, key->group);
  return (key->pub_key == NULL) ? 0 : 1;
}

unsigned int EC_KEY_get_enc_flags(const EC_KEY *key) { return key->enc_flag; }

void EC_KEY_set_enc_flags(EC_KEY *key, unsigned int flags) {
  key->enc_flag = flags;
}

point_conversion_form_t EC_KEY_get_conv_form(const EC_KEY *key) {
  return key->conv_form;
}

void EC_KEY_set_conv_form(EC_KEY *key, point_conversion_form_t cform) {
  key->conv_form = cform;
}

int EC_KEY_check_key(const EC_KEY *eckey) {
  int ok = 0;
  BN_CTX *ctx = NULL;
  EC_POINT *point = NULL;

  if (!eckey || !eckey->group || !eckey->pub_key) {
    OPENSSL_PUT_ERROR(EC, ERR_R_PASSED_NULL_PARAMETER);
    return 0;
  }

  if (EC_POINT_is_at_infinity(eckey->group, eckey->pub_key)) {
    OPENSSL_PUT_ERROR(EC, EC_R_POINT_AT_INFINITY);
    goto err;
  }

  ctx = BN_CTX_new();

  if (ctx == NULL) {
    goto err;
  }

  // testing whether the pub_key is on the elliptic curve
  if (!EC_POINT_is_on_curve(eckey->group, eckey->pub_key, ctx)) {
    OPENSSL_PUT_ERROR(EC, EC_R_POINT_IS_NOT_ON_CURVE);
    goto err;
  }
  // in case the priv_key is present :
  // check if generator * priv_key == pub_key
  if (eckey->priv_key != NULL) {
    point = EC_POINT_new(eckey->group);
    if (point == NULL ||
        !ec_point_mul_scalar(eckey->group, point, &eckey->priv_key->scalar,
                             NULL, NULL, ctx)) {
      OPENSSL_PUT_ERROR(EC, ERR_R_EC_LIB);
      goto err;
    }
    if (EC_POINT_cmp(eckey->group, point, eckey->pub_key, ctx) != 0) {
      OPENSSL_PUT_ERROR(EC, EC_R_INVALID_PRIVATE_KEY);
      goto err;
    }
  }
  ok = 1;

err:
  BN_CTX_free(ctx);
  EC_POINT_free(point);
  return ok;
}

int EC_KEY_check_fips(const EC_KEY *key) {
  if (EC_KEY_is_opaque(key)) {
    // Opaque keys can't be checked.
    OPENSSL_PUT_ERROR(EC, EC_R_PUBLIC_KEY_VALIDATION_FAILED);
    return 0;
  }

  if (!EC_KEY_check_key(key)) {
    return 0;
  }

  if (key->priv_key) {
    uint8_t data[16] = {0};
    ECDSA_SIG *sig = ECDSA_do_sign(data, sizeof(data), key);
#if defined(BORINGSSL_FIPS_BREAK_ECDSA_PWCT)
    data[0] = ~data[0];
#endif
    int ok = sig != NULL &&
             ECDSA_do_verify(data, sizeof(data), sig, key);
    ECDSA_SIG_free(sig);
    if (!ok) {
      OPENSSL_PUT_ERROR(EC, EC_R_PUBLIC_KEY_VALIDATION_FAILED);
      return 0;
    }
  }

  return 1;
}

int EC_KEY_set_public_key_affine_coordinates(EC_KEY *key, BIGNUM *x,
                                             BIGNUM *y) {
  EC_POINT *point = NULL;
  int ok = 0;

  if (!key || !key->group || !x || !y) {
    OPENSSL_PUT_ERROR(EC, ERR_R_PASSED_NULL_PARAMETER);
    return 0;
  }

  point = EC_POINT_new(key->group);
  if (point == NULL ||
      !EC_POINT_set_affine_coordinates_GFp(key->group, point, x, y, NULL) ||
      !EC_KEY_set_public_key(key, point) ||
      !EC_KEY_check_key(key)) {
    goto err;
  }

  ok = 1;

err:
  EC_POINT_free(point);
  return ok;
}

int EC_KEY_generate_key(EC_KEY *key) {
  if (key == NULL || key->group == NULL) {
    OPENSSL_PUT_ERROR(EC, ERR_R_PASSED_NULL_PARAMETER);
    return 0;
  }

  // Check that the group order is FIPS compliant (FIPS 186-4 B.4.2).
  if (BN_num_bits(EC_GROUP_get0_order(key->group)) < 160) {
    OPENSSL_PUT_ERROR(EC, EC_R_INVALID_GROUP_ORDER);
    return 0;
  }

  static const uint8_t kDefaultAdditionalData[32] = {0};
  EC_WRAPPED_SCALAR *priv_key = ec_wrapped_scalar_new(key->group);
  EC_POINT *pub_key = EC_POINT_new(key->group);
  if (priv_key == NULL || pub_key == NULL ||
      // Generate the private key by testing candidates (FIPS 186-4 B.4.2).
      !ec_random_nonzero_scalar(key->group, &priv_key->scalar,
                                kDefaultAdditionalData) ||
      !ec_point_mul_scalar(key->group, pub_key, &priv_key->scalar, NULL, NULL,
                           NULL)) {
    EC_POINT_free(pub_key);
    ec_wrapped_scalar_free(priv_key);
    return 0;
  }

  ec_wrapped_scalar_free(key->priv_key);
  key->priv_key = priv_key;
  EC_POINT_free(key->pub_key);
  key->pub_key = pub_key;
  return 1;
}

int EC_KEY_generate_key_fips(EC_KEY *eckey) {
  return EC_KEY_generate_key(eckey) && EC_KEY_check_fips(eckey);
}

int EC_KEY_get_ex_new_index(long argl, void *argp, CRYPTO_EX_unused *unused,
                            CRYPTO_EX_dup *dup_unused,
                            CRYPTO_EX_free *free_func) {
  int index;
  if (!CRYPTO_get_ex_new_index(g_ec_ex_data_class_bss_get(), &index, argl, argp,
                               free_func)) {
    return -1;
  }
  return index;
}

int EC_KEY_set_ex_data(EC_KEY *d, int idx, void *arg) {
  return CRYPTO_set_ex_data(&d->ex_data, idx, arg);
}

void *EC_KEY_get_ex_data(const EC_KEY *d, int idx) {
  return CRYPTO_get_ex_data(&d->ex_data, idx);
}

void EC_KEY_set_asn1_flag(EC_KEY *key, int flag) {}
