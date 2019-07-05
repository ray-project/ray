/* ====================================================================
 * Copyright 2002 Sun Microsystems, Inc. ALL RIGHTS RESERVED.
 *
 * The Elliptic Curve Public-Key Crypto Library (ECC Code) included
 * herein is developed by SUN MICROSYSTEMS, INC., and is contributed
 * to the OpenSSL project.
 *
 * The ECC Code is licensed pursuant to the OpenSSL open source
 * license provided below.
 *
 * The ECDH software is originally written by Douglas Stebila of
 * Sun Microsystems Laboratories.
 *
 */
/* ====================================================================
 * Copyright (c) 2000-2002 The OpenSSL Project.  All rights reserved.
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
 *    licensing@OpenSSL.org.
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

#include <openssl/ecdh.h>

#include <limits.h>
#include <string.h>

#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/ec_key.h>
#include <openssl/err.h>
#include <openssl/mem.h>
#include <openssl/sha.h>

#include "../ec/internal.h"


int ECDH_compute_key_fips(uint8_t *out, size_t out_len, const EC_POINT *pub_key,
                          const EC_KEY *priv_key) {
  if (priv_key->priv_key == NULL) {
    OPENSSL_PUT_ERROR(ECDH, ECDH_R_NO_PRIVATE_VALUE);
    return 0;
  }
  const EC_SCALAR *const priv = &priv_key->priv_key->scalar;

  BN_CTX *ctx = BN_CTX_new();
  if (ctx == NULL) {
    return 0;
  }
  BN_CTX_start(ctx);

  int ret = 0;
  size_t buflen = 0;
  uint8_t *buf = NULL;

  const EC_GROUP *const group = EC_KEY_get0_group(priv_key);
  EC_POINT *shared_point = EC_POINT_new(group);
  if (shared_point == NULL) {
    OPENSSL_PUT_ERROR(ECDH, ERR_R_MALLOC_FAILURE);
    goto err;
  }

  if (!ec_point_mul_scalar(group, shared_point, NULL, pub_key, priv, ctx)) {
    OPENSSL_PUT_ERROR(ECDH, ECDH_R_POINT_ARITHMETIC_FAILURE);
    goto err;
  }

  BIGNUM *x = BN_CTX_get(ctx);
  if (!x) {
    OPENSSL_PUT_ERROR(ECDH, ERR_R_MALLOC_FAILURE);
    goto err;
  }

  if (!EC_POINT_get_affine_coordinates_GFp(group, shared_point, x, NULL, ctx)) {
    OPENSSL_PUT_ERROR(ECDH, ECDH_R_POINT_ARITHMETIC_FAILURE);
    goto err;
  }

  buflen = (EC_GROUP_get_degree(group) + 7) / 8;
  buf = OPENSSL_malloc(buflen);
  if (buf == NULL) {
    OPENSSL_PUT_ERROR(ECDH, ERR_R_MALLOC_FAILURE);
    goto err;
  }

  if (!BN_bn2bin_padded(buf, buflen, x)) {
    OPENSSL_PUT_ERROR(ECDH, ERR_R_INTERNAL_ERROR);
    goto err;
  }

  switch (out_len) {
    case SHA224_DIGEST_LENGTH:
      SHA224(buf, buflen, out);
      break;
    case SHA256_DIGEST_LENGTH:
      SHA256(buf, buflen, out);
      break;
    case SHA384_DIGEST_LENGTH:
      SHA384(buf, buflen, out);
      break;
    case SHA512_DIGEST_LENGTH:
      SHA512(buf, buflen, out);
      break;
    default:
      OPENSSL_PUT_ERROR(ECDH, ECDH_R_UNKNOWN_DIGEST_LENGTH);
      goto err;
  }

  ret = 1;

err:
  OPENSSL_free(buf);
  EC_POINT_free(shared_point);
  BN_CTX_end(ctx);
  BN_CTX_free(ctx);
  return ret;
}
