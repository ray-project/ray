/* Copyright (c) 2018, Google Inc.
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

#include <openssl/ec.h>

#include <assert.h>

#include "internal.h"
#include "../bn/internal.h"
#include "../../internal.h"


static void ec_GFp_simple_mul_single(const EC_GROUP *group, EC_RAW_POINT *r,
                                     const EC_RAW_POINT *p,
                                     const EC_SCALAR *scalar) {
  // This is a generic implementation for uncommon curves that not do not
  // warrant a tuned one. It uses unsigned digits so that the doubling case in
  // |ec_GFp_simple_add| is always unreachable, erring on safety and simplicity.

  // Compute a table of the first 32 multiples of |p| (including infinity).
  EC_RAW_POINT precomp[32];
  ec_GFp_simple_point_set_to_infinity(group, &precomp[0]);
  ec_GFp_simple_point_copy(&precomp[1], p);
  for (size_t j = 2; j < OPENSSL_ARRAY_SIZE(precomp); j++) {
    if (j & 1) {
      ec_GFp_simple_add(group, &precomp[j], &precomp[1], &precomp[j - 1]);
    } else {
      ec_GFp_simple_dbl(group, &precomp[j], &precomp[j / 2]);
    }
  }

  // Divide bits in |scalar| into windows.
  unsigned bits = BN_num_bits(&group->order);
  int r_is_at_infinity = 1;
  for (unsigned i = bits - 1; i < bits; i--) {
    if (!r_is_at_infinity) {
      ec_GFp_simple_dbl(group, r, r);
    }
    if (i % 5 == 0) {
      // Compute the next window value.
      const size_t width = group->order.width;
      uint8_t window = bn_is_bit_set_words(scalar->words, width, i + 4) << 4;
      window |= bn_is_bit_set_words(scalar->words, width, i + 3) << 3;
      window |= bn_is_bit_set_words(scalar->words, width, i + 2) << 2;
      window |= bn_is_bit_set_words(scalar->words, width, i + 1) << 1;
      window |= bn_is_bit_set_words(scalar->words, width, i);

      // Select the entry in constant-time.
      EC_RAW_POINT tmp;
      OPENSSL_memset(&tmp, 0, sizeof(EC_RAW_POINT));
      for (size_t j = 0; j < OPENSSL_ARRAY_SIZE(precomp); j++) {
        BN_ULONG mask = constant_time_eq_w(j, window);
        ec_felem_select(group, &tmp.X, mask, &precomp[j].X, &tmp.X);
        ec_felem_select(group, &tmp.Y, mask, &precomp[j].Y, &tmp.Y);
        ec_felem_select(group, &tmp.Z, mask, &precomp[j].Z, &tmp.Z);
      }

      if (r_is_at_infinity) {
        ec_GFp_simple_point_copy(r, &tmp);
        r_is_at_infinity = 0;
      } else {
        ec_GFp_simple_add(group, r, r, &tmp);
      }
    }
  }
  if (r_is_at_infinity) {
    ec_GFp_simple_point_set_to_infinity(group, r);
  }
}

void ec_GFp_simple_mul(const EC_GROUP *group, EC_RAW_POINT *r,
                       const EC_SCALAR *g_scalar, const EC_RAW_POINT *p,
                       const EC_SCALAR *p_scalar) {
  assert(g_scalar != NULL || p_scalar != NULL);
  if (p_scalar == NULL) {
    ec_GFp_simple_mul_single(group, r, &group->generator->raw, g_scalar);
  } else if (g_scalar == NULL) {
    ec_GFp_simple_mul_single(group, r, p, p_scalar);
  } else {
    // Support constant-time two-point multiplication for compatibility.  This
    // does not actually come up in keygen, ECDH, or ECDSA, so we implement it
    // the naive way.
    ec_GFp_simple_mul_single(group, r, &group->generator->raw, g_scalar);
    EC_RAW_POINT tmp;
    ec_GFp_simple_mul_single(group, &tmp, p, p_scalar);
    ec_GFp_simple_add(group, r, r, &tmp);
  }
}
