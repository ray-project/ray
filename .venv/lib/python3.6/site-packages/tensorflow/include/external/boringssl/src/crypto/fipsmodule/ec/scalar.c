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
#include <openssl/err.h>
#include <openssl/mem.h>

#include "internal.h"
#include "../bn/internal.h"


int ec_bignum_to_scalar(const EC_GROUP *group, EC_SCALAR *out,
                        const BIGNUM *in) {
  if (!bn_copy_words(out->words, group->order.width, in) ||
      !bn_less_than_words(out->words, group->order.d, group->order.width)) {
    OPENSSL_PUT_ERROR(EC, EC_R_INVALID_SCALAR);
    return 0;
  }
  return 1;
}

int ec_random_nonzero_scalar(const EC_GROUP *group, EC_SCALAR *out,
                             const uint8_t additional_data[32]) {
  return bn_rand_range_words(out->words, 1, group->order.d, group->order.width,
                             additional_data);
}

void ec_scalar_add(const EC_GROUP *group, EC_SCALAR *r, const EC_SCALAR *a,
                   const EC_SCALAR *b) {
  const BIGNUM *order = &group->order;
  BN_ULONG tmp[EC_MAX_SCALAR_WORDS];
  bn_mod_add_words(r->words, a->words, b->words, order->d, tmp, order->width);
  OPENSSL_cleanse(tmp, sizeof(tmp));
}

void ec_scalar_to_montgomery(const EC_GROUP *group, EC_SCALAR *r,
                             const EC_SCALAR *a) {
  const BIGNUM *order = &group->order;
  bn_to_montgomery_small(r->words, a->words, order->width, group->order_mont);
}

void ec_scalar_from_montgomery(const EC_GROUP *group, EC_SCALAR *r,
                               const EC_SCALAR *a) {
  const BIGNUM *order = &group->order;
  bn_from_montgomery_small(r->words, a->words, order->width, group->order_mont);
}

void ec_scalar_mul_montgomery(const EC_GROUP *group, EC_SCALAR *r,
                              const EC_SCALAR *a, const EC_SCALAR *b) {
  const BIGNUM *order = &group->order;
  bn_mod_mul_montgomery_small(r->words, a->words, b->words, order->width,
                              group->order_mont);
}

void ec_simple_scalar_inv_montgomery(const EC_GROUP *group, EC_SCALAR *r,
                                     const EC_SCALAR *a) {
  const BIGNUM *order = &group->order;
  bn_mod_inverse_prime_mont_small(r->words, a->words, order->width,
                                  group->order_mont);
}

void ec_scalar_inv_montgomery(const EC_GROUP *group, EC_SCALAR *r,
                              const EC_SCALAR *a) {
  group->meth->scalar_inv_montgomery(group, r, a);
}
