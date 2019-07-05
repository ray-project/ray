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

#include <assert.h>

#include "internal.h"
#include "../bn/internal.h"
#include "../../internal.h"


int ec_bignum_to_felem(const EC_GROUP *group, EC_FELEM *out, const BIGNUM *in) {
  if (BN_is_negative(in) || BN_cmp(in, &group->field) >= 0) {
    OPENSSL_PUT_ERROR(EC, EC_R_COORDINATES_OUT_OF_RANGE);
    return 0;
  }
  return group->meth->bignum_to_felem(group, out, in);
}

int ec_felem_to_bignum(const EC_GROUP *group, BIGNUM *out, const EC_FELEM *in) {
  return group->meth->felem_to_bignum(group, out, in);
}

void ec_felem_neg(const EC_GROUP *group, EC_FELEM *out, const EC_FELEM *a) {
  // -a is zero if a is zero and p-a otherwise.
  BN_ULONG mask = ec_felem_non_zero_mask(group, a);
  BN_ULONG borrow =
      bn_sub_words(out->words, group->field.d, a->words, group->field.width);
  assert(borrow == 0);
  (void)borrow;
  for (int i = 0; i < group->field.width; i++) {
    out->words[i] &= mask;
  }
}

void ec_felem_add(const EC_GROUP *group, EC_FELEM *out, const EC_FELEM *a,
                  const EC_FELEM *b) {
  EC_FELEM tmp;
  bn_mod_add_words(out->words, a->words, b->words, group->field.d, tmp.words,
                   group->field.width);
}

void ec_felem_sub(const EC_GROUP *group, EC_FELEM *out, const EC_FELEM *a,
                  const EC_FELEM *b) {
  EC_FELEM tmp;
  bn_mod_sub_words(out->words, a->words, b->words, group->field.d, tmp.words,
                   group->field.width);
}

BN_ULONG ec_felem_non_zero_mask(const EC_GROUP *group, const EC_FELEM *a) {
  BN_ULONG mask = 0;
  for (int i = 0; i < group->field.width; i++) {
    mask |= a->words[i];
  }
  return ~constant_time_is_zero_w(mask);
}

void ec_felem_select(const EC_GROUP *group, EC_FELEM *out, BN_ULONG mask,
                     const EC_FELEM *a, const EC_FELEM *b) {
  bn_select_words(out->words, mask, a->words, b->words, group->field.width);
}

int ec_felem_equal(const EC_GROUP *group, const EC_FELEM *a,
                   const EC_FELEM *b) {
  // Note this function is variable-time. Constant-time operations should use
  // |ec_felem_non_zero_mask|.
  return OPENSSL_memcmp(a->words, b->words,
                        group->field.width * sizeof(BN_ULONG)) == 0;
}
