/*
 * Copyright 2014-2016 The OpenSSL Project Authors. All Rights Reserved.
 * Copyright (c) 2014, Intel Corporation. All Rights Reserved.
 *
 * Licensed under the OpenSSL license (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 *
 * Originally written by Shay Gueron (1, 2), and Vlad Krasnov (1)
 * (1) Intel Corporation, Israel Development Center, Haifa, Israel
 * (2) University of Haifa, Israel
 *
 * Reference:
 * S.Gueron and V.Krasnov, "Fast Prime Field Elliptic Curve Cryptography with
 *                          256 Bit Primes"
 */

#include <openssl/ec.h>

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/err.h>

#include "../bn/internal.h"
#include "../delocate.h"
#include "../../internal.h"
#include "internal.h"
#include "p256-x86_64.h"


#if !defined(OPENSSL_NO_ASM) && defined(OPENSSL_X86_64) && \
    !defined(OPENSSL_SMALL)

typedef P256_POINT_AFFINE PRECOMP256_ROW[64];

// One converted into the Montgomery domain
static const BN_ULONG ONE[P256_LIMBS] = {
    TOBN(0x00000000, 0x00000001), TOBN(0xffffffff, 0x00000000),
    TOBN(0xffffffff, 0xffffffff), TOBN(0x00000000, 0xfffffffe),
};

// Precomputed tables for the default generator
#include "p256-x86_64-table.h"

// Recode window to a signed digit, see util-64.c for details
static unsigned booth_recode_w5(unsigned in) {
  unsigned s, d;

  s = ~((in >> 5) - 1);
  d = (1 << 6) - in - 1;
  d = (d & s) | (in & ~s);
  d = (d >> 1) + (d & 1);

  return (d << 1) + (s & 1);
}

static unsigned booth_recode_w7(unsigned in) {
  unsigned s, d;

  s = ~((in >> 7) - 1);
  d = (1 << 8) - in - 1;
  d = (d & s) | (in & ~s);
  d = (d >> 1) + (d & 1);

  return (d << 1) + (s & 1);
}

// copy_conditional copies |src| to |dst| if |move| is one and leaves it as-is
// if |move| is zero.
//
// WARNING: this breaks the usual convention of constant-time functions
// returning masks.
static void copy_conditional(BN_ULONG dst[P256_LIMBS],
                             const BN_ULONG src[P256_LIMBS], BN_ULONG move) {
  BN_ULONG mask1 = ((BN_ULONG)0) - move;
  BN_ULONG mask2 = ~mask1;

  dst[0] = (src[0] & mask1) ^ (dst[0] & mask2);
  dst[1] = (src[1] & mask1) ^ (dst[1] & mask2);
  dst[2] = (src[2] & mask1) ^ (dst[2] & mask2);
  dst[3] = (src[3] & mask1) ^ (dst[3] & mask2);
  if (P256_LIMBS == 8) {
    dst[4] = (src[4] & mask1) ^ (dst[4] & mask2);
    dst[5] = (src[5] & mask1) ^ (dst[5] & mask2);
    dst[6] = (src[6] & mask1) ^ (dst[6] & mask2);
    dst[7] = (src[7] & mask1) ^ (dst[7] & mask2);
  }
}

// is_not_zero returns one iff in != 0 and zero otherwise.
//
// WARNING: this breaks the usual convention of constant-time functions
// returning masks.
//
// (define-fun is_not_zero ((in (_ BitVec 64))) (_ BitVec 64)
//   (bvlshr (bvor in (bvsub #x0000000000000000 in)) #x000000000000003f)
// )
//
// (declare-fun x () (_ BitVec 64))
//
// (assert (and (= x #x0000000000000000) (= (is_not_zero x) #x0000000000000001)))
// (check-sat)
//
// (assert (and (not (= x #x0000000000000000)) (= (is_not_zero x) #x0000000000000000)))
// (check-sat)
//
static BN_ULONG is_not_zero(BN_ULONG in) {
  in |= (0 - in);
  in >>= BN_BITS2 - 1;
  return in;
}

// ecp_nistz256_mod_inverse_mont sets |r| to (|in| * 2^-256)^-1 * 2^256 mod p.
// That is, |r| is the modular inverse of |in| for input and output in the
// Montgomery domain.
static void ecp_nistz256_mod_inverse_mont(BN_ULONG r[P256_LIMBS],
                                          const BN_ULONG in[P256_LIMBS]) {
  /* The poly is ffffffff 00000001 00000000 00000000 00000000 ffffffff ffffffff
     ffffffff
     We use FLT and used poly-2 as exponent */
  BN_ULONG p2[P256_LIMBS];
  BN_ULONG p4[P256_LIMBS];
  BN_ULONG p8[P256_LIMBS];
  BN_ULONG p16[P256_LIMBS];
  BN_ULONG p32[P256_LIMBS];
  BN_ULONG res[P256_LIMBS];
  int i;

  ecp_nistz256_sqr_mont(res, in);
  ecp_nistz256_mul_mont(p2, res, in);  // 3*p

  ecp_nistz256_sqr_mont(res, p2);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_mul_mont(p4, res, p2);  // f*p

  ecp_nistz256_sqr_mont(res, p4);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_mul_mont(p8, res, p4);  // ff*p

  ecp_nistz256_sqr_mont(res, p8);
  for (i = 0; i < 7; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(p16, res, p8);  // ffff*p

  ecp_nistz256_sqr_mont(res, p16);
  for (i = 0; i < 15; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(p32, res, p16);  // ffffffff*p

  ecp_nistz256_sqr_mont(res, p32);
  for (i = 0; i < 31; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(res, res, in);

  for (i = 0; i < 32 * 4; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(res, res, p32);

  for (i = 0; i < 32; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(res, res, p32);

  for (i = 0; i < 16; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(res, res, p16);

  for (i = 0; i < 8; i++) {
    ecp_nistz256_sqr_mont(res, res);
  }
  ecp_nistz256_mul_mont(res, res, p8);

  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_mul_mont(res, res, p4);

  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_mul_mont(res, res, p2);

  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_sqr_mont(res, res);
  ecp_nistz256_mul_mont(r, res, in);
}

// r = p * p_scalar
static void ecp_nistz256_windowed_mul(const EC_GROUP *group, P256_POINT *r,
                                      const EC_RAW_POINT *p,
                                      const EC_SCALAR *p_scalar) {
  assert(p != NULL);
  assert(p_scalar != NULL);
  assert(group->field.width == P256_LIMBS);

  static const unsigned kWindowSize = 5;
  static const unsigned kMask = (1 << (5 /* kWindowSize */ + 1)) - 1;

  // A |P256_POINT| is (3 * 32) = 96 bytes, and the 64-byte alignment should
  // add no more than 63 bytes of overhead. Thus, |table| should require
  // ~1599 ((96 * 16) + 63) bytes of stack space.
  alignas(64) P256_POINT table[16];
  uint8_t p_str[33];
  OPENSSL_memcpy(p_str, p_scalar->bytes, 32);
  p_str[32] = 0;

  // table[0] is implicitly (0,0,0) (the point at infinity), therefore it is
  // not stored. All other values are actually stored with an offset of -1 in
  // table.
  P256_POINT *row = table;
  assert(group->field.width == P256_LIMBS);
  OPENSSL_memcpy(row[1 - 1].X, p->X.words, P256_LIMBS * sizeof(BN_ULONG));
  OPENSSL_memcpy(row[1 - 1].Y, p->Y.words, P256_LIMBS * sizeof(BN_ULONG));
  OPENSSL_memcpy(row[1 - 1].Z, p->Z.words, P256_LIMBS * sizeof(BN_ULONG));

  ecp_nistz256_point_double(&row[2 - 1], &row[1 - 1]);
  ecp_nistz256_point_add(&row[3 - 1], &row[2 - 1], &row[1 - 1]);
  ecp_nistz256_point_double(&row[4 - 1], &row[2 - 1]);
  ecp_nistz256_point_double(&row[6 - 1], &row[3 - 1]);
  ecp_nistz256_point_double(&row[8 - 1], &row[4 - 1]);
  ecp_nistz256_point_double(&row[12 - 1], &row[6 - 1]);
  ecp_nistz256_point_add(&row[5 - 1], &row[4 - 1], &row[1 - 1]);
  ecp_nistz256_point_add(&row[7 - 1], &row[6 - 1], &row[1 - 1]);
  ecp_nistz256_point_add(&row[9 - 1], &row[8 - 1], &row[1 - 1]);
  ecp_nistz256_point_add(&row[13 - 1], &row[12 - 1], &row[1 - 1]);
  ecp_nistz256_point_double(&row[14 - 1], &row[7 - 1]);
  ecp_nistz256_point_double(&row[10 - 1], &row[5 - 1]);
  ecp_nistz256_point_add(&row[15 - 1], &row[14 - 1], &row[1 - 1]);
  ecp_nistz256_point_add(&row[11 - 1], &row[10 - 1], &row[1 - 1]);
  ecp_nistz256_point_double(&row[16 - 1], &row[8 - 1]);

  BN_ULONG tmp[P256_LIMBS];
  alignas(32) P256_POINT h;
  unsigned index = 255;
  unsigned wvalue = p_str[(index - 1) / 8];
  wvalue = (wvalue >> ((index - 1) % 8)) & kMask;

  ecp_nistz256_select_w5(r, table, booth_recode_w5(wvalue) >> 1);

  while (index >= 5) {
    if (index != 255) {
      unsigned off = (index - 1) / 8;

      wvalue = p_str[off] | p_str[off + 1] << 8;
      wvalue = (wvalue >> ((index - 1) % 8)) & kMask;

      wvalue = booth_recode_w5(wvalue);

      ecp_nistz256_select_w5(&h, table, wvalue >> 1);

      ecp_nistz256_neg(tmp, h.Y);
      copy_conditional(h.Y, tmp, (wvalue & 1));

      ecp_nistz256_point_add(r, r, &h);
    }

    index -= kWindowSize;

    ecp_nistz256_point_double(r, r);
    ecp_nistz256_point_double(r, r);
    ecp_nistz256_point_double(r, r);
    ecp_nistz256_point_double(r, r);
    ecp_nistz256_point_double(r, r);
  }

  // Final window
  wvalue = p_str[0];
  wvalue = (wvalue << 1) & kMask;

  wvalue = booth_recode_w5(wvalue);

  ecp_nistz256_select_w5(&h, table, wvalue >> 1);

  ecp_nistz256_neg(tmp, h.Y);
  copy_conditional(h.Y, tmp, wvalue & 1);

  ecp_nistz256_point_add(r, r, &h);
}

static void ecp_nistz256_points_mul(const EC_GROUP *group, EC_RAW_POINT *r,
                                    const EC_SCALAR *g_scalar,
                                    const EC_RAW_POINT *p_,
                                    const EC_SCALAR *p_scalar) {
  assert((p_ != NULL) == (p_scalar != NULL));

  static const unsigned kWindowSize = 7;
  static const unsigned kMask = (1 << (7 /* kWindowSize */ + 1)) - 1;

  alignas(32) union {
    P256_POINT p;
    P256_POINT_AFFINE a;
  } t, p;

  if (g_scalar != NULL) {
    uint8_t p_str[33];
    OPENSSL_memcpy(p_str, g_scalar->bytes, 32);
    p_str[32] = 0;

    // First window
    unsigned wvalue = (p_str[0] << 1) & kMask;
    unsigned index = kWindowSize;

    wvalue = booth_recode_w7(wvalue);

    const PRECOMP256_ROW *const precomputed_table =
        (const PRECOMP256_ROW *)ecp_nistz256_precomputed;
    ecp_nistz256_select_w7(&p.a, precomputed_table[0], wvalue >> 1);

    ecp_nistz256_neg(p.p.Z, p.p.Y);
    copy_conditional(p.p.Y, p.p.Z, wvalue & 1);

    // Convert |p| from affine to Jacobian coordinates. We set Z to zero if |p|
    // is infinity and |ONE| otherwise. |p| was computed from the table, so it
    // is infinity iff |wvalue >> 1| is zero.
    OPENSSL_memset(p.p.Z, 0, sizeof(p.p.Z));
    copy_conditional(p.p.Z, ONE, is_not_zero(wvalue >> 1));

    for (int i = 1; i < 37; i++) {
      unsigned off = (index - 1) / 8;
      wvalue = p_str[off] | p_str[off + 1] << 8;
      wvalue = (wvalue >> ((index - 1) % 8)) & kMask;
      index += kWindowSize;

      wvalue = booth_recode_w7(wvalue);

      ecp_nistz256_select_w7(&t.a, precomputed_table[i], wvalue >> 1);

      ecp_nistz256_neg(t.p.Z, t.a.Y);
      copy_conditional(t.a.Y, t.p.Z, wvalue & 1);

      ecp_nistz256_point_add_affine(&p.p, &p.p, &t.a);
    }
  }

  const int p_is_infinity = g_scalar == NULL;
  if (p_scalar != NULL) {
    P256_POINT *out = &t.p;
    if (p_is_infinity) {
      out = &p.p;
    }

    ecp_nistz256_windowed_mul(group, out, p_, p_scalar);
    if (!p_is_infinity) {
      ecp_nistz256_point_add(&p.p, &p.p, out);
    }
  }

  assert(group->field.width == P256_LIMBS);
  OPENSSL_memcpy(r->X.words, p.p.X, P256_LIMBS * sizeof(BN_ULONG));
  OPENSSL_memcpy(r->Y.words, p.p.Y, P256_LIMBS * sizeof(BN_ULONG));
  OPENSSL_memcpy(r->Z.words, p.p.Z, P256_LIMBS * sizeof(BN_ULONG));
}

static int ecp_nistz256_get_affine(const EC_GROUP *group,
                                   const EC_RAW_POINT *point, BIGNUM *x,
                                   BIGNUM *y) {
  if (ec_GFp_simple_is_at_infinity(group, point)) {
    OPENSSL_PUT_ERROR(EC, EC_R_POINT_AT_INFINITY);
    return 0;
  }

  BN_ULONG z_inv2[P256_LIMBS];
  BN_ULONG z_inv3[P256_LIMBS];
  assert(group->field.width == P256_LIMBS);
  ecp_nistz256_mod_inverse_mont(z_inv3, point->Z.words);
  ecp_nistz256_sqr_mont(z_inv2, z_inv3);

  // Instead of using |ecp_nistz256_from_mont| to convert the |x| coordinate
  // and then calling |ecp_nistz256_from_mont| again to convert the |y|
  // coordinate below, convert the common factor |z_inv2| once now, saving one
  // reduction.
  ecp_nistz256_from_mont(z_inv2, z_inv2);

  if (x != NULL) {
    BN_ULONG x_aff[P256_LIMBS];
    ecp_nistz256_mul_mont(x_aff, z_inv2, point->X.words);
    if (!bn_set_words(x, x_aff, P256_LIMBS)) {
      OPENSSL_PUT_ERROR(EC, ERR_R_MALLOC_FAILURE);
      return 0;
    }
  }

  if (y != NULL) {
    BN_ULONG y_aff[P256_LIMBS];
    ecp_nistz256_mul_mont(z_inv3, z_inv3, z_inv2);
    ecp_nistz256_mul_mont(y_aff, z_inv3, point->Y.words);
    if (!bn_set_words(y, y_aff, P256_LIMBS)) {
      OPENSSL_PUT_ERROR(EC, ERR_R_MALLOC_FAILURE);
      return 0;
    }
  }

  return 1;
}

static void ecp_nistz256_inv_mod_ord(const EC_GROUP *group, EC_SCALAR *out,
                                     const EC_SCALAR *in) {
  // table[i] stores a power of |in| corresponding to the matching enum value.
  enum {
    // The following indices specify the power in binary.
    i_1 = 0,
    i_10,
    i_11,
    i_101,
    i_111,
    i_1010,
    i_1111,
    i_10101,
    i_101010,
    i_101111,
    // The following indices specify 2^N-1, or N ones in a row.
    i_x6,
    i_x8,
    i_x16,
    i_x32
  };
  BN_ULONG table[15][P256_LIMBS];

  // https://briansmith.org/ecc-inversion-addition-chains-01#p256_scalar_inversion
  //
  // Even though this code path spares 12 squarings, 4.5%, and 13
  // multiplications, 25%, the overall sign operation is not that much faster,
  // not more that 2%. Most of the performance of this function comes from the
  // scalar operations.

  // Pre-calculate powers.
  OPENSSL_memcpy(table[i_1], in->words, P256_LIMBS * sizeof(BN_ULONG));

  ecp_nistz256_ord_sqr_mont(table[i_10], table[i_1], 1);

  ecp_nistz256_ord_mul_mont(table[i_11], table[i_1], table[i_10]);

  ecp_nistz256_ord_mul_mont(table[i_101], table[i_11], table[i_10]);

  ecp_nistz256_ord_mul_mont(table[i_111], table[i_101], table[i_10]);

  ecp_nistz256_ord_sqr_mont(table[i_1010], table[i_101], 1);

  ecp_nistz256_ord_mul_mont(table[i_1111], table[i_1010], table[i_101]);

  ecp_nistz256_ord_sqr_mont(table[i_10101], table[i_1010], 1);
  ecp_nistz256_ord_mul_mont(table[i_10101], table[i_10101], table[i_1]);

  ecp_nistz256_ord_sqr_mont(table[i_101010], table[i_10101], 1);

  ecp_nistz256_ord_mul_mont(table[i_101111], table[i_101010], table[i_101]);

  ecp_nistz256_ord_mul_mont(table[i_x6], table[i_101010], table[i_10101]);

  ecp_nistz256_ord_sqr_mont(table[i_x8], table[i_x6], 2);
  ecp_nistz256_ord_mul_mont(table[i_x8], table[i_x8], table[i_11]);

  ecp_nistz256_ord_sqr_mont(table[i_x16], table[i_x8], 8);
  ecp_nistz256_ord_mul_mont(table[i_x16], table[i_x16], table[i_x8]);

  ecp_nistz256_ord_sqr_mont(table[i_x32], table[i_x16], 16);
  ecp_nistz256_ord_mul_mont(table[i_x32], table[i_x32], table[i_x16]);

  // Compute |in| raised to the order-2.
  ecp_nistz256_ord_sqr_mont(out->words, table[i_x32], 64);
  ecp_nistz256_ord_mul_mont(out->words, out->words, table[i_x32]);
  static const struct {
    uint8_t p, i;
  } kChain[27] = {{32, i_x32},    {6, i_101111}, {5, i_111},    {4, i_11},
                  {5, i_1111},    {5, i_10101},  {4, i_101},    {3, i_101},
                  {3, i_101},     {5, i_111},    {9, i_101111}, {6, i_1111},
                  {2, i_1},       {5, i_1},      {6, i_1111},   {5, i_111},
                  {4, i_111},     {5, i_111},    {5, i_101},    {3, i_11},
                  {10, i_101111}, {2, i_11},     {5, i_11},     {5, i_11},
                  {3, i_1},       {7, i_10101},  {6, i_1111}};
  for (size_t i = 0; i < OPENSSL_ARRAY_SIZE(kChain); i++) {
    ecp_nistz256_ord_sqr_mont(out->words, out->words, kChain[i].p);
    ecp_nistz256_ord_mul_mont(out->words, out->words, table[kChain[i].i]);
  }
}

DEFINE_METHOD_FUNCTION(EC_METHOD, EC_GFp_nistz256_method) {
  out->group_init = ec_GFp_mont_group_init;
  out->group_finish = ec_GFp_mont_group_finish;
  out->group_set_curve = ec_GFp_mont_group_set_curve;
  out->point_get_affine_coordinates = ecp_nistz256_get_affine;
  out->mul = ecp_nistz256_points_mul;
  out->mul_public = ecp_nistz256_points_mul;
  out->felem_mul = ec_GFp_mont_felem_mul;
  out->felem_sqr = ec_GFp_mont_felem_sqr;
  out->bignum_to_felem = ec_GFp_mont_bignum_to_felem;
  out->felem_to_bignum = ec_GFp_mont_felem_to_bignum;
  out->scalar_inv_montgomery = ecp_nistz256_inv_mod_ord;
};

#endif /* !defined(OPENSSL_NO_ASM) && defined(OPENSSL_X86_64) && \
          !defined(OPENSSL_SMALL) */
