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
 * [including the GNU Public Licence.]
 */
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
 * Hudson (tjh@cryptsoft.com). */

#include <openssl/bn.h>

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <openssl/cpu.h>
#include <openssl/err.h>
#include <openssl/mem.h>

#include "internal.h"


#if !defined(OPENSSL_NO_ASM) && defined(OPENSSL_X86_64)
#define OPENSSL_BN_ASM_MONT5
#define RSAZ_ENABLED

#include "rsaz_exp.h"

void bn_mul_mont_gather5(BN_ULONG *rp, const BN_ULONG *ap, const void *table,
                         const BN_ULONG *np, const BN_ULONG *n0, int num,
                         int power);
void bn_scatter5(const BN_ULONG *inp, size_t num, void *table, size_t power);
void bn_gather5(BN_ULONG *out, size_t num, void *table, size_t power);
void bn_power5(BN_ULONG *rp, const BN_ULONG *ap, const void *table,
               const BN_ULONG *np, const BN_ULONG *n0, int num, int power);
int bn_from_montgomery(BN_ULONG *rp, const BN_ULONG *ap,
                       const BN_ULONG *not_used, const BN_ULONG *np,
                       const BN_ULONG *n0, int num);
#endif

int BN_exp(BIGNUM *r, const BIGNUM *a, const BIGNUM *p, BN_CTX *ctx) {
  int i, bits, ret = 0;
  BIGNUM *v, *rr;

  BN_CTX_start(ctx);
  if (r == a || r == p) {
    rr = BN_CTX_get(ctx);
  } else {
    rr = r;
  }

  v = BN_CTX_get(ctx);
  if (rr == NULL || v == NULL) {
    goto err;
  }

  if (BN_copy(v, a) == NULL) {
    goto err;
  }
  bits = BN_num_bits(p);

  if (BN_is_odd(p)) {
    if (BN_copy(rr, a) == NULL) {
      goto err;
    }
  } else {
    if (!BN_one(rr)) {
      goto err;
    }
  }

  for (i = 1; i < bits; i++) {
    if (!BN_sqr(v, v, ctx)) {
      goto err;
    }
    if (BN_is_bit_set(p, i)) {
      if (!BN_mul(rr, rr, v, ctx)) {
        goto err;
      }
    }
  }

  if (r != rr && !BN_copy(r, rr)) {
    goto err;
  }
  ret = 1;

err:
  BN_CTX_end(ctx);
  return ret;
}

typedef struct bn_recp_ctx_st {
  BIGNUM N;   // the divisor
  BIGNUM Nr;  // the reciprocal
  int num_bits;
  int shift;
  int flags;
} BN_RECP_CTX;

static void BN_RECP_CTX_init(BN_RECP_CTX *recp) {
  BN_init(&recp->N);
  BN_init(&recp->Nr);
  recp->num_bits = 0;
  recp->shift = 0;
  recp->flags = 0;
}

static void BN_RECP_CTX_free(BN_RECP_CTX *recp) {
  if (recp == NULL) {
    return;
  }

  BN_free(&recp->N);
  BN_free(&recp->Nr);
}

static int BN_RECP_CTX_set(BN_RECP_CTX *recp, const BIGNUM *d, BN_CTX *ctx) {
  if (!BN_copy(&(recp->N), d)) {
    return 0;
  }
  BN_zero(&recp->Nr);
  recp->num_bits = BN_num_bits(d);
  recp->shift = 0;

  return 1;
}

// len is the expected size of the result We actually calculate with an extra
// word of precision, so we can do faster division if the remainder is not
// required.
// r := 2^len / m
static int BN_reciprocal(BIGNUM *r, const BIGNUM *m, int len, BN_CTX *ctx) {
  int ret = -1;
  BIGNUM *t;

  BN_CTX_start(ctx);
  t = BN_CTX_get(ctx);
  if (t == NULL) {
    goto err;
  }

  if (!BN_set_bit(t, len)) {
    goto err;
  }

  if (!BN_div(r, NULL, t, m, ctx)) {
    goto err;
  }

  ret = len;

err:
  BN_CTX_end(ctx);
  return ret;
}

static int BN_div_recp(BIGNUM *dv, BIGNUM *rem, const BIGNUM *m,
                       BN_RECP_CTX *recp, BN_CTX *ctx) {
  int i, j, ret = 0;
  BIGNUM *a, *b, *d, *r;

  BN_CTX_start(ctx);
  a = BN_CTX_get(ctx);
  b = BN_CTX_get(ctx);
  if (dv != NULL) {
    d = dv;
  } else {
    d = BN_CTX_get(ctx);
  }

  if (rem != NULL) {
    r = rem;
  } else {
    r = BN_CTX_get(ctx);
  }

  if (a == NULL || b == NULL || d == NULL || r == NULL) {
    goto err;
  }

  if (BN_ucmp(m, &recp->N) < 0) {
    BN_zero(d);
    if (!BN_copy(r, m)) {
      goto err;
    }
    BN_CTX_end(ctx);
    return 1;
  }

  // We want the remainder
  // Given input of ABCDEF / ab
  // we need multiply ABCDEF by 3 digests of the reciprocal of ab

  // i := max(BN_num_bits(m), 2*BN_num_bits(N))
  i = BN_num_bits(m);
  j = recp->num_bits << 1;
  if (j > i) {
    i = j;
  }

  // Nr := round(2^i / N)
  if (i != recp->shift) {
    recp->shift =
        BN_reciprocal(&(recp->Nr), &(recp->N), i,
                      ctx);  // BN_reciprocal returns i, or -1 for an error
  }

  if (recp->shift == -1) {
    goto err;
  }

  // d := |round(round(m / 2^BN_num_bits(N)) * recp->Nr / 2^(i -
  // BN_num_bits(N)))|
  //    = |round(round(m / 2^BN_num_bits(N)) * round(2^i / N) / 2^(i -
  // BN_num_bits(N)))|
  //   <= |(m / 2^BN_num_bits(N)) * (2^i / N) * (2^BN_num_bits(N) / 2^i)|
  //    = |m/N|
  if (!BN_rshift(a, m, recp->num_bits)) {
    goto err;
  }
  if (!BN_mul(b, a, &(recp->Nr), ctx)) {
    goto err;
  }
  if (!BN_rshift(d, b, i - recp->num_bits)) {
    goto err;
  }
  d->neg = 0;

  if (!BN_mul(b, &(recp->N), d, ctx)) {
    goto err;
  }
  if (!BN_usub(r, m, b)) {
    goto err;
  }
  r->neg = 0;

  j = 0;
  while (BN_ucmp(r, &(recp->N)) >= 0) {
    if (j++ > 2) {
      OPENSSL_PUT_ERROR(BN, BN_R_BAD_RECIPROCAL);
      goto err;
    }
    if (!BN_usub(r, r, &(recp->N))) {
      goto err;
    }
    if (!BN_add_word(d, 1)) {
      goto err;
    }
  }

  r->neg = BN_is_zero(r) ? 0 : m->neg;
  d->neg = m->neg ^ recp->N.neg;
  ret = 1;

err:
  BN_CTX_end(ctx);
  return ret;
}

static int BN_mod_mul_reciprocal(BIGNUM *r, const BIGNUM *x, const BIGNUM *y,
                                 BN_RECP_CTX *recp, BN_CTX *ctx) {
  int ret = 0;
  BIGNUM *a;
  const BIGNUM *ca;

  BN_CTX_start(ctx);
  a = BN_CTX_get(ctx);
  if (a == NULL) {
    goto err;
  }

  if (y != NULL) {
    if (x == y) {
      if (!BN_sqr(a, x, ctx)) {
        goto err;
      }
    } else {
      if (!BN_mul(a, x, y, ctx)) {
        goto err;
      }
    }
    ca = a;
  } else {
    ca = x;  // Just do the mod
  }

  ret = BN_div_recp(NULL, r, ca, recp, ctx);

err:
  BN_CTX_end(ctx);
  return ret;
}

// BN_window_bits_for_exponent_size returns sliding window size for mod_exp with
// a |b| bit exponent.
//
// For window size 'w' (w >= 2) and a random 'b' bits exponent, the number of
// multiplications is a constant plus on average
//
//    2^(w-1) + (b-w)/(w+1);
//
// here 2^(w-1)  is for precomputing the table (we actually need entries only
// for windows that have the lowest bit set), and (b-w)/(w+1)  is an
// approximation for the expected number of w-bit windows, not counting the
// first one.
//
// Thus we should use
//
//    w >= 6  if        b > 671
//     w = 5  if  671 > b > 239
//     w = 4  if  239 > b >  79
//     w = 3  if   79 > b >  23
//    w <= 2  if   23 > b
//
// (with draws in between).  Very small exponents are often selected
// with low Hamming weight, so we use  w = 1  for b <= 23.
static int BN_window_bits_for_exponent_size(int b) {
  if (b > 671) {
    return 6;
  }
  if (b > 239) {
    return 5;
  }
  if (b > 79) {
    return 4;
  }
  if (b > 23) {
    return 3;
  }
  return 1;
}

// TABLE_SIZE is the maximum precomputation table size for *variable* sliding
// windows. This must be 2^(max_window - 1), where max_window is the largest
// value returned from |BN_window_bits_for_exponent_size|.
#define TABLE_SIZE 32

// TABLE_BITS_SMALL is the smallest value returned from
// |BN_window_bits_for_exponent_size| when |b| is at most |BN_BITS2| *
// |BN_SMALL_MAX_WORDS| words.
#define TABLE_BITS_SMALL 5

// TABLE_SIZE_SMALL is the same as |TABLE_SIZE|, but when |b| is at most
// |BN_BITS2| * |BN_SMALL_MAX_WORDS|.
#define TABLE_SIZE_SMALL (1 << (TABLE_BITS_SMALL - 1))

static int mod_exp_recp(BIGNUM *r, const BIGNUM *a, const BIGNUM *p,
                        const BIGNUM *m, BN_CTX *ctx) {
  int i, j, bits, ret = 0, wstart, window;
  int start = 1;
  BIGNUM *aa;
  // Table of variables obtained from 'ctx'
  BIGNUM *val[TABLE_SIZE];
  BN_RECP_CTX recp;

  bits = BN_num_bits(p);

  if (bits == 0) {
    // x**0 mod 1 is still zero.
    if (BN_is_one(m)) {
      BN_zero(r);
      return 1;
    }
    return BN_one(r);
  }

  BN_CTX_start(ctx);
  aa = BN_CTX_get(ctx);
  val[0] = BN_CTX_get(ctx);
  if (!aa || !val[0]) {
    goto err;
  }

  BN_RECP_CTX_init(&recp);
  if (m->neg) {
    // ignore sign of 'm'
    if (!BN_copy(aa, m)) {
      goto err;
    }
    aa->neg = 0;
    if (BN_RECP_CTX_set(&recp, aa, ctx) <= 0) {
      goto err;
    }
  } else {
    if (BN_RECP_CTX_set(&recp, m, ctx) <= 0) {
      goto err;
    }
  }

  if (!BN_nnmod(val[0], a, m, ctx)) {
    goto err;  // 1
  }
  if (BN_is_zero(val[0])) {
    BN_zero(r);
    ret = 1;
    goto err;
  }

  window = BN_window_bits_for_exponent_size(bits);
  if (window > 1) {
    if (!BN_mod_mul_reciprocal(aa, val[0], val[0], &recp, ctx)) {
      goto err;  // 2
    }
    j = 1 << (window - 1);
    for (i = 1; i < j; i++) {
      if (((val[i] = BN_CTX_get(ctx)) == NULL) ||
          !BN_mod_mul_reciprocal(val[i], val[i - 1], aa, &recp, ctx)) {
        goto err;
      }
    }
  }

  start = 1;  // This is used to avoid multiplication etc
              // when there is only the value '1' in the
              // buffer.
  wstart = bits - 1;  // The top bit of the window

  if (!BN_one(r)) {
    goto err;
  }

  for (;;) {
    int wvalue;  // The 'value' of the window
    int wend;  // The bottom bit of the window

    if (!BN_is_bit_set(p, wstart)) {
      if (!start) {
        if (!BN_mod_mul_reciprocal(r, r, r, &recp, ctx)) {
          goto err;
        }
      }
      if (wstart == 0) {
        break;
      }
      wstart--;
      continue;
    }

    // We now have wstart on a 'set' bit, we now need to work out
    // how bit a window to do.  To do this we need to scan
    // forward until the last set bit before the end of the
    // window
    wvalue = 1;
    wend = 0;
    for (i = 1; i < window; i++) {
      if (wstart - i < 0) {
        break;
      }
      if (BN_is_bit_set(p, wstart - i)) {
        wvalue <<= (i - wend);
        wvalue |= 1;
        wend = i;
      }
    }

    // wend is the size of the current window
    j = wend + 1;
    // add the 'bytes above'
    if (!start) {
      for (i = 0; i < j; i++) {
        if (!BN_mod_mul_reciprocal(r, r, r, &recp, ctx)) {
          goto err;
        }
      }
    }

    // wvalue will be an odd number < 2^window
    if (!BN_mod_mul_reciprocal(r, r, val[wvalue >> 1], &recp, ctx)) {
      goto err;
    }

    // move the 'window' down further
    wstart -= wend + 1;
    start = 0;
    if (wstart < 0) {
      break;
    }
  }
  ret = 1;

err:
  BN_CTX_end(ctx);
  BN_RECP_CTX_free(&recp);
  return ret;
}

int BN_mod_exp(BIGNUM *r, const BIGNUM *a, const BIGNUM *p, const BIGNUM *m,
               BN_CTX *ctx) {
  if (a->neg || BN_ucmp(a, m) >= 0) {
    if (!BN_nnmod(r, a, m, ctx)) {
      return 0;
    }
    a = r;
  }

  if (BN_is_odd(m)) {
    return BN_mod_exp_mont(r, a, p, m, ctx, NULL);
  }

  return mod_exp_recp(r, a, p, m, ctx);
}

int BN_mod_exp_mont(BIGNUM *rr, const BIGNUM *a, const BIGNUM *p,
                    const BIGNUM *m, BN_CTX *ctx, const BN_MONT_CTX *mont) {
  if (!BN_is_odd(m)) {
    OPENSSL_PUT_ERROR(BN, BN_R_CALLED_WITH_EVEN_MODULUS);
    return 0;
  }
  if (a->neg || BN_ucmp(a, m) >= 0) {
    OPENSSL_PUT_ERROR(BN, BN_R_INPUT_NOT_REDUCED);
    return 0;
  }

  int bits = BN_num_bits(p);
  if (bits == 0) {
    // x**0 mod 1 is still zero.
    if (BN_is_one(m)) {
      BN_zero(rr);
      return 1;
    }
    return BN_one(rr);
  }

  int ret = 0;
  BIGNUM *val[TABLE_SIZE];
  BN_MONT_CTX *new_mont = NULL;

  BN_CTX_start(ctx);
  BIGNUM *d = BN_CTX_get(ctx);
  BIGNUM *r = BN_CTX_get(ctx);
  val[0] = BN_CTX_get(ctx);
  if (!d || !r || !val[0]) {
    goto err;
  }

  // Allocate a montgomery context if it was not supplied by the caller.
  if (mont == NULL) {
    new_mont = BN_MONT_CTX_new_consttime(m, ctx);
    if (new_mont == NULL) {
      goto err;
    }
    mont = new_mont;
  }

  // We exponentiate by looking at sliding windows of the exponent and
  // precomputing powers of |a|. Windows may be shifted so they always end on a
  // set bit, so only precompute odd powers. We compute val[i] = a^(2*i + 1)
  // for i = 0 to 2^(window-1), all in Montgomery form.
  int window = BN_window_bits_for_exponent_size(bits);
  if (!BN_to_montgomery(val[0], a, mont, ctx)) {
    goto err;
  }
  if (window > 1) {
    if (!BN_mod_mul_montgomery(d, val[0], val[0], mont, ctx)) {
      goto err;
    }
    for (int i = 1; i < 1 << (window - 1); i++) {
      val[i] = BN_CTX_get(ctx);
      if (val[i] == NULL ||
          !BN_mod_mul_montgomery(val[i], val[i - 1], d, mont, ctx)) {
        goto err;
      }
    }
  }

  // |p| is non-zero, so at least one window is non-zero. To save some
  // multiplications, defer initializing |r| until then.
  int r_is_one = 1;
  int wstart = bits - 1;  // The top bit of the window.
  for (;;) {
    if (!BN_is_bit_set(p, wstart)) {
      if (!r_is_one && !BN_mod_mul_montgomery(r, r, r, mont, ctx)) {
        goto err;
      }
      if (wstart == 0) {
        break;
      }
      wstart--;
      continue;
    }

    // We now have wstart on a set bit. Find the largest window we can use.
    int wvalue = 1;
    int wsize = 0;
    for (int i = 1; i < window && i <= wstart; i++) {
      if (BN_is_bit_set(p, wstart - i)) {
        wvalue <<= (i - wsize);
        wvalue |= 1;
        wsize = i;
      }
    }

    // Shift |r| to the end of the window.
    if (!r_is_one) {
      for (int i = 0; i < wsize + 1; i++) {
        if (!BN_mod_mul_montgomery(r, r, r, mont, ctx)) {
          goto err;
        }
      }
    }

    assert(wvalue & 1);
    assert(wvalue < (1 << window));
    if (r_is_one) {
      if (!BN_copy(r, val[wvalue >> 1])) {
        goto err;
      }
    } else if (!BN_mod_mul_montgomery(r, r, val[wvalue >> 1], mont, ctx)) {
      goto err;
    }

    r_is_one = 0;
    if (wstart == wsize) {
      break;
    }
    wstart -= wsize + 1;
  }

  // |p| is non-zero, so |r_is_one| must be cleared at some point.
  assert(!r_is_one);

  if (!BN_from_montgomery(rr, r, mont, ctx)) {
    goto err;
  }
  ret = 1;

err:
  BN_MONT_CTX_free(new_mont);
  BN_CTX_end(ctx);
  return ret;
}

void bn_mod_exp_mont_small(BN_ULONG *r, const BN_ULONG *a, size_t num,
                           const BN_ULONG *p, size_t num_p,
                           const BN_MONT_CTX *mont) {
  if (num != (size_t)mont->N.width || num > BN_SMALL_MAX_WORDS) {
    abort();
  }
  assert(BN_is_odd(&mont->N));

  // Count the number of bits in |p|. Note this function treats |p| as public.
  while (num_p != 0 && p[num_p - 1] == 0) {
    num_p--;
  }
  if (num_p == 0) {
    bn_from_montgomery_small(r, mont->RR.d, num, mont);
    return;
  }
  unsigned bits = BN_num_bits_word(p[num_p - 1]) + (num_p - 1) * BN_BITS2;
  assert(bits != 0);

  // We exponentiate by looking at sliding windows of the exponent and
  // precomputing powers of |a|. Windows may be shifted so they always end on a
  // set bit, so only precompute odd powers. We compute val[i] = a^(2*i + 1) for
  // i = 0 to 2^(window-1), all in Montgomery form.
  unsigned window = BN_window_bits_for_exponent_size(bits);
  if (window > TABLE_BITS_SMALL) {
    window = TABLE_BITS_SMALL;  // Tolerate excessively large |p|.
  }
  BN_ULONG val[TABLE_SIZE_SMALL][BN_SMALL_MAX_WORDS];
  OPENSSL_memcpy(val[0], a, num * sizeof(BN_ULONG));
  if (window > 1) {
    BN_ULONG d[BN_SMALL_MAX_WORDS];
    bn_mod_mul_montgomery_small(d, val[0], val[0], num, mont);
    for (unsigned i = 1; i < 1u << (window - 1); i++) {
      bn_mod_mul_montgomery_small(val[i], val[i - 1], d, num, mont);
    }
  }

  // |p| is non-zero, so at least one window is non-zero. To save some
  // multiplications, defer initializing |r| until then.
  int r_is_one = 1;
  unsigned wstart = bits - 1;  // The top bit of the window.
  for (;;) {
    if (!bn_is_bit_set_words(p, num_p, wstart)) {
      if (!r_is_one) {
        bn_mod_mul_montgomery_small(r, r, r, num, mont);
      }
      if (wstart == 0) {
        break;
      }
      wstart--;
      continue;
    }

    // We now have wstart on a set bit. Find the largest window we can use.
    unsigned wvalue = 1;
    unsigned wsize = 0;
    for (unsigned i = 1; i < window && i <= wstart; i++) {
      if (bn_is_bit_set_words(p, num_p, wstart - i)) {
        wvalue <<= (i - wsize);
        wvalue |= 1;
        wsize = i;
      }
    }

    // Shift |r| to the end of the window.
    if (!r_is_one) {
      for (unsigned i = 0; i < wsize + 1; i++) {
        bn_mod_mul_montgomery_small(r, r, r, num, mont);
      }
    }

    assert(wvalue & 1);
    assert(wvalue < (1u << window));
    if (r_is_one) {
      OPENSSL_memcpy(r, val[wvalue >> 1], num * sizeof(BN_ULONG));
    } else {
      bn_mod_mul_montgomery_small(r, r, val[wvalue >> 1], num, mont);
    }
    r_is_one = 0;
    if (wstart == wsize) {
      break;
    }
    wstart -= wsize + 1;
  }

  // |p| is non-zero, so |r_is_one| must be cleared at some point.
  assert(!r_is_one);
  OPENSSL_cleanse(val, sizeof(val));
}

void bn_mod_inverse_prime_mont_small(BN_ULONG *r, const BN_ULONG *a, size_t num,
                                     const BN_MONT_CTX *mont) {
  if (num != (size_t)mont->N.width || num > BN_SMALL_MAX_WORDS) {
    abort();
  }

  // Per Fermat's Little Theorem, a^-1 = a^(p-2) (mod p) for p prime.
  BN_ULONG p_minus_two[BN_SMALL_MAX_WORDS];
  const BN_ULONG *p = mont->N.d;
  OPENSSL_memcpy(p_minus_two, p, num * sizeof(BN_ULONG));
  if (p_minus_two[0] >= 2) {
    p_minus_two[0] -= 2;
  } else {
    p_minus_two[0] -= 2;
    for (size_t i = 1; i < num; i++) {
      if (p_minus_two[i]-- != 0) {
        break;
      }
    }
  }

  bn_mod_exp_mont_small(r, a, num, p_minus_two, num, mont);
}


// |BN_mod_exp_mont_consttime| stores the precomputed powers in a specific
// layout so that accessing any of these table values shows the same access
// pattern as far as cache lines are concerned. The following functions are
// used to transfer a BIGNUM from/to that table.

static void copy_to_prebuf(const BIGNUM *b, int top, unsigned char *buf,
                           int idx, int window) {
  int i, j;
  const int width = 1 << window;
  BN_ULONG *table = (BN_ULONG *) buf;

  if (top > b->width) {
    top = b->width;  // this works because 'buf' is explicitly zeroed
  }

  for (i = 0, j = idx; i < top; i++, j += width)  {
    table[j] = b->d[i];
  }
}

static int copy_from_prebuf(BIGNUM *b, int top, unsigned char *buf, int idx,
                            int window) {
  int i, j;
  const int width = 1 << window;
  volatile BN_ULONG *table = (volatile BN_ULONG *)buf;

  if (!bn_wexpand(b, top)) {
    return 0;
  }

  if (window <= 3) {
    for (i = 0; i < top; i++, table += width) {
      BN_ULONG acc = 0;

      for (j = 0; j < width; j++) {
        acc |= table[j] & ((BN_ULONG)0 - (constant_time_eq_int(j, idx) & 1));
      }

      b->d[i] = acc;
    }
  } else {
    int xstride = 1 << (window - 2);
    BN_ULONG y0, y1, y2, y3;

    i = idx >> (window - 2);  // equivalent of idx / xstride
    idx &= xstride - 1;       // equivalent of idx % xstride

    y0 = (BN_ULONG)0 - (constant_time_eq_int(i, 0) & 1);
    y1 = (BN_ULONG)0 - (constant_time_eq_int(i, 1) & 1);
    y2 = (BN_ULONG)0 - (constant_time_eq_int(i, 2) & 1);
    y3 = (BN_ULONG)0 - (constant_time_eq_int(i, 3) & 1);

    for (i = 0; i < top; i++, table += width) {
      BN_ULONG acc = 0;

      for (j = 0; j < xstride; j++) {
        acc |= ((table[j + 0 * xstride] & y0) | (table[j + 1 * xstride] & y1) |
                (table[j + 2 * xstride] & y2) | (table[j + 3 * xstride] & y3)) &
               ((BN_ULONG)0 - (constant_time_eq_int(j, idx) & 1));
      }

      b->d[i] = acc;
    }
  }

  b->width = top;
  return 1;
}

#define MOD_EXP_CTIME_MIN_CACHE_LINE_MASK \
  (MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH - 1)

// Window sizes optimized for fixed window size modular exponentiation
// algorithm (BN_mod_exp_mont_consttime).
//
// To achieve the security goals of BN_mode_exp_mont_consttime, the maximum
// size of the window must not exceed
// log_2(MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH).
//
// Window size thresholds are defined for cache line sizes of 32 and 64, cache
// line sizes where log_2(32)=5 and log_2(64)=6 respectively. A window size of
// 7 should only be used on processors that have a 128 byte or greater cache
// line size.
#if MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH == 64

#define BN_window_bits_for_ctime_exponent_size(b) \
  ((b) > 937 ? 6 : (b) > 306 ? 5 : (b) > 89 ? 4 : (b) > 22 ? 3 : 1)
#define BN_MAX_WINDOW_BITS_FOR_CTIME_EXPONENT_SIZE (6)

#elif MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH == 32

#define BN_window_bits_for_ctime_exponent_size(b) \
  ((b) > 306 ? 5 : (b) > 89 ? 4 : (b) > 22 ? 3 : 1)
#define BN_MAX_WINDOW_BITS_FOR_CTIME_EXPONENT_SIZE (5)

#endif

// Given a pointer value, compute the next address that is a cache line
// multiple.
#define MOD_EXP_CTIME_ALIGN(x_)          \
  ((unsigned char *)(x_) +               \
   (MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH - \
    (((size_t)(x_)) & (MOD_EXP_CTIME_MIN_CACHE_LINE_MASK))))

// This variant of BN_mod_exp_mont() uses fixed windows and the special
// precomputation memory layout to limit data-dependency to a minimum
// to protect secret exponents (cf. the hyper-threading timing attacks
// pointed out by Colin Percival,
// http://www.daemonology.net/hyperthreading-considered-harmful/)
int BN_mod_exp_mont_consttime(BIGNUM *rr, const BIGNUM *a, const BIGNUM *p,
                              const BIGNUM *m, BN_CTX *ctx,
                              const BN_MONT_CTX *mont) {
  int i, ret = 0, window, wvalue;
  BN_MONT_CTX *new_mont = NULL;

  int numPowers;
  unsigned char *powerbufFree = NULL;
  int powerbufLen = 0;
  unsigned char *powerbuf = NULL;
  BIGNUM tmp, am;

  if (!BN_is_odd(m)) {
    OPENSSL_PUT_ERROR(BN, BN_R_CALLED_WITH_EVEN_MODULUS);
    return 0;
  }
  if (a->neg || BN_ucmp(a, m) >= 0) {
    OPENSSL_PUT_ERROR(BN, BN_R_INPUT_NOT_REDUCED);
    return 0;
  }

  // Use all bits stored in |p|, rather than |BN_num_bits|, so we do not leak
  // whether the top bits are zero.
  int max_bits = p->width * BN_BITS2;
  int bits = max_bits;
  if (bits == 0) {
    // x**0 mod 1 is still zero.
    if (BN_is_one(m)) {
      BN_zero(rr);
      return 1;
    }
    return BN_one(rr);
  }

  // Allocate a montgomery context if it was not supplied by the caller.
  if (mont == NULL) {
    new_mont = BN_MONT_CTX_new_consttime(m, ctx);
    if (new_mont == NULL) {
      goto err;
    }
    mont = new_mont;
  }

  // Use the width in |mont->N|, rather than the copy in |m|. The assembly
  // implementation assumes it can use |top| to size R.
  int top = mont->N.width;

#if defined(OPENSSL_BN_ASM_MONT5) || defined(RSAZ_ENABLED)
  // Share one large stack-allocated buffer between the RSAZ and non-RSAZ code
  // paths. If we were to use separate static buffers for each then there is
  // some chance that both large buffers would be allocated on the stack,
  // causing the stack space requirement to be truly huge (~10KB).
  alignas(MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH) BN_ULONG
    storage[MOD_EXP_CTIME_STORAGE_LEN];
#endif
#ifdef RSAZ_ENABLED
  // If the size of the operands allow it, perform the optimized
  // RSAZ exponentiation. For further information see
  // crypto/bn/rsaz_exp.c and accompanying assembly modules.
  if ((16 == a->width) && (16 == p->width) && (BN_num_bits(m) == 1024) &&
      rsaz_avx2_eligible()) {
    if (!bn_wexpand(rr, 16)) {
      goto err;
    }
    RSAZ_1024_mod_exp_avx2(rr->d, a->d, p->d, m->d, mont->RR.d, mont->n0[0],
                           storage);
    rr->width = 16;
    rr->neg = 0;
    ret = 1;
    goto err;
  }
#endif

  // Get the window size to use with size of p.
  window = BN_window_bits_for_ctime_exponent_size(bits);
#if defined(OPENSSL_BN_ASM_MONT5)
  if (window >= 5) {
    window = 5;  // ~5% improvement for RSA2048 sign, and even for RSA4096
    // reserve space for mont->N.d[] copy
    powerbufLen += top * sizeof(mont->N.d[0]);
  }
#endif

  // Allocate a buffer large enough to hold all of the pre-computed
  // powers of am, am itself and tmp.
  numPowers = 1 << window;
  powerbufLen +=
      sizeof(m->d[0]) *
      (top * numPowers + ((2 * top) > numPowers ? (2 * top) : numPowers));

#if defined(OPENSSL_BN_ASM_MONT5)
  if ((size_t)powerbufLen <= sizeof(storage)) {
    powerbuf = (unsigned char *)storage;
  }
  // |storage| is more than large enough to handle 1024-bit inputs.
  assert(powerbuf != NULL || top * BN_BITS2 > 1024);
#endif
  if (powerbuf == NULL) {
    powerbufFree =
        OPENSSL_malloc(powerbufLen + MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH);
    if (powerbufFree == NULL) {
      goto err;
    }
    powerbuf = MOD_EXP_CTIME_ALIGN(powerbufFree);
  }
  OPENSSL_memset(powerbuf, 0, powerbufLen);

  // lay down tmp and am right after powers table
  tmp.d = (BN_ULONG *)(powerbuf + sizeof(m->d[0]) * top * numPowers);
  am.d = tmp.d + top;
  tmp.width = am.width = 0;
  tmp.dmax = am.dmax = top;
  tmp.neg = am.neg = 0;
  tmp.flags = am.flags = BN_FLG_STATIC_DATA;

  if (!bn_one_to_montgomery(&tmp, mont, ctx)) {
    goto err;
  }

  // prepare a^1 in Montgomery domain
  assert(!a->neg);
  assert(BN_ucmp(a, m) < 0);
  if (!BN_to_montgomery(&am, a, mont, ctx)) {
    goto err;
  }

#if defined(OPENSSL_BN_ASM_MONT5)
  // This optimization uses ideas from http://eprint.iacr.org/2011/239,
  // specifically optimization of cache-timing attack countermeasures
  // and pre-computation optimization.

  // Dedicated window==4 case improves 512-bit RSA sign by ~15%, but as
  // 512-bit RSA is hardly relevant, we omit it to spare size...
  if (window == 5 && top > 1) {
    const BN_ULONG *n0 = mont->n0;
    BN_ULONG *np;

    // BN_to_montgomery can contaminate words above .top
    // [in BN_DEBUG[_DEBUG] build]...
    for (i = am.width; i < top; i++) {
      am.d[i] = 0;
    }
    for (i = tmp.width; i < top; i++) {
      tmp.d[i] = 0;
    }

    // copy mont->N.d[] to improve cache locality
    for (np = am.d + top, i = 0; i < top; i++) {
      np[i] = mont->N.d[i];
    }

    bn_scatter5(tmp.d, top, powerbuf, 0);
    bn_scatter5(am.d, am.width, powerbuf, 1);
    bn_mul_mont(tmp.d, am.d, am.d, np, n0, top);
    bn_scatter5(tmp.d, top, powerbuf, 2);

    // same as above, but uses squaring for 1/2 of operations
    for (i = 4; i < 32; i *= 2) {
      bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
      bn_scatter5(tmp.d, top, powerbuf, i);
    }
    for (i = 3; i < 8; i += 2) {
      int j;
      bn_mul_mont_gather5(tmp.d, am.d, powerbuf, np, n0, top, i - 1);
      bn_scatter5(tmp.d, top, powerbuf, i);
      for (j = 2 * i; j < 32; j *= 2) {
        bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
        bn_scatter5(tmp.d, top, powerbuf, j);
      }
    }
    for (; i < 16; i += 2) {
      bn_mul_mont_gather5(tmp.d, am.d, powerbuf, np, n0, top, i - 1);
      bn_scatter5(tmp.d, top, powerbuf, i);
      bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
      bn_scatter5(tmp.d, top, powerbuf, 2 * i);
    }
    for (; i < 32; i += 2) {
      bn_mul_mont_gather5(tmp.d, am.d, powerbuf, np, n0, top, i - 1);
      bn_scatter5(tmp.d, top, powerbuf, i);
    }

    bits--;
    for (wvalue = 0, i = bits % 5; i >= 0; i--, bits--) {
      wvalue = (wvalue << 1) + BN_is_bit_set(p, bits);
    }
    bn_gather5(tmp.d, top, powerbuf, wvalue);

    // At this point |bits| is 4 mod 5 and at least -1. (|bits| is the first bit
    // that has not been read yet.)
    assert(bits >= -1 && (bits == -1 || bits % 5 == 4));

    // Scan the exponent one window at a time starting from the most
    // significant bits.
    if (top & 7) {
      while (bits >= 0) {
        for (wvalue = 0, i = 0; i < 5; i++, bits--) {
          wvalue = (wvalue << 1) + BN_is_bit_set(p, bits);
        }

        bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
        bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
        bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
        bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
        bn_mul_mont(tmp.d, tmp.d, tmp.d, np, n0, top);
        bn_mul_mont_gather5(tmp.d, tmp.d, powerbuf, np, n0, top, wvalue);
      }
    } else {
      const uint8_t *p_bytes = (const uint8_t *)p->d;
      assert(bits < max_bits);
      // |p = 0| has been handled as a special case, so |max_bits| is at least
      // one word.
      assert(max_bits >= 64);

      // If the first bit to be read lands in the last byte, unroll the first
      // iteration to avoid reading past the bounds of |p->d|. (After the first
      // iteration, we are guaranteed to be past the last byte.) Note |bits|
      // here is the top bit, inclusive.
      if (bits - 4 >= max_bits - 8) {
        // Read five bits from |bits-4| through |bits|, inclusive.
        wvalue = p_bytes[p->width * BN_BYTES - 1];
        wvalue >>= (bits - 4) & 7;
        wvalue &= 0x1f;
        bits -= 5;
        bn_power5(tmp.d, tmp.d, powerbuf, np, n0, top, wvalue);
      }
      while (bits >= 0) {
        // Read five bits from |bits-4| through |bits|, inclusive.
        int first_bit = bits - 4;
        uint16_t val;
        OPENSSL_memcpy(&val, p_bytes + (first_bit >> 3), sizeof(val));
        val >>= first_bit & 7;
        val &= 0x1f;
        bits -= 5;
        bn_power5(tmp.d, tmp.d, powerbuf, np, n0, top, val);
      }
    }

    ret = bn_from_montgomery(tmp.d, tmp.d, NULL, np, n0, top);
    tmp.width = top;
    if (ret) {
      if (!BN_copy(rr, &tmp)) {
        ret = 0;
      }
      goto err;  // non-zero ret means it's not error
    }
  } else
#endif
  {
    copy_to_prebuf(&tmp, top, powerbuf, 0, window);
    copy_to_prebuf(&am, top, powerbuf, 1, window);

    // If the window size is greater than 1, then calculate
    // val[i=2..2^winsize-1]. Powers are computed as a*a^(i-1)
    // (even powers could instead be computed as (a^(i/2))^2
    // to use the slight performance advantage of sqr over mul).
    if (window > 1) {
      if (!BN_mod_mul_montgomery(&tmp, &am, &am, mont, ctx)) {
        goto err;
      }

      copy_to_prebuf(&tmp, top, powerbuf, 2, window);

      for (i = 3; i < numPowers; i++) {
        // Calculate a^i = a^(i-1) * a
        if (!BN_mod_mul_montgomery(&tmp, &am, &tmp, mont, ctx)) {
          goto err;
        }

        copy_to_prebuf(&tmp, top, powerbuf, i, window);
      }
    }

    bits--;
    for (wvalue = 0, i = bits % window; i >= 0; i--, bits--) {
      wvalue = (wvalue << 1) + BN_is_bit_set(p, bits);
    }
    if (!copy_from_prebuf(&tmp, top, powerbuf, wvalue, window)) {
      goto err;
    }

    // Scan the exponent one window at a time starting from the most
    // significant bits.
    while (bits >= 0) {
      wvalue = 0;  // The 'value' of the window

      // Scan the window, squaring the result as we go
      for (i = 0; i < window; i++, bits--) {
        if (!BN_mod_mul_montgomery(&tmp, &tmp, &tmp, mont, ctx)) {
          goto err;
        }
        wvalue = (wvalue << 1) + BN_is_bit_set(p, bits);
      }

      // Fetch the appropriate pre-computed value from the pre-buf
      if (!copy_from_prebuf(&am, top, powerbuf, wvalue, window)) {
        goto err;
      }

      // Multiply the result into the intermediate result
      if (!BN_mod_mul_montgomery(&tmp, &tmp, &am, mont, ctx)) {
        goto err;
      }
    }
  }

  // Convert the final result from montgomery to standard format
  if (!BN_from_montgomery(rr, &tmp, mont, ctx)) {
    goto err;
  }
  ret = 1;

err:
  BN_MONT_CTX_free(new_mont);
  if (powerbuf != NULL && powerbufFree == NULL) {
    OPENSSL_cleanse(powerbuf, powerbufLen);
  }
  OPENSSL_free(powerbufFree);
  return (ret);
}

int BN_mod_exp_mont_word(BIGNUM *rr, BN_ULONG a, const BIGNUM *p,
                         const BIGNUM *m, BN_CTX *ctx,
                         const BN_MONT_CTX *mont) {
  BIGNUM a_bignum;
  BN_init(&a_bignum);

  int ret = 0;

  // BN_mod_exp_mont requires reduced inputs.
  if (bn_minimal_width(m) == 1) {
    a %= m->d[0];
  }

  if (!BN_set_word(&a_bignum, a)) {
    OPENSSL_PUT_ERROR(BN, ERR_R_INTERNAL_ERROR);
    goto err;
  }

  ret = BN_mod_exp_mont(rr, &a_bignum, p, m, ctx, mont);

err:
  BN_free(&a_bignum);

  return ret;
}

#define TABLE_SIZE 32

int BN_mod_exp2_mont(BIGNUM *rr, const BIGNUM *a1, const BIGNUM *p1,
                     const BIGNUM *a2, const BIGNUM *p2, const BIGNUM *m,
                     BN_CTX *ctx, const BN_MONT_CTX *mont) {
  BIGNUM tmp;
  BN_init(&tmp);

  int ret = 0;
  BN_MONT_CTX *new_mont = NULL;

  // Allocate a montgomery context if it was not supplied by the caller.
  if (mont == NULL) {
    new_mont = BN_MONT_CTX_new_for_modulus(m, ctx);
    if (new_mont == NULL) {
      goto err;
    }
    mont = new_mont;
  }

  // BN_mod_mul_montgomery removes one Montgomery factor, so passing one
  // Montgomery-encoded and one non-Montgomery-encoded value gives a
  // non-Montgomery-encoded result.
  if (!BN_mod_exp_mont(rr, a1, p1, m, ctx, mont) ||
      !BN_mod_exp_mont(&tmp, a2, p2, m, ctx, mont) ||
      !BN_to_montgomery(rr, rr, mont, ctx) ||
      !BN_mod_mul_montgomery(rr, rr, &tmp, mont, ctx)) {
    goto err;
  }

  ret = 1;

err:
  BN_MONT_CTX_free(new_mont);
  BN_free(&tmp);

  return ret;
}
