/* Copyright (c) 2015, Google Inc.
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

#include <openssl/base.h>

#include <openssl/ec.h>

#include "internal.h"

// This function looks at 5+1 scalar bits (5 current, 1 adjacent less
// significant bit), and recodes them into a signed digit for use in fast point
// multiplication: the use of signed rather than unsigned digits means that
// fewer points need to be precomputed, given that point inversion is easy (a
// precomputed point dP makes -dP available as well).
//
// BACKGROUND:
//
// Signed digits for multiplication were introduced by Booth ("A signed binary
// multiplication technique", Quart. Journ. Mech. and Applied Math., vol. IV,
// pt. 2 (1951), pp. 236-240), in that case for multiplication of integers.
// Booth's original encoding did not generally improve the density of nonzero
// digits over the binary representation, and was merely meant to simplify the
// handling of signed factors given in two's complement; but it has since been
// shown to be the basis of various signed-digit representations that do have
// further advantages, including the wNAF, using the following general
// approach:
//
// (1) Given a binary representation
//
//       b_k  ...  b_2  b_1  b_0,
//
//     of a nonnegative integer (b_k in {0, 1}), rewrite it in digits 0, 1, -1
//     by using bit-wise subtraction as follows:
//
//        b_k b_(k-1)  ...  b_2  b_1  b_0
//      -     b_k      ...  b_3  b_2  b_1  b_0
//       -------------------------------------
//        s_k b_(k-1)  ...  s_3  s_2  s_1  s_0
//
//     A left-shift followed by subtraction of the original value yields a new
//     representation of the same value, using signed bits s_i = b_(i+1) - b_i.
//     This representation from Booth's paper has since appeared in the
//     literature under a variety of different names including "reversed binary
//     form", "alternating greedy expansion", "mutual opposite form", and
//     "sign-alternating {+-1}-representation".
//
//     An interesting property is that among the nonzero bits, values 1 and -1
//     strictly alternate.
//
// (2) Various window schemes can be applied to the Booth representation of
//     integers: for example, right-to-left sliding windows yield the wNAF
//     (a signed-digit encoding independently discovered by various researchers
//     in the 1990s), and left-to-right sliding windows yield a left-to-right
//     equivalent of the wNAF (independently discovered by various researchers
//     around 2004).
//
// To prevent leaking information through side channels in point multiplication,
// we need to recode the given integer into a regular pattern: sliding windows
// as in wNAFs won't do, we need their fixed-window equivalent -- which is a few
// decades older: we'll be using the so-called "modified Booth encoding" due to
// MacSorley ("High-speed arithmetic in binary computers", Proc. IRE, vol. 49
// (1961), pp. 67-91), in a radix-2^5 setting.  That is, we always combine five
// signed bits into a signed digit:
//
//       s_(4j + 4) s_(4j + 3) s_(4j + 2) s_(4j + 1) s_(4j)
//
// The sign-alternating property implies that the resulting digit values are
// integers from -16 to 16.
//
// Of course, we don't actually need to compute the signed digits s_i as an
// intermediate step (that's just a nice way to see how this scheme relates
// to the wNAF): a direct computation obtains the recoded digit from the
// six bits b_(4j + 4) ... b_(4j - 1).
//
// This function takes those five bits as an integer (0 .. 63), writing the
// recoded digit to *sign (0 for positive, 1 for negative) and *digit (absolute
// value, in the range 0 .. 8).  Note that this integer essentially provides the
// input bits "shifted to the left" by one position: for example, the input to
// compute the least significant recoded digit, given that there's no bit b_-1,
// has to be b_4 b_3 b_2 b_1 b_0 0.
void ec_GFp_nistp_recode_scalar_bits(uint8_t *sign, uint8_t *digit,
                                     uint8_t in) {
  uint8_t s, d;

  s = ~((in >> 5) - 1); /* sets all bits to MSB(in), 'in' seen as
                          * 6-bit value */
  d = (1 << 6) - in - 1;
  d = (d & s) | (in & ~s);
  d = (d >> 1) + (d & 1);

  *sign = s & 1;
  *digit = d;
}
