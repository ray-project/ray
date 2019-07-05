/*
 * Copyright 2013-2016 The OpenSSL Project Authors. All Rights Reserved.
 * Copyright (c) 2012, Intel Corporation. All Rights Reserved.
 *
 * Licensed under the OpenSSL license (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 *
 * Originally written by Shay Gueron (1, 2), and Vlad Krasnov (1)
 * (1) Intel Corporation, Israel Development Center, Haifa, Israel
 * (2) University of Haifa, Israel
 */

#ifndef OPENSSL_HEADER_BN_RSAZ_EXP_H
#define OPENSSL_HEADER_BN_RSAZ_EXP_H

#include <openssl/bn.h>

#include "internal.h"

// RSAZ_1024_mod_exp_avx2 sets |result| to |base_norm| raised to |exponent|
// modulo |m_norm|. |base_norm| must be fully-reduced and |exponent| must have
// the high bit set (it is 1024 bits wide). |RR| and |k0| must be |RR| and |n0|,
// respectively, extracted from |m_norm|'s |BN_MONT_CTX|. |storage_words| is a
// temporary buffer that must be aligned to |MOD_EXP_CTIME_MIN_CACHE_LINE_WIDTH|
// bytes.
void RSAZ_1024_mod_exp_avx2(BN_ULONG result[16], const BN_ULONG base_norm[16],
                            const BN_ULONG exponent[16],
                            const BN_ULONG m_norm[16], const BN_ULONG RR[16],
                            BN_ULONG k0,
                            BN_ULONG storage_words[MOD_EXP_CTIME_STORAGE_LEN]);

// rsaz_avx2_eligible returns one if |RSAZ_1024_mod_exp_avx2| should be used and
// zero otherwise.
int rsaz_avx2_eligible(void);

#endif  // OPENSSL_HEADER_BN_RSAZ_EXP_H
