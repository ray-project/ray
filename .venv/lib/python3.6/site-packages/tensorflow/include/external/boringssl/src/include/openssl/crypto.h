/* Copyright (c) 2014, Google Inc.
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

#ifndef OPENSSL_HEADER_CRYPTO_H
#define OPENSSL_HEADER_CRYPTO_H

#include <openssl/base.h>

// Upstream OpenSSL defines |OPENSSL_malloc|, etc., in crypto.h rather than
// mem.h.
#include <openssl/mem.h>

// Upstream OpenSSL defines |CRYPTO_LOCK|, etc., in crypto.h rather than
// thread.h.
#include <openssl/thread.h>


#if defined(__cplusplus)
extern "C" {
#endif


// crypto.h contains functions for initializing the crypto library.


// CRYPTO_library_init initializes the crypto library. It must be called if the
// library is built with BORINGSSL_NO_STATIC_INITIALIZER. Otherwise, it does
// nothing and a static initializer is used instead. It is safe to call this
// function multiple times and concurrently from multiple threads.
//
// On some ARM configurations, this function may require filesystem access and
// should be called before entering a sandbox.
OPENSSL_EXPORT void CRYPTO_library_init(void);

// CRYPTO_is_confidential_build returns one if the linked version of BoringSSL
// has been built with the BORINGSSL_CONFIDENTIAL define and zero otherwise.
//
// This is used by some consumers to identify whether they are using an
// internal version of BoringSSL.
OPENSSL_EXPORT int CRYPTO_is_confidential_build(void);

// CRYPTO_has_asm returns one unless BoringSSL was built with OPENSSL_NO_ASM,
// in which case it returns zero.
OPENSSL_EXPORT int CRYPTO_has_asm(void);

// FIPS_mode returns zero unless BoringSSL is built with BORINGSSL_FIPS, in
// which case it returns one.
OPENSSL_EXPORT int FIPS_mode(void);

// BORINGSSL_self_test triggers the FIPS KAT-based self tests. It returns one
// on success and zero on error.
OPENSSL_EXPORT int BORINGSSL_self_test(void);


// Deprecated functions.

// OPENSSL_VERSION_TEXT contains a string the identifies the version of
// “OpenSSL”. node.js requires a version number in this text.
#define OPENSSL_VERSION_TEXT "OpenSSL 1.1.0 (compatible; BoringSSL)"

#define OPENSSL_VERSION 0
#define OPENSSL_CFLAGS 1
#define OPENSSL_BUILT_ON 2
#define OPENSSL_PLATFORM 3
#define OPENSSL_DIR 4

// OpenSSL_version is a compatibility function that returns the string
// "BoringSSL" if |which| is |OPENSSL_VERSION| and placeholder strings
// otherwise.
OPENSSL_EXPORT const char *OpenSSL_version(int which);

#define SSLEAY_VERSION OPENSSL_VERSION
#define SSLEAY_CFLAGS OPENSSL_CFLAGS
#define SSLEAY_BUILT_ON OPENSSL_BUILT_ON
#define SSLEAY_PLATFORM OPENSSL_PLATFORM
#define SSLEAY_DIR OPENSSL_DIR

// SSLeay_version calls |OpenSSL_version|.
OPENSSL_EXPORT const char *SSLeay_version(int which);

// SSLeay is a compatibility function that returns OPENSSL_VERSION_NUMBER from
// base.h.
OPENSSL_EXPORT unsigned long SSLeay(void);

// OpenSSL_version_num is a compatibility function that returns
// OPENSSL_VERSION_NUMBER from base.h.
OPENSSL_EXPORT unsigned long OpenSSL_version_num(void);

// CRYPTO_malloc_init returns one.
OPENSSL_EXPORT int CRYPTO_malloc_init(void);

// OPENSSL_malloc_init returns one.
OPENSSL_EXPORT int OPENSSL_malloc_init(void);

// ENGINE_load_builtin_engines does nothing.
OPENSSL_EXPORT void ENGINE_load_builtin_engines(void);

// ENGINE_register_all_complete returns one.
OPENSSL_EXPORT int ENGINE_register_all_complete(void);

// OPENSSL_load_builtin_modules does nothing.
OPENSSL_EXPORT void OPENSSL_load_builtin_modules(void);

#define OPENSSL_INIT_NO_LOAD_CRYPTO_STRINGS 0
#define OPENSSL_INIT_LOAD_CRYPTO_STRINGS 0
#define OPENSSL_INIT_ADD_ALL_CIPHERS 0
#define OPENSSL_INIT_ADD_ALL_DIGESTS 0
#define OPENSSL_INIT_NO_ADD_ALL_CIPHERS 0
#define OPENSSL_INIT_NO_ADD_ALL_DIGESTS 0
#define OPENSSL_INIT_LOAD_CONFIG 0
#define OPENSSL_INIT_NO_LOAD_CONFIG 0

// OPENSSL_init_crypto calls |CRYPTO_library_init| and returns one.
OPENSSL_EXPORT int OPENSSL_init_crypto(uint64_t opts,
                                       const OPENSSL_INIT_SETTINGS *settings);

// FIPS_mode_set returns one if |on| matches whether BoringSSL was built with
// |BORINGSSL_FIPS| and zero otherwise.
OPENSSL_EXPORT int FIPS_mode_set(int on);


#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_CRYPTO_H
