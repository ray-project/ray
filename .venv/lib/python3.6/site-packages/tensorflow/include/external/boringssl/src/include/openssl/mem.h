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
 * [including the GNU Public Licence.] */

#ifndef OPENSSL_HEADER_MEM_H
#define OPENSSL_HEADER_MEM_H

#include <openssl/base.h>

#include <stdlib.h>
#include <stdarg.h>

#if defined(__cplusplus)
extern "C" {
#endif


// Memory and string functions, see also buf.h.
//
// BoringSSL has its own set of allocation functions, which keep track of
// allocation lengths and zero them out before freeing. All memory returned by
// BoringSSL API calls must therefore generally be freed using |OPENSSL_free|
// unless stated otherwise.


// OPENSSL_malloc acts like a regular |malloc|.
OPENSSL_EXPORT void *OPENSSL_malloc(size_t size);

// OPENSSL_free does nothing if |ptr| is NULL. Otherwise it zeros out the
// memory allocated at |ptr| and frees it.
OPENSSL_EXPORT void OPENSSL_free(void *ptr);

// OPENSSL_realloc returns a pointer to a buffer of |new_size| bytes that
// contains the contents of |ptr|. Unlike |realloc|, a new buffer is always
// allocated and the data at |ptr| is always wiped and freed.
OPENSSL_EXPORT void *OPENSSL_realloc(void *ptr, size_t new_size);

// OPENSSL_cleanse zeros out |len| bytes of memory at |ptr|. This is similar to
// |memset_s| from C11.
OPENSSL_EXPORT void OPENSSL_cleanse(void *ptr, size_t len);

// CRYPTO_memcmp returns zero iff the |len| bytes at |a| and |b| are equal. It
// takes an amount of time dependent on |len|, but independent of the contents
// of |a| and |b|. Unlike memcmp, it cannot be used to put elements into a
// defined order as the return value when a != b is undefined, other than to be
// non-zero.
OPENSSL_EXPORT int CRYPTO_memcmp(const void *a, const void *b, size_t len);

// OPENSSL_hash32 implements the 32 bit, FNV-1a hash.
OPENSSL_EXPORT uint32_t OPENSSL_hash32(const void *ptr, size_t len);

// OPENSSL_strdup has the same behaviour as strdup(3).
OPENSSL_EXPORT char *OPENSSL_strdup(const char *s);

// OPENSSL_strnlen has the same behaviour as strnlen(3).
OPENSSL_EXPORT size_t OPENSSL_strnlen(const char *s, size_t len);

// OPENSSL_tolower is a locale-independent version of tolower(3).
OPENSSL_EXPORT int OPENSSL_tolower(int c);

// OPENSSL_strcasecmp is a locale-independent version of strcasecmp(3).
OPENSSL_EXPORT int OPENSSL_strcasecmp(const char *a, const char *b);

// OPENSSL_strncasecmp is a locale-independent version of strncasecmp(3).
OPENSSL_EXPORT int OPENSSL_strncasecmp(const char *a, const char *b, size_t n);

// DECIMAL_SIZE returns an upper bound for the length of the decimal
// representation of the given type.
#define DECIMAL_SIZE(type)	((sizeof(type)*8+2)/3+1)

// BIO_snprintf has the same behavior as snprintf(3).
OPENSSL_EXPORT int BIO_snprintf(char *buf, size_t n, const char *format, ...)
    OPENSSL_PRINTF_FORMAT_FUNC(3, 4);

// BIO_vsnprintf has the same behavior as vsnprintf(3).
OPENSSL_EXPORT int BIO_vsnprintf(char *buf, size_t n, const char *format,
                                 va_list args)
    OPENSSL_PRINTF_FORMAT_FUNC(3, 0);


// Deprecated functions.

#define CRYPTO_malloc OPENSSL_malloc
#define CRYPTO_realloc OPENSSL_realloc
#define CRYPTO_free OPENSSL_free


#if defined(__cplusplus)
}  // extern C

extern "C++" {

namespace bssl {

BORINGSSL_MAKE_DELETER(char, OPENSSL_free)
BORINGSSL_MAKE_DELETER(uint8_t, OPENSSL_free)

}  // namespace bssl

}  // extern C++

#endif

#endif  // OPENSSL_HEADER_MEM_H
