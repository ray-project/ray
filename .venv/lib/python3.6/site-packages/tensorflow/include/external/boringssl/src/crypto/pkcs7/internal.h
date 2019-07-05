/* Copyright (c) 2017, Google Inc.
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

#ifndef OPENSSL_HEADER_PKCS7_INTERNAL_H
#define OPENSSL_HEADER_PKCS7_INTERNAL_H

#include <openssl/base.h>

#if defined(__cplusplus)
extern "C" {
#endif


// pkcs7_parse_header reads the non-certificate/non-CRL prefix of a PKCS#7
// SignedData blob from |cbs| and sets |*out| to point to the rest of the
// input. If the input is in BER format, then |*der_bytes| will be set to a
// pointer that needs to be freed by the caller once they have finished
// processing |*out| (which will be pointing into |*der_bytes|).
//
// It returns one on success or zero on error. On error, |*der_bytes| is
// NULL.
int pkcs7_parse_header(uint8_t **der_bytes, CBS *out, CBS *cbs);

// pkcs7_bundle writes a PKCS#7, SignedData structure to |out| and then calls
// |cb| with a CBB to which certificate or CRL data can be written, and the
// opaque context pointer, |arg|. The callback can return zero to indicate an
// error.
//
// pkcs7_bundle returns one on success or zero on error.
int pkcs7_bundle(CBB *out, int (*cb)(CBB *out, const void *arg),
                 const void *arg);


#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_PKCS7_INTERNAL_H
