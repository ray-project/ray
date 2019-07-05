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
 * Copyright (c) 1998-2007 The OpenSSL Project.  All rights reserved.
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
 * Hudson (tjh@cryptsoft.com).
 *
 */
/* ====================================================================
 * Copyright 2002 Sun Microsystems, Inc. ALL RIGHTS RESERVED.
 * ECC cipher suite support in OpenSSL originally developed by
 * SUN MICROSYSTEMS, INC., and contributed to the OpenSSL project.
 */
/* ====================================================================
 * Copyright 2005 Nokia. All rights reserved.
 *
 * The portions of the attached software ("Contribution") is developed by
 * Nokia Corporation and is licensed pursuant to the OpenSSL open source
 * license.
 *
 * The Contribution, originally written by Mika Kousa and Pasi Eronen of
 * Nokia Corporation, consists of the "PSK" (Pre-Shared Key) ciphersuites
 * support (see RFC 4279) to OpenSSL.
 *
 * No patent licenses or other rights except those expressly stated in
 * the OpenSSL open source license shall be deemed granted or received
 * expressly, by implication, estoppel, or otherwise.
 *
 * No assurances are provided by Nokia that the Contribution does not
 * infringe the patent or other intellectual property rights of any third
 * party or that the license provides you with all the necessary rights
 * to make use of the Contribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND. IN
 * ADDITION TO THE DISCLAIMERS INCLUDED IN THE LICENSE, NOKIA
 * SPECIFICALLY DISCLAIMS ANY LIABILITY FOR CLAIMS BROUGHT BY YOU OR ANY
 * OTHER ENTITY BASED ON INFRINGEMENT OF INTELLECTUAL PROPERTY RIGHTS OR
 * OTHERWISE.
 */

#ifndef OPENSSL_HEADER_SSL_INTERNAL_H
#define OPENSSL_HEADER_SSL_INTERNAL_H

#include <openssl/base.h>

#include <stdlib.h>

#include <limits>
#include <new>
#include <type_traits>
#include <utility>

#include <openssl/aead.h>
#include <openssl/err.h>
#include <openssl/lhash.h>
#include <openssl/mem.h>
#include <openssl/ssl.h>
#include <openssl/span.h>
#include <openssl/stack.h>

#include "../crypto/err/internal.h"
#include "../crypto/internal.h"


#if defined(OPENSSL_WINDOWS)
// Windows defines struct timeval in winsock2.h.
OPENSSL_MSVC_PRAGMA(warning(push, 3))
#include <winsock2.h>
OPENSSL_MSVC_PRAGMA(warning(pop))
#else
#include <sys/time.h>
#endif


namespace bssl {

struct SSL_CONFIG;
struct SSL_HANDSHAKE;
struct SSL_PROTOCOL_METHOD;
struct SSL_X509_METHOD;

// C++ utilities.

// New behaves like |new| but uses |OPENSSL_malloc| for memory allocation. It
// returns nullptr on allocation error. It only implements single-object
// allocation and not new T[n].
//
// Note: unlike |new|, this does not support non-public constructors.
template <typename T, typename... Args>
T *New(Args &&... args) {
  void *t = OPENSSL_malloc(sizeof(T));
  if (t == nullptr) {
    OPENSSL_PUT_ERROR(SSL, ERR_R_MALLOC_FAILURE);
    return nullptr;
  }
  return new (t) T(std::forward<Args>(args)...);
}

// Delete behaves like |delete| but uses |OPENSSL_free| to release memory.
//
// Note: unlike |delete| this does not support non-public destructors.
template <typename T>
void Delete(T *t) {
  if (t != nullptr) {
    t->~T();
    OPENSSL_free(t);
  }
}

// All types with kAllowUniquePtr set may be used with UniquePtr. Other types
// may be C structs which require a |BORINGSSL_MAKE_DELETER| registration.
namespace internal {
template <typename T>
struct DeleterImpl<T, typename std::enable_if<T::kAllowUniquePtr>::type> {
  static void Free(T *t) { Delete(t); }
};
}

// MakeUnique behaves like |std::make_unique| but returns nullptr on allocation
// error.
template <typename T, typename... Args>
UniquePtr<T> MakeUnique(Args &&... args) {
  return UniquePtr<T>(New<T>(std::forward<Args>(args)...));
}

#if defined(BORINGSSL_ALLOW_CXX_RUNTIME)
#define HAS_VIRTUAL_DESTRUCTOR
#define PURE_VIRTUAL = 0
#else
// HAS_VIRTUAL_DESTRUCTOR should be declared in any base class which defines a
// virtual destructor. This avoids a dependency on |_ZdlPv| and prevents the
// class from being used with |delete|.
#define HAS_VIRTUAL_DESTRUCTOR \
  void operator delete(void *) { abort(); }

// PURE_VIRTUAL should be used instead of = 0 when defining pure-virtual
// functions. This avoids a dependency on |__cxa_pure_virtual| but loses
// compile-time checking.
#define PURE_VIRTUAL { abort(); }
#endif

// CONSTEXPR_ARRAY works around a VS 2015 bug where ranged for loops don't work
// on constexpr arrays.
#if defined(_MSC_VER) && !defined(__clang__) && _MSC_VER < 1910
#define CONSTEXPR_ARRAY const
#else
#define CONSTEXPR_ARRAY constexpr
#endif

// Array<T> is an owning array of elements of |T|.
template <typename T>
class Array {
 public:
  // Array's default constructor creates an empty array.
  Array() {}
  Array(const Array &) = delete;
  Array(Array &&other) { *this = std::move(other); }

  ~Array() { Reset(); }

  Array &operator=(const Array &) = delete;
  Array &operator=(Array &&other) {
    Reset();
    other.Release(&data_, &size_);
    return *this;
  }

  const T *data() const { return data_; }
  T *data() { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  const T &operator[](size_t i) const { return data_[i]; }
  T &operator[](size_t i) { return data_[i]; }

  T *begin() { return data_; }
  const T *cbegin() const { return data_; }
  T *end() { return data_ + size_; }
  const T *cend() const { return data_ + size_; }

  void Reset() { Reset(nullptr, 0); }

  // Reset releases the current contents of the array and takes ownership of the
  // raw pointer supplied by the caller.
  void Reset(T *new_data, size_t new_size) {
    for (size_t i = 0; i < size_; i++) {
      data_[i].~T();
    }
    OPENSSL_free(data_);
    data_ = new_data;
    size_ = new_size;
  }

  // Release releases ownership of the array to a raw pointer supplied by the
  // caller.
  void Release(T **out, size_t *out_size) {
    *out = data_;
    *out_size = size_;
    data_ = nullptr;
    size_ = 0;
  }

  // Init replaces the array with a newly-allocated array of |new_size|
  // default-constructed copies of |T|. It returns true on success and false on
  // error.
  //
  // Note that if |T| is a primitive type like |uint8_t|, it is uninitialized.
  bool Init(size_t new_size) {
    Reset();
    if (new_size == 0) {
      return true;
    }

    if (new_size > std::numeric_limits<size_t>::max() / sizeof(T)) {
      OPENSSL_PUT_ERROR(SSL, ERR_R_OVERFLOW);
      return false;
    }
    data_ = reinterpret_cast<T*>(OPENSSL_malloc(new_size * sizeof(T)));
    if (data_ == nullptr) {
      OPENSSL_PUT_ERROR(SSL, ERR_R_MALLOC_FAILURE);
      return false;
    }
    size_ = new_size;
    for (size_t i = 0; i < size_; i++) {
      new (&data_[i]) T;
    }
    return true;
  }

  // CopyFrom replaces the array with a newly-allocated copy of |in|. It returns
  // true on success and false on error.
  bool CopyFrom(Span<const T> in) {
    if (!Init(in.size())) {
      return false;
    }
    OPENSSL_memcpy(data_, in.data(), sizeof(T) * in.size());
    return true;
  }

 private:
  T *data_ = nullptr;
  size_t size_ = 0;
};

// CBBFinishArray behaves like |CBB_finish| but stores the result in an Array.
OPENSSL_EXPORT bool CBBFinishArray(CBB *cbb, Array<uint8_t> *out);


// Protocol versions.
//
// Due to DTLS's historical wire version differences and to support multiple
// variants of the same protocol during development, we maintain two notions of
// version.
//
// The "version" or "wire version" is the actual 16-bit value that appears on
// the wire. It uniquely identifies a version and is also used at API
// boundaries. The set of supported versions differs between TLS and DTLS. Wire
// versions are opaque values and may not be compared numerically.
//
// The "protocol version" identifies the high-level handshake variant being
// used. DTLS versions map to the corresponding TLS versions. Draft TLS 1.3
// variants all map to TLS 1.3. Protocol versions are sequential and may be
// compared numerically.

// ssl_protocol_version_from_wire sets |*out| to the protocol version
// corresponding to wire version |version| and returns true. If |version| is not
// a valid TLS or DTLS version, it returns false.
//
// Note this simultaneously handles both DTLS and TLS. Use one of the
// higher-level functions below for most operations.
bool ssl_protocol_version_from_wire(uint16_t *out, uint16_t version);

// ssl_get_version_range sets |*out_min_version| and |*out_max_version| to the
// minimum and maximum enabled protocol versions, respectively.
bool ssl_get_version_range(const SSL_HANDSHAKE *hs, uint16_t *out_min_version,
                           uint16_t *out_max_version);

// ssl_supports_version returns whether |hs| supports |version|.
bool ssl_supports_version(SSL_HANDSHAKE *hs, uint16_t version);

// ssl_method_supports_version returns whether |method| supports |version|.
bool ssl_method_supports_version(const SSL_PROTOCOL_METHOD *method,
                                 uint16_t version);

// ssl_add_supported_versions writes the supported versions of |hs| to |cbb|, in
// decreasing preference order.
bool ssl_add_supported_versions(SSL_HANDSHAKE *hs, CBB *cbb);

// ssl_negotiate_version negotiates a common version based on |hs|'s preferences
// and the peer preference list in |peer_versions|. On success, it returns true
// and sets |*out_version| to the selected version. Otherwise, it returns false
// and sets |*out_alert| to an alert to send.
bool ssl_negotiate_version(SSL_HANDSHAKE *hs, uint8_t *out_alert,
                           uint16_t *out_version, const CBS *peer_versions);

// ssl_protocol_version returns |ssl|'s protocol version. It is an error to
// call this function before the version is determined.
uint16_t ssl_protocol_version(const SSL *ssl);

// ssl_is_draft28 returns whether the version corresponds to a draft28 TLS 1.3
// variant.
bool ssl_is_draft28(uint16_t version);

// Cipher suites.

}  // namespace bssl

struct ssl_cipher_st {
  // name is the OpenSSL name for the cipher.
  const char *name;
  // standard_name is the IETF name for the cipher.
  const char *standard_name;
  // id is the cipher suite value bitwise OR-d with 0x03000000.
  uint32_t id;

  // algorithm_* determine the cipher suite. See constants below for the values.
  uint32_t algorithm_mkey;
  uint32_t algorithm_auth;
  uint32_t algorithm_enc;
  uint32_t algorithm_mac;
  uint32_t algorithm_prf;
};

namespace bssl {

// Bits for |algorithm_mkey| (key exchange algorithm).
#define SSL_kRSA 0x00000001u
#define SSL_kECDHE 0x00000002u
// SSL_kPSK is only set for plain PSK, not ECDHE_PSK.
#define SSL_kPSK 0x00000004u
#define SSL_kGENERIC 0x00000008u

// Bits for |algorithm_auth| (server authentication).
#define SSL_aRSA 0x00000001u
#define SSL_aECDSA 0x00000002u
// SSL_aPSK is set for both PSK and ECDHE_PSK.
#define SSL_aPSK 0x00000004u
#define SSL_aGENERIC 0x00000008u

#define SSL_aCERT (SSL_aRSA | SSL_aECDSA)

// Bits for |algorithm_enc| (symmetric encryption).
#define SSL_3DES                 0x00000001u
#define SSL_AES128               0x00000002u
#define SSL_AES256               0x00000004u
#define SSL_AES128GCM            0x00000008u
#define SSL_AES256GCM            0x00000010u
#define SSL_eNULL                0x00000020u
#define SSL_CHACHA20POLY1305     0x00000040u

#define SSL_AES (SSL_AES128 | SSL_AES256 | SSL_AES128GCM | SSL_AES256GCM)

// Bits for |algorithm_mac| (symmetric authentication).
#define SSL_SHA1 0x00000001u
// SSL_AEAD is set for all AEADs.
#define SSL_AEAD 0x00000002u

// Bits for |algorithm_prf| (handshake digest).
#define SSL_HANDSHAKE_MAC_DEFAULT 0x1
#define SSL_HANDSHAKE_MAC_SHA256 0x2
#define SSL_HANDSHAKE_MAC_SHA384 0x4

// An SSLCipherPreferenceList contains a list of SSL_CIPHERs with equal-
// preference groups. For TLS clients, the groups are moot because the server
// picks the cipher and groups cannot be expressed on the wire. However, for
// servers, the equal-preference groups allow the client's preferences to be
// partially respected. (This only has an effect with
// SSL_OP_CIPHER_SERVER_PREFERENCE).
//
// The equal-preference groups are expressed by grouping SSL_CIPHERs together.
// All elements of a group have the same priority: no ordering is expressed
// within a group.
//
// The values in |ciphers| are in one-to-one correspondence with
// |in_group_flags|. (That is, sk_SSL_CIPHER_num(ciphers) is the number of
// bytes in |in_group_flags|.) The bytes in |in_group_flags| are either 1, to
// indicate that the corresponding SSL_CIPHER is not the last element of a
// group, or 0 to indicate that it is.
//
// For example, if |in_group_flags| contains all zeros then that indicates a
// traditional, fully-ordered preference. Every SSL_CIPHER is the last element
// of the group (i.e. they are all in a one-element group).
//
// For a more complex example, consider:
//   ciphers:        A  B  C  D  E  F
//   in_group_flags: 1  1  0  0  1  0
//
// That would express the following, order:
//
//    A         E
//    B -> D -> F
//    C
struct SSLCipherPreferenceList {
  static constexpr bool kAllowUniquePtr = true;

  SSLCipherPreferenceList() = default;
  ~SSLCipherPreferenceList();

  bool Init(UniquePtr<STACK_OF(SSL_CIPHER)> ciphers,
            Span<const bool> in_group_flags);

  UniquePtr<STACK_OF(SSL_CIPHER)> ciphers;
  bool *in_group_flags = nullptr;
};

// ssl_cipher_get_evp_aead sets |*out_aead| to point to the correct EVP_AEAD
// object for |cipher| protocol version |version|. It sets |*out_mac_secret_len|
// and |*out_fixed_iv_len| to the MAC key length and fixed IV length,
// respectively. The MAC key length is zero except for legacy block and stream
// ciphers. It returns true on success and false on error.
bool ssl_cipher_get_evp_aead(const EVP_AEAD **out_aead,
                             size_t *out_mac_secret_len,
                             size_t *out_fixed_iv_len, const SSL_CIPHER *cipher,
                             uint16_t version, int is_dtls);

// ssl_get_handshake_digest returns the |EVP_MD| corresponding to |version| and
// |cipher|.
const EVP_MD *ssl_get_handshake_digest(uint16_t version,
                                       const SSL_CIPHER *cipher);

// ssl_create_cipher_list evaluates |rule_str|. It sets |*out_cipher_list| to a
// newly-allocated |SSLCipherPreferenceList| containing the result. It returns
// true on success and false on failure. If |strict| is true, nonsense will be
// rejected. If false, nonsense will be silently ignored. An empty result is
// considered an error regardless of |strict|.
bool ssl_create_cipher_list(UniquePtr<SSLCipherPreferenceList> *out_cipher_list,
                            const char *rule_str, bool strict);

// ssl_cipher_get_value returns the cipher suite id of |cipher|.
uint16_t ssl_cipher_get_value(const SSL_CIPHER *cipher);

// ssl_cipher_auth_mask_for_key returns the mask of cipher |algorithm_auth|
// values suitable for use with |key| in TLS 1.2 and below.
uint32_t ssl_cipher_auth_mask_for_key(const EVP_PKEY *key);

// ssl_cipher_uses_certificate_auth returns whether |cipher| authenticates the
// server and, optionally, the client with a certificate.
bool ssl_cipher_uses_certificate_auth(const SSL_CIPHER *cipher);

// ssl_cipher_requires_server_key_exchange returns whether |cipher| requires a
// ServerKeyExchange message.
//
// This function may return false while still allowing |cipher| an optional
// ServerKeyExchange. This is the case for plain PSK ciphers.
bool ssl_cipher_requires_server_key_exchange(const SSL_CIPHER *cipher);

// ssl_cipher_get_record_split_len, for TLS 1.0 CBC mode ciphers, returns the
// length of an encrypted 1-byte record, for use in record-splitting. Otherwise
// it returns zero.
size_t ssl_cipher_get_record_split_len(const SSL_CIPHER *cipher);


// Transcript layer.

// SSLTranscript maintains the handshake transcript as a combination of a
// buffer and running hash.
class SSLTranscript {
 public:
  SSLTranscript();
  ~SSLTranscript();

  // Init initializes the handshake transcript. If called on an existing
  // transcript, it resets the transcript and hash. It returns true on success
  // and false on failure.
  bool Init();

  // InitHash initializes the handshake hash based on the PRF and contents of
  // the handshake transcript. Subsequent calls to |Update| will update the
  // rolling hash. It returns one on success and zero on failure. It is an error
  // to call this function after the handshake buffer is released.
  bool InitHash(uint16_t version, const SSL_CIPHER *cipher);

  // UpdateForHelloRetryRequest resets the rolling hash with the
  // HelloRetryRequest construction. It returns true on success and false on
  // failure. It is an error to call this function before the handshake buffer
  // is released.
  bool UpdateForHelloRetryRequest();

  // CopyHashContext copies the hash context into |ctx| and returns true on
  // success.
  bool CopyHashContext(EVP_MD_CTX *ctx);

  Span<const uint8_t> buffer() {
    return MakeConstSpan(reinterpret_cast<const uint8_t *>(buffer_->data),
                         buffer_->length);
  }

  // FreeBuffer releases the handshake buffer. Subsequent calls to
  // |Update| will not update the handshake buffer.
  void FreeBuffer();

  // DigestLen returns the length of the PRF hash.
  size_t DigestLen() const;

  // Digest returns the PRF hash. For TLS 1.1 and below, this is
  // |EVP_md5_sha1|.
  const EVP_MD *Digest() const;

  // Update adds |in| to the handshake buffer and handshake hash, whichever is
  // enabled. It returns true on success and false on failure.
  bool Update(Span<const uint8_t> in);

  // GetHash writes the handshake hash to |out| which must have room for at
  // least |DigestLen| bytes. On success, it returns true and sets |*out_len| to
  // the number of bytes written. Otherwise, it returns false.
  bool GetHash(uint8_t *out, size_t *out_len);

  // GetFinishedMAC computes the MAC for the Finished message into the bytes
  // pointed by |out| and writes the number of bytes to |*out_len|. |out| must
  // have room for |EVP_MAX_MD_SIZE| bytes. It returns true on success and false
  // on failure.
  bool GetFinishedMAC(uint8_t *out, size_t *out_len, const SSL_SESSION *session,
                      bool from_server);

 private:
  // buffer_, if non-null, contains the handshake transcript.
  UniquePtr<BUF_MEM> buffer_;
  // hash, if initialized with an |EVP_MD|, maintains the handshake hash.
  ScopedEVP_MD_CTX hash_;
};

// tls1_prf computes the PRF function for |ssl|. It fills |out|, using |secret|
// as the secret and |label| as the label. |seed1| and |seed2| are concatenated
// to form the seed parameter. It returns true on success and false on failure.
bool tls1_prf(const EVP_MD *digest, Span<uint8_t> out,
              Span<const uint8_t> secret, Span<const char> label,
              Span<const uint8_t> seed1, Span<const uint8_t> seed2);


// Encryption layer.

// SSLAEADContext contains information about an AEAD that is being used to
// encrypt an SSL connection.
class SSLAEADContext {
 public:
  SSLAEADContext(uint16_t version, bool is_dtls, const SSL_CIPHER *cipher);
  ~SSLAEADContext();
  static constexpr bool kAllowUniquePtr = true;

  SSLAEADContext(const SSLAEADContext &&) = delete;
  SSLAEADContext &operator=(const SSLAEADContext &&) = delete;

  // CreateNullCipher creates an |SSLAEADContext| for the null cipher.
  static UniquePtr<SSLAEADContext> CreateNullCipher(bool is_dtls);

  // Create creates an |SSLAEADContext| using the supplied key material. It
  // returns nullptr on error. Only one of |Open| or |Seal| may be used with the
  // resulting object, depending on |direction|. |version| is the normalized
  // protocol version, so DTLS 1.0 is represented as 0x0301, not 0xffef.
  static UniquePtr<SSLAEADContext> Create(enum evp_aead_direction_t direction,
                                          uint16_t version, int is_dtls,
                                          const SSL_CIPHER *cipher,
                                          Span<const uint8_t> enc_key,
                                          Span<const uint8_t> mac_key,
                                          Span<const uint8_t> fixed_iv);

  // SetVersionIfNullCipher sets the version the SSLAEADContext for the null
  // cipher, to make version-specific determinations in the record layer prior
  // to a cipher being selected.
  void SetVersionIfNullCipher(uint16_t version);

  // ProtocolVersion returns the protocol version associated with this
  // SSLAEADContext. It can only be called once |version_| has been set to a
  // valid value.
  uint16_t ProtocolVersion() const;

  // RecordVersion returns the record version that should be used with this
  // SSLAEADContext for record construction and crypto.
  uint16_t RecordVersion() const;

  const SSL_CIPHER *cipher() const { return cipher_; }

  // is_null_cipher returns true if this is the null cipher.
  bool is_null_cipher() const { return !cipher_; }

  // ExplicitNonceLen returns the length of the explicit nonce.
  size_t ExplicitNonceLen() const;

  // MaxOverhead returns the maximum overhead of calling |Seal|.
  size_t MaxOverhead() const;

  // SuffixLen calculates the suffix length written by |SealScatter| and writes
  // it to |*out_suffix_len|. It returns true on success and false on error.
  // |in_len| and |extra_in_len| should equal the argument of the same names
  // passed to |SealScatter|.
  bool SuffixLen(size_t *out_suffix_len, size_t in_len,
                 size_t extra_in_len) const;

  // CiphertextLen calculates the total ciphertext length written by
  // |SealScatter| and writes it to |*out_len|. It returns true on success and
  // false on error. |in_len| and |extra_in_len| should equal the argument of
  // the same names passed to |SealScatter|.
  bool CiphertextLen(size_t *out_len, size_t in_len, size_t extra_in_len) const;

  // Open authenticates and decrypts |in| in-place. On success, it sets |*out|
  // to the plaintext in |in| and returns true.  Otherwise, it returns
  // false. The output will always be |ExplicitNonceLen| bytes ahead of |in|.
  bool Open(Span<uint8_t> *out, uint8_t type, uint16_t record_version,
            const uint8_t seqnum[8], Span<const uint8_t> header,
            Span<uint8_t> in);

  // Seal encrypts and authenticates |in_len| bytes from |in| and writes the
  // result to |out|. It returns true on success and false on error.
  //
  // If |in| and |out| alias then |out| + |ExplicitNonceLen| must be == |in|.
  bool Seal(uint8_t *out, size_t *out_len, size_t max_out, uint8_t type,
            uint16_t record_version, const uint8_t seqnum[8],
            Span<const uint8_t> header, const uint8_t *in, size_t in_len);

  // SealScatter encrypts and authenticates |in_len| bytes from |in| and splits
  // the result between |out_prefix|, |out| and |out_suffix|. It returns one on
  // success and zero on error.
  //
  // On successful return, exactly |ExplicitNonceLen| bytes are written to
  // |out_prefix|, |in_len| bytes to |out|, and |SuffixLen| bytes to
  // |out_suffix|.
  //
  // |extra_in| may point to an additional plaintext buffer. If present,
  // |extra_in_len| additional bytes are encrypted and authenticated, and the
  // ciphertext is written to the beginning of |out_suffix|. |SuffixLen| should
  // be used to size |out_suffix| accordingly.
  //
  // If |in| and |out| alias then |out| must be == |in|. Other arguments may not
  // alias anything.
  bool SealScatter(uint8_t *out_prefix, uint8_t *out, uint8_t *out_suffix,
                   uint8_t type, uint16_t record_version,
                   const uint8_t seqnum[8], Span<const uint8_t> header,
                   const uint8_t *in, size_t in_len, const uint8_t *extra_in,
                   size_t extra_in_len);

  bool GetIV(const uint8_t **out_iv, size_t *out_iv_len) const;

 private:
  // GetAdditionalData returns the additional data, writing into |storage| if
  // necessary.
  Span<const uint8_t> GetAdditionalData(uint8_t storage[13], uint8_t type,
                                        uint16_t record_version,
                                        const uint8_t seqnum[8],
                                        size_t plaintext_len,
                                        Span<const uint8_t> header);

  const SSL_CIPHER *cipher_;
  ScopedEVP_AEAD_CTX ctx_;
  // fixed_nonce_ contains any bytes of the nonce that are fixed for all
  // records.
  uint8_t fixed_nonce_[12];
  uint8_t fixed_nonce_len_ = 0, variable_nonce_len_ = 0;
  // version_ is the wire version that should be used with this AEAD.
  uint16_t version_;
  // is_dtls_ is whether DTLS is being used with this AEAD.
  bool is_dtls_;
  // variable_nonce_included_in_record_ is true if the variable nonce
  // for a record is included as a prefix before the ciphertext.
  bool variable_nonce_included_in_record_ : 1;
  // random_variable_nonce_ is true if the variable nonce is
  // randomly generated, rather than derived from the sequence
  // number.
  bool random_variable_nonce_ : 1;
  // xor_fixed_nonce_ is true if the fixed nonce should be XOR'd into the
  // variable nonce rather than prepended.
  bool xor_fixed_nonce_ : 1;
  // omit_length_in_ad_ is true if the length should be omitted in the
  // AEAD's ad parameter.
  bool omit_length_in_ad_ : 1;
  // omit_ad_ is true if the AEAD's ad parameter should be omitted.
  bool omit_ad_ : 1;
  // ad_is_header_ is true if the AEAD's ad parameter is the record header.
  bool ad_is_header_ : 1;
};


// DTLS replay bitmap.

// DTLS1_BITMAP maintains a sliding window of 64 sequence numbers to detect
// replayed packets. It should be initialized by zeroing every field.
struct DTLS1_BITMAP {
  // map is a bit mask of the last 64 sequence numbers. Bit
  // |1<<i| corresponds to |max_seq_num - i|.
  uint64_t map = 0;
  // max_seq_num is the largest sequence number seen so far as a 64-bit
  // integer.
  uint64_t max_seq_num = 0;
};


// Record layer.

// ssl_record_sequence_update increments the sequence number in |seq|. It
// returns one on success and zero on wraparound.
int ssl_record_sequence_update(uint8_t *seq, size_t seq_len);

// ssl_record_prefix_len returns the length of the prefix before the ciphertext
// of a record for |ssl|.
//
// TODO(davidben): Expose this as part of public API once the high-level
// buffer-free APIs are available.
size_t ssl_record_prefix_len(const SSL *ssl);

enum ssl_open_record_t {
  ssl_open_record_success,
  ssl_open_record_discard,
  ssl_open_record_partial,
  ssl_open_record_close_notify,
  ssl_open_record_error,
};

// tls_open_record decrypts a record from |in| in-place.
//
// If the input did not contain a complete record, it returns
// |ssl_open_record_partial|. It sets |*out_consumed| to the total number of
// bytes necessary. It is guaranteed that a successful call to |tls_open_record|
// will consume at least that many bytes.
//
// Otherwise, it sets |*out_consumed| to the number of bytes of input
// consumed. Note that input may be consumed on all return codes if a record was
// decrypted.
//
// On success, it returns |ssl_open_record_success|. It sets |*out_type| to the
// record type and |*out| to the record body in |in|. Note that |*out| may be
// empty.
//
// If a record was successfully processed but should be discarded, it returns
// |ssl_open_record_discard|.
//
// If a record was successfully processed but is a close_notify, it returns
// |ssl_open_record_close_notify|.
//
// On failure or fatal alert, it returns |ssl_open_record_error| and sets
// |*out_alert| to an alert to emit, or zero if no alert should be emitted.
enum ssl_open_record_t tls_open_record(SSL *ssl, uint8_t *out_type,
                                       Span<uint8_t> *out, size_t *out_consumed,
                                       uint8_t *out_alert, Span<uint8_t> in);

// dtls_open_record implements |tls_open_record| for DTLS. It only returns
// |ssl_open_record_partial| if |in| was empty and sets |*out_consumed| to
// zero. The caller should read one packet and try again.
enum ssl_open_record_t dtls_open_record(SSL *ssl, uint8_t *out_type,
                                        Span<uint8_t> *out,
                                        size_t *out_consumed,
                                        uint8_t *out_alert, Span<uint8_t> in);

// ssl_seal_align_prefix_len returns the length of the prefix before the start
// of the bulk of the ciphertext when sealing a record with |ssl|. Callers may
// use this to align buffers.
//
// Note when TLS 1.0 CBC record-splitting is enabled, this includes the one byte
// record and is the offset into second record's ciphertext. Thus sealing a
// small record may result in a smaller output than this value.
//
// TODO(davidben): Is this alignment valuable? Record-splitting makes this a
// mess.
size_t ssl_seal_align_prefix_len(const SSL *ssl);

// tls_seal_record seals a new record of type |type| and body |in| and writes it
// to |out|. At most |max_out| bytes will be written. It returns one on success
// and zero on error. If enabled, |tls_seal_record| implements TLS 1.0 CBC 1/n-1
// record splitting and may write two records concatenated.
//
// For a large record, the bulk of the ciphertext will begin
// |ssl_seal_align_prefix_len| bytes into out. Aligning |out| appropriately may
// improve performance. It writes at most |in_len| + |SSL_max_seal_overhead|
// bytes to |out|.
//
// |in| and |out| may not alias.
int tls_seal_record(SSL *ssl, uint8_t *out, size_t *out_len, size_t max_out,
                    uint8_t type, const uint8_t *in, size_t in_len);

enum dtls1_use_epoch_t {
  dtls1_use_previous_epoch,
  dtls1_use_current_epoch,
};

// dtls_max_seal_overhead returns the maximum overhead, in bytes, of sealing a
// record.
size_t dtls_max_seal_overhead(const SSL *ssl, enum dtls1_use_epoch_t use_epoch);

// dtls_seal_prefix_len returns the number of bytes of prefix to reserve in
// front of the plaintext when sealing a record in-place.
size_t dtls_seal_prefix_len(const SSL *ssl, enum dtls1_use_epoch_t use_epoch);

// dtls_seal_record implements |tls_seal_record| for DTLS. |use_epoch| selects
// which epoch's cipher state to use. Unlike |tls_seal_record|, |in| and |out|
// may alias but, if they do, |in| must be exactly |dtls_seal_prefix_len| bytes
// ahead of |out|.
int dtls_seal_record(SSL *ssl, uint8_t *out, size_t *out_len, size_t max_out,
                     uint8_t type, const uint8_t *in, size_t in_len,
                     enum dtls1_use_epoch_t use_epoch);

// ssl_process_alert processes |in| as an alert and updates |ssl|'s shutdown
// state. It returns one of |ssl_open_record_discard|, |ssl_open_record_error|,
// |ssl_open_record_close_notify|, or |ssl_open_record_fatal_alert| as
// appropriate.
enum ssl_open_record_t ssl_process_alert(SSL *ssl, uint8_t *out_alert,
                                         Span<const uint8_t> in);


// Private key operations.

// ssl_has_private_key returns one if |cfg| has a private key configured and
// zero otherwise.
int ssl_has_private_key(const SSL_CONFIG *cfg);

// ssl_private_key_* perform the corresponding operation on
// |SSL_PRIVATE_KEY_METHOD|. If there is a custom private key configured, they
// call the corresponding function or |complete| depending on whether there is a
// pending operation. Otherwise, they implement the operation with
// |EVP_PKEY|.

enum ssl_private_key_result_t ssl_private_key_sign(
    SSL_HANDSHAKE *hs, uint8_t *out, size_t *out_len, size_t max_out,
    uint16_t sigalg, Span<const uint8_t> in);

enum ssl_private_key_result_t ssl_private_key_decrypt(SSL_HANDSHAKE *hs,
                                                      uint8_t *out,
                                                      size_t *out_len,
                                                      size_t max_out,
                                                      Span<const uint8_t> in);

// ssl_private_key_supports_signature_algorithm returns whether |hs|'s private
// key supports |sigalg|.
bool ssl_private_key_supports_signature_algorithm(SSL_HANDSHAKE *hs,
                                                 uint16_t sigalg);

// ssl_public_key_verify verifies that the |signature| is valid for the public
// key |pkey| and input |in|, using the signature algorithm |sigalg|.
bool ssl_public_key_verify(SSL *ssl, Span<const uint8_t> signature,
                           uint16_t sigalg, EVP_PKEY *pkey,
                           Span<const uint8_t> in);


// Key shares.

// SSLKeyShare abstracts over Diffie-Hellman-like key exchanges.
class SSLKeyShare {
 public:
  virtual ~SSLKeyShare() {}
  static constexpr bool kAllowUniquePtr = true;
  HAS_VIRTUAL_DESTRUCTOR

  // Create returns a SSLKeyShare instance for use with group |group_id| or
  // nullptr on error.
  static UniquePtr<SSLKeyShare> Create(uint16_t group_id);

  // Create deserializes an SSLKeyShare instance previously serialized by
  // |Serialize|.
  static UniquePtr<SSLKeyShare> Create(CBS *in);

  // GroupID returns the group ID.
  virtual uint16_t GroupID() const PURE_VIRTUAL;

  // Offer generates a keypair and writes the public value to
  // |out_public_key|. It returns true on success and false on error.
  virtual bool Offer(CBB *out_public_key) PURE_VIRTUAL;

  // Accept performs a key exchange against the |peer_key| generated by |offer|.
  // On success, it returns true, writes the public value to |out_public_key|,
  // and sets |*out_secret| the shared secret. On failure, it returns false and
  // sets |*out_alert| to an alert to send to the peer.
  //
  // The default implementation calls |Offer| and then |Finish|, assuming a key
  // exchange protocol where the peers are symmetric.
  virtual bool Accept(CBB *out_public_key, Array<uint8_t> *out_secret,
                      uint8_t *out_alert, Span<const uint8_t> peer_key);

  // Finish performs a key exchange against the |peer_key| generated by
  // |Accept|. On success, it returns true and sets |*out_secret| to the shared
  // secret. On failure, it returns zero and sets |*out_alert| to an alert to
  // send to the peer.
  virtual bool Finish(Array<uint8_t> *out_secret, uint8_t *out_alert,
                      Span<const uint8_t> peer_key) PURE_VIRTUAL;

  // Serialize writes the state of the key exchange to |out|, returning true if
  // successful and false otherwise.
  virtual bool Serialize(CBB *out) { return false; }

  // Deserialize initializes the state of the key exchange from |in|, returning
  // true if successful and false otherwise.  It is called by |Create|.
  virtual bool Deserialize(CBS *in) { return false; }
};

// ssl_nid_to_group_id looks up the group corresponding to |nid|. On success, it
// sets |*out_group_id| to the group ID and returns one. Otherwise, it returns
// zero.
int ssl_nid_to_group_id(uint16_t *out_group_id, int nid);

// ssl_name_to_group_id looks up the group corresponding to the |name| string
// of length |len|. On success, it sets |*out_group_id| to the group ID and
// returns one. Otherwise, it returns zero.
int ssl_name_to_group_id(uint16_t *out_group_id, const char *name, size_t len);


// Handshake messages.

struct SSLMessage {
  bool is_v2_hello;
  uint8_t type;
  CBS body;
  // raw is the entire serialized handshake message, including the TLS or DTLS
  // message header.
  CBS raw;
};

// SSL_MAX_HANDSHAKE_FLIGHT is the number of messages, including
// ChangeCipherSpec, in the longest handshake flight. Currently this is the
// client's second leg in a full handshake when client certificates, NPN, and
// Channel ID, are all enabled.
#define SSL_MAX_HANDSHAKE_FLIGHT 7

extern const uint8_t kHelloRetryRequest[SSL3_RANDOM_SIZE];
extern const uint8_t kDraftDowngradeRandom[8];

// ssl_max_handshake_message_len returns the maximum number of bytes permitted
// in a handshake message for |ssl|.
size_t ssl_max_handshake_message_len(const SSL *ssl);

// tls_can_accept_handshake_data returns whether |ssl| is able to accept more
// data into handshake buffer.
bool tls_can_accept_handshake_data(const SSL *ssl, uint8_t *out_alert);

// tls_has_unprocessed_handshake_data returns whether there is buffered
// handshake data that has not been consumed by |get_message|.
bool tls_has_unprocessed_handshake_data(const SSL *ssl);

// dtls_has_unprocessed_handshake_data behaves like
// |tls_has_unprocessed_handshake_data| for DTLS.
bool dtls_has_unprocessed_handshake_data(const SSL *ssl);

// tls_flush_pending_hs_data flushes any handshake plaintext data.
bool tls_flush_pending_hs_data(SSL *ssl);

struct DTLS_OUTGOING_MESSAGE {
  DTLS_OUTGOING_MESSAGE() {}
  DTLS_OUTGOING_MESSAGE(const DTLS_OUTGOING_MESSAGE &) = delete;
  DTLS_OUTGOING_MESSAGE &operator=(const DTLS_OUTGOING_MESSAGE &) = delete;
  ~DTLS_OUTGOING_MESSAGE() { Clear(); }

  void Clear();

  uint8_t *data = nullptr;
  uint32_t len = 0;
  uint16_t epoch = 0;
  bool is_ccs = false;
};

// dtls_clear_outgoing_messages releases all buffered outgoing messages.
void dtls_clear_outgoing_messages(SSL *ssl);


// Callbacks.

// ssl_do_info_callback calls |ssl|'s info callback, if set.
void ssl_do_info_callback(const SSL *ssl, int type, int value);

// ssl_do_msg_callback calls |ssl|'s message callback, if set.
void ssl_do_msg_callback(SSL *ssl, int is_write, int content_type,
                         Span<const uint8_t> in);


// Transport buffers.

class SSLBuffer {
 public:
  SSLBuffer() {}
  ~SSLBuffer() { Clear(); }

  SSLBuffer(const SSLBuffer &) = delete;
  SSLBuffer &operator=(const SSLBuffer &) = delete;

  uint8_t *data() { return buf_ + offset_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }
  size_t cap() const { return cap_; }

  Span<uint8_t> span() { return MakeSpan(data(), size()); }

  Span<uint8_t> remaining() {
    return MakeSpan(data() + size(), cap() - size());
  }

  // Clear releases the buffer.
  void Clear();

  // EnsureCap ensures the buffer has capacity at least |new_cap|, aligned such
  // that data written after |header_len| is aligned to a
  // |SSL3_ALIGN_PAYLOAD|-byte boundary. It returns true on success and false
  // on error.
  bool EnsureCap(size_t header_len, size_t new_cap);

  // DidWrite extends the buffer by |len|. The caller must have filled in to
  // this point.
  void DidWrite(size_t len);

  // Consume consumes |len| bytes from the front of the buffer.  The memory
  // consumed will remain valid until the next call to |DiscardConsumed| or
  // |Clear|.
  void Consume(size_t len);

  // DiscardConsumed discards the consumed bytes from the buffer. If the buffer
  // is now empty, it releases memory used by it.
  void DiscardConsumed();

 private:
  // buf_ is the memory allocated for this buffer.
  uint8_t *buf_ = nullptr;
  // offset_ is the offset into |buf_| which the buffer contents start at.
  uint16_t offset_ = 0;
  // size_ is the size of the buffer contents from |buf_| + |offset_|.
  uint16_t size_ = 0;
  // cap_ is how much memory beyond |buf_| + |offset_| is available.
  uint16_t cap_ = 0;
};

// ssl_read_buffer_extend_to extends the read buffer to the desired length. For
// TLS, it reads to the end of the buffer until the buffer is |len| bytes
// long. For DTLS, it reads a new packet and ignores |len|. It returns one on
// success, zero on EOF, and a negative number on error.
//
// It is an error to call |ssl_read_buffer_extend_to| in DTLS when the buffer is
// non-empty.
int ssl_read_buffer_extend_to(SSL *ssl, size_t len);

// ssl_handle_open_record handles the result of passing |ssl->s3->read_buffer|
// to a record-processing function. If |ret| is a success or if the caller
// should retry, it returns one and sets |*out_retry|. Otherwise, it returns <=
// 0.
int ssl_handle_open_record(SSL *ssl, bool *out_retry, ssl_open_record_t ret,
                           size_t consumed, uint8_t alert);

// ssl_write_buffer_flush flushes the write buffer to the transport. It returns
// one on success and <= 0 on error. For DTLS, whether or not the write
// succeeds, the write buffer will be cleared.
int ssl_write_buffer_flush(SSL *ssl);


// Certificate functions.

// ssl_has_certificate returns one if a certificate and private key are
// configured and zero otherwise.
int ssl_has_certificate(const SSL_CONFIG *cfg);

// ssl_parse_cert_chain parses a certificate list from |cbs| in the format used
// by a TLS Certificate message. On success, it advances |cbs| and returns
// true. Otherwise, it returns false and sets |*out_alert| to an alert to send
// to the peer.
//
// If the list is non-empty then |*out_chain| and |*out_pubkey| will be set to
// the certificate chain and the leaf certificate's public key
// respectively. Otherwise, both will be set to nullptr.
//
// If the list is non-empty and |out_leaf_sha256| is non-NULL, it writes the
// SHA-256 hash of the leaf to |out_leaf_sha256|.
bool ssl_parse_cert_chain(uint8_t *out_alert,
                          UniquePtr<STACK_OF(CRYPTO_BUFFER)> *out_chain,
                          UniquePtr<EVP_PKEY> *out_pubkey,
                          uint8_t *out_leaf_sha256, CBS *cbs,
                          CRYPTO_BUFFER_POOL *pool);

// ssl_add_cert_chain adds |hs->ssl|'s certificate chain to |cbb| in the format
// used by a TLS Certificate message. If there is no certificate chain, it emits
// an empty certificate list. It returns one on success and zero on error.
int ssl_add_cert_chain(SSL_HANDSHAKE *hs, CBB *cbb);

// ssl_cert_check_digital_signature_key_usage parses the DER-encoded, X.509
// certificate in |in| and returns one if doesn't specify a key usage or, if it
// does, if it includes digitalSignature. Otherwise it pushes to the error
// queue and returns zero.
int ssl_cert_check_digital_signature_key_usage(const CBS *in);

// ssl_cert_parse_pubkey extracts the public key from the DER-encoded, X.509
// certificate in |in|. It returns an allocated |EVP_PKEY| or else returns
// nullptr and pushes to the error queue.
UniquePtr<EVP_PKEY> ssl_cert_parse_pubkey(const CBS *in);

// ssl_parse_client_CA_list parses a CA list from |cbs| in the format used by a
// TLS CertificateRequest message. On success, it returns a newly-allocated
// |CRYPTO_BUFFER| list and advances |cbs|. Otherwise, it returns nullptr and
// sets |*out_alert| to an alert to send to the peer.
UniquePtr<STACK_OF(CRYPTO_BUFFER)> ssl_parse_client_CA_list(SSL *ssl,
                                                            uint8_t *out_alert,
                                                            CBS *cbs);

// ssl_has_client_CAs returns there are configured CAs.
bool ssl_has_client_CAs(const SSL_CONFIG *cfg);

// ssl_add_client_CA_list adds the configured CA list to |cbb| in the format
// used by a TLS CertificateRequest message. It returns one on success and zero
// on error.
int ssl_add_client_CA_list(SSL_HANDSHAKE *hs, CBB *cbb);

// ssl_check_leaf_certificate returns one if |pkey| and |leaf| are suitable as
// a server's leaf certificate for |hs|. Otherwise, it returns zero and pushes
// an error on the error queue.
int ssl_check_leaf_certificate(SSL_HANDSHAKE *hs, EVP_PKEY *pkey,
                               const CRYPTO_BUFFER *leaf);

// ssl_on_certificate_selected is called once the certificate has been selected.
// It finalizes the certificate and initializes |hs->local_pubkey|. It returns
// one on success and zero on error.
int ssl_on_certificate_selected(SSL_HANDSHAKE *hs);


// TLS 1.3 key derivation.

// tls13_init_key_schedule initializes the handshake hash and key derivation
// state, and incorporates the PSK. The cipher suite and PRF hash must have been
// selected at this point. It returns one on success and zero on error.
int tls13_init_key_schedule(SSL_HANDSHAKE *hs, const uint8_t *psk,
                            size_t psk_len);

// tls13_init_early_key_schedule initializes the handshake hash and key
// derivation state from the resumption secret and incorporates the PSK to
// derive the early secrets. It returns one on success and zero on error.
int tls13_init_early_key_schedule(SSL_HANDSHAKE *hs, const uint8_t *psk,
                                  size_t psk_len);

// tls13_advance_key_schedule incorporates |in| into the key schedule with
// HKDF-Extract. It returns one on success and zero on error.
int tls13_advance_key_schedule(SSL_HANDSHAKE *hs, const uint8_t *in,
                               size_t len);

// tls13_set_traffic_key sets the read or write traffic keys to
// |traffic_secret|. It returns one on success and zero on error.
int tls13_set_traffic_key(SSL *ssl, enum evp_aead_direction_t direction,
                          const uint8_t *traffic_secret,
                          size_t traffic_secret_len);

// tls13_derive_early_secrets derives the early traffic secret. It returns one
// on success and zero on error.
int tls13_derive_early_secrets(SSL_HANDSHAKE *hs);

// tls13_derive_handshake_secrets derives the handshake traffic secret. It
// returns one on success and zero on error.
int tls13_derive_handshake_secrets(SSL_HANDSHAKE *hs);

// tls13_rotate_traffic_key derives the next read or write traffic secret. It
// returns one on success and zero on error.
int tls13_rotate_traffic_key(SSL *ssl, enum evp_aead_direction_t direction);

// tls13_derive_application_secrets derives the initial application data traffic
// and exporter secrets based on the handshake transcripts and |master_secret|.
// It returns one on success and zero on error.
int tls13_derive_application_secrets(SSL_HANDSHAKE *hs);

// tls13_derive_resumption_secret derives the |resumption_secret|.
int tls13_derive_resumption_secret(SSL_HANDSHAKE *hs);

// tls13_export_keying_material provides an exporter interface to use the
// |exporter_secret|.
int tls13_export_keying_material(SSL *ssl, Span<uint8_t> out,
                                 Span<const uint8_t> secret,
                                 Span<const char> label,
                                 Span<const uint8_t> context);

// tls13_finished_mac calculates the MAC of the handshake transcript to verify
// the integrity of the Finished message, and stores the result in |out| and
// length in |out_len|. |is_server| is 1 if this is for the Server Finished and
// 0 for the Client Finished.
int tls13_finished_mac(SSL_HANDSHAKE *hs, uint8_t *out,
                       size_t *out_len, int is_server);

// tls13_derive_session_psk calculates the PSK for this session based on the
// resumption master secret and |nonce|. It returns true on success, and false
// on failure.
bool tls13_derive_session_psk(SSL_SESSION *session, Span<const uint8_t> nonce);

// tls13_write_psk_binder calculates the PSK binder value and replaces the last
// bytes of |msg| with the resulting value. It returns 1 on success, and 0 on
// failure.
int tls13_write_psk_binder(SSL_HANDSHAKE *hs, uint8_t *msg, size_t len);

// tls13_verify_psk_binder verifies that the handshake transcript, truncated
// up to the binders has a valid signature using the value of |session|'s
// resumption secret. It returns 1 on success, and 0 on failure.
int tls13_verify_psk_binder(SSL_HANDSHAKE *hs, SSL_SESSION *session,
                            const SSLMessage &msg, CBS *binders);


// Handshake functions.

enum ssl_hs_wait_t {
  ssl_hs_error,
  ssl_hs_ok,
  ssl_hs_read_server_hello,
  ssl_hs_read_message,
  ssl_hs_flush,
  ssl_hs_certificate_selection_pending,
  ssl_hs_handoff,
  ssl_hs_handback,
  ssl_hs_x509_lookup,
  ssl_hs_channel_id_lookup,
  ssl_hs_private_key_operation,
  ssl_hs_pending_session,
  ssl_hs_pending_ticket,
  ssl_hs_early_return,
  ssl_hs_early_data_rejected,
  ssl_hs_read_end_of_early_data,
  ssl_hs_read_change_cipher_spec,
  ssl_hs_certificate_verify,
};

enum ssl_grease_index_t {
  ssl_grease_cipher = 0,
  ssl_grease_group,
  ssl_grease_extension1,
  ssl_grease_extension2,
  ssl_grease_version,
  ssl_grease_ticket_extension,
  ssl_grease_last_index = ssl_grease_ticket_extension,
};

enum tls12_server_hs_state_t {
  state12_start_accept = 0,
  state12_read_client_hello,
  state12_select_certificate,
  state12_tls13,
  state12_select_parameters,
  state12_send_server_hello,
  state12_send_server_certificate,
  state12_send_server_key_exchange,
  state12_send_server_hello_done,
  state12_read_client_certificate,
  state12_verify_client_certificate,
  state12_read_client_key_exchange,
  state12_read_client_certificate_verify,
  state12_read_change_cipher_spec,
  state12_process_change_cipher_spec,
  state12_read_next_proto,
  state12_read_channel_id,
  state12_read_client_finished,
  state12_send_server_finished,
  state12_finish_server_handshake,
  state12_done,
};

// handback_t lists the points in the state machine where a handback can occur.
// These are the different points at which key material is no longer needed.
enum handback_t {
  handback_after_session_resumption,
  handback_after_ecdhe,
  handback_after_handshake,
};

struct SSL_HANDSHAKE {
  explicit SSL_HANDSHAKE(SSL *ssl);
  ~SSL_HANDSHAKE();
  static constexpr bool kAllowUniquePtr = true;

  // ssl is a non-owning pointer to the parent |SSL| object.
  SSL *ssl;

  // config is a non-owning pointer to the handshake configuration.
  SSL_CONFIG *config;

  // wait contains the operation the handshake is currently blocking on or
  // |ssl_hs_ok| if none.
  enum ssl_hs_wait_t wait = ssl_hs_ok;

  // state is the internal state for the TLS 1.2 and below handshake. Its
  // values depend on |do_handshake| but the starting state is always zero.
  int state = 0;

  // tls13_state is the internal state for the TLS 1.3 handshake. Its values
  // depend on |do_handshake| but the starting state is always zero.
  int tls13_state = 0;

  // min_version is the minimum accepted protocol version, taking account both
  // |SSL_OP_NO_*| and |SSL_CTX_set_min_proto_version| APIs.
  uint16_t min_version = 0;

  // max_version is the maximum accepted protocol version, taking account both
  // |SSL_OP_NO_*| and |SSL_CTX_set_max_proto_version| APIs.
  uint16_t max_version = 0;

  size_t hash_len = 0;
  uint8_t secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t early_traffic_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t client_handshake_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t server_handshake_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t client_traffic_secret_0[EVP_MAX_MD_SIZE] = {0};
  uint8_t server_traffic_secret_0[EVP_MAX_MD_SIZE] = {0};
  uint8_t expected_client_finished[EVP_MAX_MD_SIZE] = {0};

  union {
    // sent is a bitset where the bits correspond to elements of kExtensions
    // in t1_lib.c. Each bit is set if that extension was sent in a
    // ClientHello. It's not used by servers.
    uint32_t sent = 0;
    // received is a bitset, like |sent|, but is used by servers to record
    // which extensions were received from a client.
    uint32_t received;
  } extensions;

  // retry_group is the group ID selected by the server in HelloRetryRequest in
  // TLS 1.3.
  uint16_t retry_group = 0;

  // error, if |wait| is |ssl_hs_error|, is the error the handshake failed on.
  UniquePtr<ERR_SAVE_STATE> error;

  // key_share is the current key exchange instance.
  UniquePtr<SSLKeyShare> key_share;

  // transcript is the current handshake transcript.
  SSLTranscript transcript;

  // cookie is the value of the cookie received from the server, if any.
  Array<uint8_t> cookie;

  // key_share_bytes is the value of the previously sent KeyShare extension by
  // the client in TLS 1.3.
  Array<uint8_t> key_share_bytes;

  // ecdh_public_key, for servers, is the key share to be sent to the client in
  // TLS 1.3.
  Array<uint8_t> ecdh_public_key;

  // peer_sigalgs are the signature algorithms that the peer supports. These are
  // taken from the contents of the signature algorithms extension for a server
  // or from the CertificateRequest for a client.
  Array<uint16_t> peer_sigalgs;

  // peer_supported_group_list contains the supported group IDs advertised by
  // the peer. This is only set on the server's end. The server does not
  // advertise this extension to the client.
  Array<uint16_t> peer_supported_group_list;

  // peer_key is the peer's ECDH key for a TLS 1.2 client.
  Array<uint8_t> peer_key;

  // negotiated_token_binding_version is used by a server to store the
  // on-the-wire encoding of the Token Binding protocol version to advertise in
  // the ServerHello/EncryptedExtensions if the Token Binding extension is to be
  // sent.
  uint16_t negotiated_token_binding_version;

  // cert_compression_alg_id, for a server, contains the negotiated certificate
  // compression algorithm for this client. It is only valid if
  // |cert_compression_negotiated| is true.
  uint16_t cert_compression_alg_id;

  // server_params, in a TLS 1.2 server, stores the ServerKeyExchange
  // parameters. It has client and server randoms prepended for signing
  // convenience.
  Array<uint8_t> server_params;

  // peer_psk_identity_hint, on the client, is the psk_identity_hint sent by the
  // server when using a TLS 1.2 PSK key exchange.
  UniquePtr<char> peer_psk_identity_hint;

  // ca_names, on the client, contains the list of CAs received in a
  // CertificateRequest message.
  UniquePtr<STACK_OF(CRYPTO_BUFFER)> ca_names;

  // cached_x509_ca_names contains a cache of parsed versions of the elements of
  // |ca_names|. This pointer is left non-owning so only
  // |ssl_crypto_x509_method| needs to link against crypto/x509.
  STACK_OF(X509_NAME) *cached_x509_ca_names = nullptr;

  // certificate_types, on the client, contains the set of certificate types
  // received in a CertificateRequest message.
  Array<uint8_t> certificate_types;

  // local_pubkey is the public key we are authenticating as.
  UniquePtr<EVP_PKEY> local_pubkey;

  // peer_pubkey is the public key parsed from the peer's leaf certificate.
  UniquePtr<EVP_PKEY> peer_pubkey;

  // new_session is the new mutable session being established by the current
  // handshake. It should not be cached.
  UniquePtr<SSL_SESSION> new_session;

  // early_session is the session corresponding to the current 0-RTT state on
  // the client if |in_early_data| is true.
  UniquePtr<SSL_SESSION> early_session;

  // new_cipher is the cipher being negotiated in this handshake.
  const SSL_CIPHER *new_cipher = nullptr;

  // key_block is the record-layer key block for TLS 1.2 and earlier.
  Array<uint8_t> key_block;

  // scts_requested is true if the SCT extension is in the ClientHello.
  bool scts_requested:1;

  // needs_psk_binder is true if the ClientHello has a placeholder PSK binder to
  // be filled in.
  bool needs_psk_binder:1;

  bool received_hello_retry_request:1;
  bool sent_hello_retry_request:1;

  // handshake_finalized is true once the handshake has completed, at which
  // point accessors should use the established state.
  bool handshake_finalized:1;

  // accept_psk_mode stores whether the client's PSK mode is compatible with our
  // preferences.
  bool accept_psk_mode:1;

  // cert_request is true if a client certificate was requested.
  bool cert_request:1;

  // certificate_status_expected is true if OCSP stapling was negotiated and the
  // server is expected to send a CertificateStatus message. (This is used on
  // both the client and server sides.)
  bool certificate_status_expected:1;

  // ocsp_stapling_requested is true if a client requested OCSP stapling.
  bool ocsp_stapling_requested:1;

  // should_ack_sni is used by a server and indicates that the SNI extension
  // should be echoed in the ServerHello.
  bool should_ack_sni:1;

  // in_false_start is true if there is a pending client handshake in False
  // Start. The client may write data at this point.
  bool in_false_start:1;

  // in_early_data is true if there is a pending handshake that has progressed
  // enough to send and receive early data.
  bool in_early_data:1;

  // early_data_offered is true if the client sent the early_data extension.
  bool early_data_offered:1;

  // can_early_read is true if application data may be read at this point in the
  // handshake.
  bool can_early_read:1;

  // can_early_write is true if application data may be written at this point in
  // the handshake.
  bool can_early_write:1;

  // next_proto_neg_seen is one of NPN was negotiated.
  bool next_proto_neg_seen:1;

  // ticket_expected is true if a TLS 1.2 NewSessionTicket message is to be sent
  // or received.
  bool ticket_expected:1;

  // extended_master_secret is true if the extended master secret extension is
  // negotiated in this handshake.
  bool extended_master_secret:1;

  // pending_private_key_op is true if there is a pending private key operation
  // in progress.
  bool pending_private_key_op:1;

  // grease_seeded is true if |grease_seed| has been initialized.
  bool grease_seeded:1;

  // handback indicates that a server should pause the handshake after
  // finishing operations that require private key material, in such a way that
  // |SSL_get_error| returns |SSL_HANDBACK|.  It is set by |SSL_apply_handoff|.
  bool handback:1;

  // cert_compression_negotiated is true iff |cert_compression_alg_id| is valid.
  bool cert_compression_negotiated:1;

  // client_version is the value sent or received in the ClientHello version.
  uint16_t client_version = 0;

  // early_data_read is the amount of early data that has been read by the
  // record layer.
  uint16_t early_data_read = 0;

  // early_data_written is the amount of early data that has been written by the
  // record layer.
  uint16_t early_data_written = 0;

  // session_id is the session ID in the ClientHello, used for the experimental
  // TLS 1.3 variant.
  uint8_t session_id[SSL_MAX_SSL_SESSION_ID_LENGTH] = {0};
  uint8_t session_id_len = 0;

  // grease_seed is the entropy for GREASE values. It is valid if
  // |grease_seeded| is true.
  uint8_t grease_seed[ssl_grease_last_index + 1] = {0};

  // dummy_pq_padding_len, in a server, is the length of the extension that
  // should be echoed in a ServerHello, or zero if no extension should be
  // echoed.
  uint16_t dummy_pq_padding_len = 0;
};

UniquePtr<SSL_HANDSHAKE> ssl_handshake_new(SSL *ssl);

// ssl_check_message_type checks if |msg| has type |type|. If so it returns
// one. Otherwise, it sends an alert and returns zero.
bool ssl_check_message_type(SSL *ssl, const SSLMessage &msg, int type);

// ssl_run_handshake runs the TLS handshake. It returns one on success and <= 0
// on error. It sets |out_early_return| to one if we've completed the handshake
// early.
int ssl_run_handshake(SSL_HANDSHAKE *hs, bool *out_early_return);

// The following are implementations of |do_handshake| for the client and
// server.
enum ssl_hs_wait_t ssl_client_handshake(SSL_HANDSHAKE *hs);
enum ssl_hs_wait_t ssl_server_handshake(SSL_HANDSHAKE *hs);
enum ssl_hs_wait_t tls13_client_handshake(SSL_HANDSHAKE *hs);
enum ssl_hs_wait_t tls13_server_handshake(SSL_HANDSHAKE *hs);

// The following functions return human-readable representations of the TLS
// handshake states for debugging.
const char *ssl_client_handshake_state(SSL_HANDSHAKE *hs);
const char *ssl_server_handshake_state(SSL_HANDSHAKE *hs);
const char *tls13_client_handshake_state(SSL_HANDSHAKE *hs);
const char *tls13_server_handshake_state(SSL_HANDSHAKE *hs);

// tls13_post_handshake processes a post-handshake message. It returns one on
// success and zero on failure.
int tls13_post_handshake(SSL *ssl, const SSLMessage &msg);

int tls13_process_certificate(SSL_HANDSHAKE *hs, const SSLMessage &msg,
                              int allow_anonymous);
int tls13_process_certificate_verify(SSL_HANDSHAKE *hs, const SSLMessage &msg);

// tls13_process_finished processes |msg| as a Finished message from the
// peer. If |use_saved_value| is one, the verify_data is compared against
// |hs->expected_client_finished| rather than computed fresh.
int tls13_process_finished(SSL_HANDSHAKE *hs, const SSLMessage &msg,
                           int use_saved_value);

int tls13_add_certificate(SSL_HANDSHAKE *hs);

// tls13_add_certificate_verify adds a TLS 1.3 CertificateVerify message to the
// handshake. If it returns |ssl_private_key_retry|, it should be called again
// to retry when the signing operation is completed.
enum ssl_private_key_result_t tls13_add_certificate_verify(SSL_HANDSHAKE *hs);

int tls13_add_finished(SSL_HANDSHAKE *hs);
int tls13_process_new_session_ticket(SSL *ssl, const SSLMessage &msg);

bool ssl_ext_key_share_parse_serverhello(SSL_HANDSHAKE *hs,
                                         Array<uint8_t> *out_secret,
                                         uint8_t *out_alert, CBS *contents);
bool ssl_ext_key_share_parse_clienthello(SSL_HANDSHAKE *hs, bool *out_found,
                                         Array<uint8_t> *out_secret,
                                         uint8_t *out_alert, CBS *contents);
bool ssl_ext_key_share_add_serverhello(SSL_HANDSHAKE *hs, CBB *out);

bool ssl_ext_pre_shared_key_parse_serverhello(SSL_HANDSHAKE *hs,
                                              uint8_t *out_alert,
                                              CBS *contents);
bool ssl_ext_pre_shared_key_parse_clienthello(
    SSL_HANDSHAKE *hs, CBS *out_ticket, CBS *out_binders,
    uint32_t *out_obfuscated_ticket_age, uint8_t *out_alert, CBS *contents);
bool ssl_ext_pre_shared_key_add_serverhello(SSL_HANDSHAKE *hs, CBB *out);

// ssl_is_sct_list_valid does a shallow parse of the SCT list in |contents| and
// returns whether it's valid.
bool ssl_is_sct_list_valid(const CBS *contents);

int ssl_write_client_hello(SSL_HANDSHAKE *hs);

enum ssl_cert_verify_context_t {
  ssl_cert_verify_server,
  ssl_cert_verify_client,
  ssl_cert_verify_channel_id,
};

// tls13_get_cert_verify_signature_input generates the message to be signed for
// TLS 1.3's CertificateVerify message. |cert_verify_context| determines the
// type of signature. It sets |*out| to a newly allocated buffer containing the
// result. This function returns true on success and false on failure.
bool tls13_get_cert_verify_signature_input(
    SSL_HANDSHAKE *hs, Array<uint8_t> *out,
    enum ssl_cert_verify_context_t cert_verify_context);

// ssl_is_alpn_protocol_allowed returns whether |protocol| is a valid server
// selection for |hs->ssl|'s client preferences.
bool ssl_is_alpn_protocol_allowed(const SSL_HANDSHAKE *hs,
                                  Span<const uint8_t> protocol);

// ssl_negotiate_alpn negotiates the ALPN extension, if applicable. It returns
// true on successful negotiation or if nothing was negotiated. It returns false
// and sets |*out_alert| to an alert on error.
bool ssl_negotiate_alpn(SSL_HANDSHAKE *hs, uint8_t *out_alert,
                        const SSL_CLIENT_HELLO *client_hello);

struct SSL_EXTENSION_TYPE {
  uint16_t type;
  bool *out_present;
  CBS *out_data;
};

// ssl_parse_extensions parses a TLS extensions block out of |cbs| and advances
// it. It writes the parsed extensions to pointers denoted by |ext_types|. On
// success, it fills in the |out_present| and |out_data| fields and returns one.
// Otherwise, it sets |*out_alert| to an alert to send and returns zero. Unknown
// extensions are rejected unless |ignore_unknown| is 1.
int ssl_parse_extensions(const CBS *cbs, uint8_t *out_alert,
                         const SSL_EXTENSION_TYPE *ext_types,
                         size_t num_ext_types, int ignore_unknown);

// ssl_verify_peer_cert verifies the peer certificate for |hs|.
enum ssl_verify_result_t ssl_verify_peer_cert(SSL_HANDSHAKE *hs);

enum ssl_hs_wait_t ssl_get_finished(SSL_HANDSHAKE *hs);
bool ssl_send_finished(SSL_HANDSHAKE *hs);
bool ssl_output_cert_chain(SSL_HANDSHAKE *hs);

// SSLKEYLOGFILE functions.

// ssl_log_secret logs |secret| with label |label|, if logging is enabled for
// |ssl|. It returns one on success and zero on failure.
int ssl_log_secret(const SSL *ssl, const char *label, const uint8_t *secret,
                   size_t secret_len);


// ClientHello functions.

bool ssl_client_hello_init(SSL *ssl, SSL_CLIENT_HELLO *out,
                           const SSLMessage &msg);

bool ssl_client_hello_get_extension(const SSL_CLIENT_HELLO *client_hello,
                                    CBS *out, uint16_t extension_type);

bool ssl_client_cipher_list_contains_cipher(
    const SSL_CLIENT_HELLO *client_hello, uint16_t id);


// GREASE.

// ssl_get_grease_value returns a GREASE value for |hs|. For a given
// connection, the values for each index will be deterministic. This allows the
// same ClientHello be sent twice for a HelloRetryRequest or the same group be
// advertised in both supported_groups and key_shares.
uint16_t ssl_get_grease_value(SSL_HANDSHAKE *hs, enum ssl_grease_index_t index);


// Signature algorithms.

// tls1_parse_peer_sigalgs parses |sigalgs| as the list of peer signature
// algorithms and saves them on |hs|. It returns true on success and false on
// error.
bool tls1_parse_peer_sigalgs(SSL_HANDSHAKE *hs, const CBS *sigalgs);

// tls1_get_legacy_signature_algorithm sets |*out| to the signature algorithm
// that should be used with |pkey| in TLS 1.1 and earlier. It returns true on
// success and false if |pkey| may not be used at those versions.
bool tls1_get_legacy_signature_algorithm(uint16_t *out, const EVP_PKEY *pkey);

// tls1_choose_signature_algorithm sets |*out| to a signature algorithm for use
// with |hs|'s private key based on the peer's preferences and the algorithms
// supported. It returns true on success and false on error.
bool tls1_choose_signature_algorithm(SSL_HANDSHAKE *hs, uint16_t *out);

// tls12_add_verify_sigalgs adds the signature algorithms acceptable for the
// peer signature to |out|. It returns true on success and false on error. If
// |for_certs| is true, the potentially more restrictive list of algorithms for
// certificates is used. Otherwise, the online signature one is used.
bool tls12_add_verify_sigalgs(const SSL *ssl, CBB *out, bool for_certs);

// tls12_check_peer_sigalg checks if |sigalg| is acceptable for the peer
// signature. It returns true on success and false on error, setting
// |*out_alert| to an alert to send.
bool tls12_check_peer_sigalg(const SSL *ssl, uint8_t *out_alert,
                             uint16_t sigalg);

// tls12_has_different_verify_sigalgs_for_certs returns whether |ssl| has a
// different, more restrictive, list of signature algorithms acceptable for the
// certificate than the online signature.
bool tls12_has_different_verify_sigalgs_for_certs(const SSL *ssl);


// Underdocumented functions.
//
// Functions below here haven't been touched up and may be underdocumented.

#define TLSEXT_CHANNEL_ID_SIZE 128

// From RFC4492, used in encoding the curve type in ECParameters
#define NAMED_CURVE_TYPE 3

struct CERT {
  static constexpr bool kAllowUniquePtr = true;

  explicit CERT(const SSL_X509_METHOD *x509_method);
  ~CERT();

  UniquePtr<EVP_PKEY> privatekey;

  // chain contains the certificate chain, with the leaf at the beginning. The
  // first element of |chain| may be NULL to indicate that the leaf certificate
  // has not yet been set.
  //   If |chain| != NULL -> len(chain) >= 1
  //   If |chain[0]| == NULL -> len(chain) >= 2.
  //   |chain[1..]| != NULL
  UniquePtr<STACK_OF(CRYPTO_BUFFER)> chain;

  // x509_chain may contain a parsed copy of |chain[1..]|. This is only used as
  // a cache in order to implement “get0” functions that return a non-owning
  // pointer to the certificate chain.
  STACK_OF(X509) *x509_chain = nullptr;

  // x509_leaf may contain a parsed copy of the first element of |chain|. This
  // is only used as a cache in order to implement “get0” functions that return
  // a non-owning pointer to the certificate chain.
  X509 *x509_leaf = nullptr;

  // x509_stash contains the last |X509| object append to the chain. This is a
  // workaround for some third-party code that continue to use an |X509| object
  // even after passing ownership with an “add0” function.
  X509 *x509_stash = nullptr;

  // key_method, if non-NULL, is a set of callbacks to call for private key
  // operations.
  const SSL_PRIVATE_KEY_METHOD *key_method = nullptr;

  // x509_method contains pointers to functions that might deal with |X509|
  // compatibility, or might be a no-op, depending on the application.
  const SSL_X509_METHOD *x509_method = nullptr;

  // sigalgs, if non-empty, is the set of signature algorithms supported by
  // |privatekey| in decreasing order of preference.
  Array<uint16_t> sigalgs;

  // Certificate setup callback: if set is called whenever a
  // certificate may be required (client or server). the callback
  // can then examine any appropriate parameters and setup any
  // certificates required. This allows advanced applications
  // to select certificates on the fly: for example based on
  // supported signature algorithms or curves.
  int (*cert_cb)(SSL *ssl, void *arg) = nullptr;
  void *cert_cb_arg = nullptr;

  // Optional X509_STORE for certificate validation. If NULL the parent SSL_CTX
  // store is used instead.
  X509_STORE *verify_store = nullptr;

  // Signed certificate timestamp list to be sent to the client, if requested
  UniquePtr<CRYPTO_BUFFER> signed_cert_timestamp_list;

  // OCSP response to be sent to the client, if requested.
  UniquePtr<CRYPTO_BUFFER> ocsp_response;

  // sid_ctx partitions the session space within a shared session cache or
  // ticket key. Only sessions with a matching value will be accepted.
  uint8_t sid_ctx_length = 0;
  uint8_t sid_ctx[SSL_MAX_SID_CTX_LENGTH] = {0};
};

// |SSL_PROTOCOL_METHOD| abstracts between TLS and DTLS.
struct SSL_PROTOCOL_METHOD {
  bool is_dtls;
  bool (*ssl_new)(SSL *ssl);
  void (*ssl_free)(SSL *ssl);
  // get_message sets |*out| to the current handshake message and returns true
  // if one has been received. It returns false if more input is needed.
  bool (*get_message)(SSL *ssl, SSLMessage *out);
  // next_message is called to release the current handshake message.
  void (*next_message)(SSL *ssl);
  // Use the |ssl_open_handshake| wrapper.
  ssl_open_record_t (*open_handshake)(SSL *ssl, size_t *out_consumed,
                                      uint8_t *out_alert, Span<uint8_t> in);
  // Use the |ssl_open_change_cipher_spec| wrapper.
  ssl_open_record_t (*open_change_cipher_spec)(SSL *ssl, size_t *out_consumed,
                                               uint8_t *out_alert,
                                               Span<uint8_t> in);
  // Use the |ssl_open_app_data| wrapper.
  ssl_open_record_t (*open_app_data)(SSL *ssl, Span<uint8_t> *out,
                                     size_t *out_consumed, uint8_t *out_alert,
                                     Span<uint8_t> in);
  int (*write_app_data)(SSL *ssl, bool *out_needs_handshake, const uint8_t *buf,
                        int len);
  int (*dispatch_alert)(SSL *ssl);
  // init_message begins a new handshake message of type |type|. |cbb| is the
  // root CBB to be passed into |finish_message|. |*body| is set to a child CBB
  // the caller should write to. It returns true on success and false on error.
  bool (*init_message)(SSL *ssl, CBB *cbb, CBB *body, uint8_t type);
  // finish_message finishes a handshake message. It sets |*out_msg| to the
  // serialized message. It returns true on success and false on error.
  bool (*finish_message)(SSL *ssl, CBB *cbb, bssl::Array<uint8_t> *out_msg);
  // add_message adds a handshake message to the pending flight. It returns
  // true on success and false on error.
  bool (*add_message)(SSL *ssl, bssl::Array<uint8_t> msg);
  // add_change_cipher_spec adds a ChangeCipherSpec record to the pending
  // flight. It returns true on success and false on error.
  bool (*add_change_cipher_spec)(SSL *ssl);
  // add_alert adds an alert to the pending flight. It returns true on success
  // and false on error.
  bool (*add_alert)(SSL *ssl, uint8_t level, uint8_t desc);
  // flush_flight flushes the pending flight to the transport. It returns one on
  // success and <= 0 on error.
  int (*flush_flight)(SSL *ssl);
  // on_handshake_complete is called when the handshake is complete.
  void (*on_handshake_complete)(SSL *ssl);
  // set_read_state sets |ssl|'s read cipher state to |aead_ctx|. It returns
  // true on success and false if changing the read state is forbidden at this
  // point.
  bool (*set_read_state)(SSL *ssl, UniquePtr<SSLAEADContext> aead_ctx);
  // set_write_state sets |ssl|'s write cipher state to |aead_ctx|. It returns
  // true on success and false if changing the write state is forbidden at this
  // point.
  bool (*set_write_state)(SSL *ssl, UniquePtr<SSLAEADContext> aead_ctx);
};

// The following wrappers call |open_*| but handle |read_shutdown| correctly.

// ssl_open_handshake processes a record from |in| for reading a handshake
// message.
ssl_open_record_t ssl_open_handshake(SSL *ssl, size_t *out_consumed,
                                     uint8_t *out_alert, Span<uint8_t> in);

// ssl_open_change_cipher_spec processes a record from |in| for reading a
// ChangeCipherSpec.
ssl_open_record_t ssl_open_change_cipher_spec(SSL *ssl, size_t *out_consumed,
                                              uint8_t *out_alert,
                                              Span<uint8_t> in);

// ssl_open_app_data processes a record from |in| for reading application data.
// On success, it returns |ssl_open_record_success| and sets |*out| to the
// input. If it encounters a post-handshake message, it returns
// |ssl_open_record_discard|. The caller should then retry, after processing any
// messages received with |get_message|.
ssl_open_record_t ssl_open_app_data(SSL *ssl, Span<uint8_t> *out,
                                    size_t *out_consumed, uint8_t *out_alert,
                                    Span<uint8_t> in);

struct SSL_X509_METHOD {
  // check_client_CA_list returns one if |names| is a good list of X.509
  // distinguished names and zero otherwise. This is used to ensure that we can
  // reject unparsable values at handshake time when using crypto/x509.
  int (*check_client_CA_list)(STACK_OF(CRYPTO_BUFFER) *names);

  // cert_clear frees and NULLs all X509 certificate-related state.
  void (*cert_clear)(CERT *cert);
  // cert_free frees all X509-related state.
  void (*cert_free)(CERT *cert);
  // cert_flush_cached_chain drops any cached |X509|-based certificate chain
  // from |cert|.
  // cert_dup duplicates any needed fields from |cert| to |new_cert|.
  void (*cert_dup)(CERT *new_cert, const CERT *cert);
  void (*cert_flush_cached_chain)(CERT *cert);
  // cert_flush_cached_chain drops any cached |X509|-based leaf certificate
  // from |cert|.
  void (*cert_flush_cached_leaf)(CERT *cert);

  // session_cache_objects fills out |sess->x509_peer| and |sess->x509_chain|
  // from |sess->certs| and erases |sess->x509_chain_without_leaf|. It returns
  // one on success or zero on error.
  int (*session_cache_objects)(SSL_SESSION *session);
  // session_dup duplicates any needed fields from |session| to |new_session|.
  // It returns one on success or zero on error.
  int (*session_dup)(SSL_SESSION *new_session, const SSL_SESSION *session);
  // session_clear frees any X509-related state from |session|.
  void (*session_clear)(SSL_SESSION *session);
  // session_verify_cert_chain verifies the certificate chain in |session|,
  // sets |session->verify_result| and returns one on success or zero on
  // error.
  int (*session_verify_cert_chain)(SSL_SESSION *session, SSL_HANDSHAKE *ssl,
                                   uint8_t *out_alert);

  // hs_flush_cached_ca_names drops any cached |X509_NAME|s from |hs|.
  void (*hs_flush_cached_ca_names)(SSL_HANDSHAKE *hs);
  // ssl_new does any neccessary initialisation of |hs|. It returns one on
  // success or zero on error.
  int (*ssl_new)(SSL_HANDSHAKE *hs);
  // ssl_free frees anything created by |ssl_new|.
  void (*ssl_config_free)(SSL_CONFIG *cfg);
  // ssl_flush_cached_client_CA drops any cached |X509_NAME|s from |ssl|.
  void (*ssl_flush_cached_client_CA)(SSL_CONFIG *cfg);
  // ssl_auto_chain_if_needed runs the deprecated auto-chaining logic if
  // necessary. On success, it updates |ssl|'s certificate configuration as
  // needed and returns one. Otherwise, it returns zero.
  int (*ssl_auto_chain_if_needed)(SSL_HANDSHAKE *hs);
  // ssl_ctx_new does any neccessary initialisation of |ctx|. It returns one on
  // success or zero on error.
  int (*ssl_ctx_new)(SSL_CTX *ctx);
  // ssl_ctx_free frees anything created by |ssl_ctx_new|.
  void (*ssl_ctx_free)(SSL_CTX *ctx);
  // ssl_ctx_flush_cached_client_CA drops any cached |X509_NAME|s from |ctx|.
  void (*ssl_ctx_flush_cached_client_CA)(SSL_CTX *ssl);
};

// ssl_crypto_x509_method provides the |SSL_X509_METHOD| functions using
// crypto/x509.
extern const SSL_X509_METHOD ssl_crypto_x509_method;

// ssl_noop_x509_method provides the |SSL_X509_METHOD| functions that avoid
// crypto/x509.
extern const SSL_X509_METHOD ssl_noop_x509_method;

struct TicketKey {
  static constexpr bool kAllowUniquePtr = true;

  uint8_t name[SSL_TICKET_KEY_NAME_LEN] = {0};
  uint8_t hmac_key[16] = {0};
  uint8_t aes_key[16] = {0};
  // next_rotation_tv_sec is the time (in seconds from the epoch) when the
  // current key should be superseded by a new key, or the time when a previous
  // key should be dropped. If zero, then the key should not be automatically
  // rotated.
  uint64_t next_rotation_tv_sec = 0;
};

struct CertCompressionAlg {
  static constexpr bool kAllowUniquePtr = true;

  ssl_cert_compression_func_t compress = nullptr;
  ssl_cert_decompression_func_t decompress = nullptr;
  uint16_t alg_id = 0;
};

}  // namespace bssl

DECLARE_LHASH_OF(SSL_SESSION)

DEFINE_NAMED_STACK_OF(CertCompressionAlg, bssl::CertCompressionAlg);

namespace bssl {

// An ssl_shutdown_t describes the shutdown state of one end of the connection,
// whether it is alive or has been shutdown via close_notify or fatal alert.
enum ssl_shutdown_t {
  ssl_shutdown_none = 0,
  ssl_shutdown_close_notify = 1,
  ssl_shutdown_error = 2,
};

struct SSL3_STATE {
  static constexpr bool kAllowUniquePtr = true;

  SSL3_STATE();
  ~SSL3_STATE();

  uint8_t read_sequence[8] = {0};
  uint8_t write_sequence[8] = {0};

  uint8_t server_random[SSL3_RANDOM_SIZE] = {0};
  uint8_t client_random[SSL3_RANDOM_SIZE] = {0};

  // read_buffer holds data from the transport to be processed.
  SSLBuffer read_buffer;
  // write_buffer holds data to be written to the transport.
  SSLBuffer write_buffer;

  // pending_app_data is the unconsumed application data. It points into
  // |read_buffer|.
  Span<uint8_t> pending_app_data;

  // partial write - check the numbers match
  unsigned int wnum = 0;  // number of bytes sent so far
  int wpend_tot = 0;      // number bytes written
  int wpend_type = 0;
  int wpend_ret = 0;  // number of bytes submitted
  const uint8_t *wpend_buf = nullptr;

  // read_shutdown is the shutdown state for the read half of the connection.
  enum ssl_shutdown_t read_shutdown = ssl_shutdown_none;

  // write_shutdown is the shutdown state for the write half of the connection.
  enum ssl_shutdown_t write_shutdown = ssl_shutdown_none;

  // read_error, if |read_shutdown| is |ssl_shutdown_error|, is the error for
  // the receive half of the connection.
  UniquePtr<ERR_SAVE_STATE> read_error;

  int alert_dispatch = 0;

  int total_renegotiations = 0;

  // This holds a variable that indicates what we were doing when a 0 or -1 is
  // returned.  This is needed for non-blocking IO so we know what request
  // needs re-doing when in SSL_accept or SSL_connect
  int rwstate = SSL_NOTHING;

  // early_data_skipped is the amount of early data that has been skipped by the
  // record layer.
  uint16_t early_data_skipped = 0;

  // empty_record_count is the number of consecutive empty records received.
  uint8_t empty_record_count = 0;

  // warning_alert_count is the number of consecutive warning alerts
  // received.
  uint8_t warning_alert_count = 0;

  // key_update_count is the number of consecutive KeyUpdates received.
  uint8_t key_update_count = 0;

  // The negotiated Token Binding key parameter. Only valid if
  // |token_binding_negotiated| is set.
  uint8_t negotiated_token_binding_param = 0;

  // skip_early_data instructs the record layer to skip unexpected early data
  // messages when 0RTT is rejected.
  bool skip_early_data:1;

  // have_version is true if the connection's final version is known. Otherwise
  // the version has not been negotiated yet.
  bool have_version:1;

  // v2_hello_done is true if the peer's V2ClientHello, if any, has been handled
  // and future messages should use the record layer.
  bool v2_hello_done:1;

  // is_v2_hello is true if the current handshake message was derived from a
  // V2ClientHello rather than received from the peer directly.
  bool is_v2_hello:1;

  // has_message is true if the current handshake message has been returned
  // at least once by |get_message| and false otherwise.
  bool has_message:1;

  // initial_handshake_complete is true if the initial handshake has
  // completed.
  bool initial_handshake_complete:1;

  // session_reused indicates whether a session was resumed.
  bool session_reused:1;

  bool send_connection_binding:1;

  // In a client, this means that the server supported Channel ID and that a
  // Channel ID was sent. In a server it means that we echoed support for
  // Channel IDs and that |channel_id| will be valid after the handshake.
  bool channel_id_valid:1;

  // key_update_pending is true if we have a KeyUpdate acknowledgment
  // outstanding.
  bool key_update_pending:1;

  // wpend_pending is true if we have a pending write outstanding.
  bool wpend_pending:1;

  // early_data_accepted is true if early data was accepted by the server.
  bool early_data_accepted:1;

  // draft_downgrade is whether the TLS 1.3 anti-downgrade logic would have
  // fired, were it not a draft.
  bool draft_downgrade:1;

  // token_binding_negotiated is set if Token Binding was negotiated.
  bool token_binding_negotiated:1;

  // hs_buf is the buffer of handshake data to process.
  UniquePtr<BUF_MEM> hs_buf;

  // pending_hs_data contains the pending handshake data that has not yet
  // been encrypted to |pending_flight|. This allows packing the handshake into
  // fewer records.
  UniquePtr<BUF_MEM> pending_hs_data;

  // pending_flight is the pending outgoing flight. This is used to flush each
  // handshake flight in a single write. |write_buffer| must be written out
  // before this data.
  UniquePtr<BUF_MEM> pending_flight;

  // pending_flight_offset is the number of bytes of |pending_flight| which have
  // been successfully written.
  uint32_t pending_flight_offset = 0;

  // ticket_age_skew is the difference, in seconds, between the client-sent
  // ticket age and the server-computed value in TLS 1.3 server connections
  // which resumed a session.
  int32_t ticket_age_skew = 0;

  // aead_read_ctx is the current read cipher state.
  UniquePtr<SSLAEADContext> aead_read_ctx;

  // aead_write_ctx is the current write cipher state.
  UniquePtr<SSLAEADContext> aead_write_ctx;

  // hs is the handshake state for the current handshake or NULL if there isn't
  // one.
  UniquePtr<SSL_HANDSHAKE> hs;

  uint8_t write_traffic_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t read_traffic_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t exporter_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t early_exporter_secret[EVP_MAX_MD_SIZE] = {0};
  uint8_t write_traffic_secret_len = 0;
  uint8_t read_traffic_secret_len = 0;
  uint8_t exporter_secret_len = 0;
  uint8_t early_exporter_secret_len = 0;

  // Connection binding to prevent renegotiation attacks
  uint8_t previous_client_finished[12] = {0};
  uint8_t previous_client_finished_len = 0;
  uint8_t previous_server_finished_len = 0;
  uint8_t previous_server_finished[12] = {0};

  uint8_t send_alert[2] = {0};

  // established_session is the session established by the connection. This
  // session is only filled upon the completion of the handshake and is
  // immutable.
  UniquePtr<SSL_SESSION> established_session;

  // Next protocol negotiation. For the client, this is the protocol that we
  // sent in NextProtocol and is set when handling ServerHello extensions.
  //
  // For a server, this is the client's selected_protocol from NextProtocol and
  // is set when handling the NextProtocol message, before the Finished
  // message.
  Array<uint8_t> next_proto_negotiated;

  // ALPN information
  // (we are in the process of transitioning from NPN to ALPN.)

  // In a server these point to the selected ALPN protocol after the
  // ClientHello has been processed. In a client these contain the protocol
  // that the server selected once the ServerHello has been processed.
  Array<uint8_t> alpn_selected;

  // hostname, on the server, is the value of the SNI extension.
  UniquePtr<char> hostname;

  // For a server:
  //     If |channel_id_valid| is true, then this contains the
  //     verified Channel ID from the client: a P256 point, (x,y), where
  //     each are big-endian values.
  uint8_t channel_id[64] = {0};

  // Contains the QUIC transport params received by the peer.
  Array<uint8_t> peer_quic_transport_params;

  // srtp_profile is the selected SRTP protection profile for
  // DTLS-SRTP.
  const SRTP_PROTECTION_PROFILE *srtp_profile = nullptr;
};

// lengths of messages
#define DTLS1_COOKIE_LENGTH 256

#define DTLS1_RT_HEADER_LENGTH 13

#define DTLS1_HM_HEADER_LENGTH 12

#define DTLS1_CCS_HEADER_LENGTH 1

#define DTLS1_AL_HEADER_LENGTH 2

struct hm_header_st {
  uint8_t type;
  uint32_t msg_len;
  uint16_t seq;
  uint32_t frag_off;
  uint32_t frag_len;
};

// An hm_fragment is an incoming DTLS message, possibly not yet assembled.
struct hm_fragment {
  static constexpr bool kAllowUniquePtr = true;

  hm_fragment() {}
  hm_fragment(const hm_fragment &) = delete;
  hm_fragment &operator=(const hm_fragment &) = delete;

  ~hm_fragment();

  // type is the type of the message.
  uint8_t type = 0;
  // seq is the sequence number of this message.
  uint16_t seq = 0;
  // msg_len is the length of the message body.
  uint32_t msg_len = 0;
  // data is a pointer to the message, including message header. It has length
  // |DTLS1_HM_HEADER_LENGTH| + |msg_len|.
  uint8_t *data = nullptr;
  // reassembly is a bitmask of |msg_len| bits corresponding to which parts of
  // the message have been received. It is NULL if the message is complete.
  uint8_t *reassembly = nullptr;
};

struct OPENSSL_timeval {
  uint64_t tv_sec;
  uint32_t tv_usec;
};

struct DTLS1_STATE {
  static constexpr bool kAllowUniquePtr = true;

  DTLS1_STATE();
  ~DTLS1_STATE();

  // has_change_cipher_spec is true if we have received a ChangeCipherSpec from
  // the peer in this epoch.
  bool has_change_cipher_spec:1;

  // outgoing_messages_complete is true if |outgoing_messages| has been
  // completed by an attempt to flush it. Future calls to |add_message| and
  // |add_change_cipher_spec| will start a new flight.
  bool outgoing_messages_complete:1;

  // flight_has_reply is true if the current outgoing flight is complete and has
  // processed at least one message. This is used to detect whether we or the
  // peer sent the final flight.
  bool flight_has_reply:1;

  uint8_t cookie[DTLS1_COOKIE_LENGTH] = {0};
  size_t cookie_len = 0;

  // The current data and handshake epoch.  This is initially undefined, and
  // starts at zero once the initial handshake is completed.
  uint16_t r_epoch = 0;
  uint16_t w_epoch = 0;

  // records being received in the current epoch
  DTLS1_BITMAP bitmap;

  uint16_t handshake_write_seq = 0;
  uint16_t handshake_read_seq = 0;

  // save last sequence number for retransmissions
  uint8_t last_write_sequence[8] = {0};
  UniquePtr<SSLAEADContext> last_aead_write_ctx;

  // incoming_messages is a ring buffer of incoming handshake messages that have
  // yet to be processed. The front of the ring buffer is message number
  // |handshake_read_seq|, at position |handshake_read_seq| %
  // |SSL_MAX_HANDSHAKE_FLIGHT|.
  UniquePtr<hm_fragment> incoming_messages[SSL_MAX_HANDSHAKE_FLIGHT];

  // outgoing_messages is the queue of outgoing messages from the last handshake
  // flight.
  DTLS_OUTGOING_MESSAGE outgoing_messages[SSL_MAX_HANDSHAKE_FLIGHT];
  uint8_t outgoing_messages_len = 0;

  // outgoing_written is the number of outgoing messages that have been
  // written.
  uint8_t outgoing_written = 0;
  // outgoing_offset is the number of bytes of the next outgoing message have
  // been written.
  uint32_t outgoing_offset = 0;

  unsigned mtu = 0;  // max DTLS packet size

  // num_timeouts is the number of times the retransmit timer has fired since
  // the last time it was reset.
  unsigned num_timeouts = 0;

  // Indicates when the last handshake msg or heartbeat sent will
  // timeout.
  struct OPENSSL_timeval next_timeout = {0, 0};

  // timeout_duration_ms is the timeout duration in milliseconds.
  unsigned timeout_duration_ms = 0;
};

// SSL_CONFIG contains configuration bits that can be shed after the handshake
// completes.  Objects of this type are not shared; they are unique to a
// particular |SSL|.
//
// See SSL_shed_handshake_config() for more about the conditions under which
// configuration can be shed.
struct SSL_CONFIG {
  static constexpr bool kAllowUniquePtr = true;

  explicit SSL_CONFIG(SSL *ssl_arg);
  ~SSL_CONFIG();

  // ssl is a non-owning pointer to the parent |SSL| object.
  SSL *const ssl = nullptr;

  // conf_max_version is the maximum acceptable protocol version configured by
  // |SSL_set_max_proto_version|. Note this version is normalized in DTLS and is
  // further constrainted by |SSL_OP_NO_*|.
  uint16_t conf_max_version = 0;

  // conf_min_version is the minimum acceptable protocol version configured by
  // |SSL_set_min_proto_version|. Note this version is normalized in DTLS and is
  // further constrainted by |SSL_OP_NO_*|.
  uint16_t conf_min_version = 0;

  X509_VERIFY_PARAM *param = nullptr;

  // crypto
  UniquePtr<SSLCipherPreferenceList> cipher_list;

  // This is used to hold the local certificate used (i.e. the server
  // certificate for a server or the client certificate for a client).
  UniquePtr<CERT> cert;

  int (*verify_callback)(int ok,
                         X509_STORE_CTX *ctx) =
      nullptr;  // fail if callback returns 0

  enum ssl_verify_result_t (*custom_verify_callback)(
      SSL *ssl, uint8_t *out_alert) = nullptr;
  // Server-only: psk_identity_hint is the identity hint to send in
  // PSK-based key exchanges.
  UniquePtr<char> psk_identity_hint;

  unsigned (*psk_client_callback)(SSL *ssl, const char *hint, char *identity,
                                  unsigned max_identity_len, uint8_t *psk,
                                  unsigned max_psk_len) = nullptr;
  unsigned (*psk_server_callback)(SSL *ssl, const char *identity, uint8_t *psk,
                                  unsigned max_psk_len) = nullptr;

  // for server side, keep the list of CA_dn we can use
  UniquePtr<STACK_OF(CRYPTO_BUFFER)> client_CA;

  // cached_x509_client_CA is a cache of parsed versions of the elements of
  // |client_CA|.
  STACK_OF(X509_NAME) *cached_x509_client_CA = nullptr;

  uint16_t dummy_pq_padding_len = 0;
  Array<uint16_t> supported_group_list;  // our list

  // The client's Channel ID private key.
  UniquePtr<EVP_PKEY> channel_id_private;

  // For a client, this contains the list of supported protocols in wire
  // format.
  Array<uint8_t> alpn_client_proto_list;

  // Contains a list of supported Token Binding key parameters.
  Array<uint8_t> token_binding_params;

  // Contains the QUIC transport params that this endpoint will send.
  Array<uint8_t> quic_transport_params;

  // verify_sigalgs, if not empty, is the set of signature algorithms
  // accepted from the peer in decreasing order of preference.
  Array<uint16_t> verify_sigalgs;

  // srtp_profiles is the list of configured SRTP protection profiles for
  // DTLS-SRTP.
  UniquePtr<STACK_OF(SRTP_PROTECTION_PROFILE)> srtp_profiles;

  // verify_mode is a bitmask of |SSL_VERIFY_*| values.
  uint8_t verify_mode = SSL_VERIFY_NONE;

  // Enable signed certificate time stamps. Currently client only.
  bool signed_cert_timestamps_enabled:1;

  // ocsp_stapling_enabled is only used by client connections and indicates
  // whether OCSP stapling will be requested.
  bool ocsp_stapling_enabled:1;

  // channel_id_enabled is copied from the |SSL_CTX|. For a server, means that
  // we'll accept Channel IDs from clients. For a client, means that we'll
  // advertise support.
  bool channel_id_enabled:1;

  // retain_only_sha256_of_client_certs is true if we should compute the SHA256
  // hash of the peer's certificate and then discard it to save memory and
  // session space. Only effective on the server side.
  bool retain_only_sha256_of_client_certs:1;

  // handoff indicates that a server should stop after receiving the
  // ClientHello and pause the handshake in such a way that |SSL_get_error|
  // returns |SSL_HANDOFF|. This is copied in |SSL_new| from the |SSL_CTX|
  // element of the same name and may be cleared if the handoff is declined.
  bool handoff:1;

  // shed_handshake_config indicates that the handshake config (this object!)
  // should be freed after the handshake completes.
  bool shed_handshake_config : 1;
};

// From draft-ietf-tls-tls13-18, used in determining PSK modes.
#define SSL_PSK_DHE_KE 0x1

// From draft-ietf-tls-tls13-16, used in determining whether to respond with a
// KeyUpdate.
#define SSL_KEY_UPDATE_NOT_REQUESTED 0
#define SSL_KEY_UPDATE_REQUESTED 1

// kMaxEarlyDataAccepted is the advertised number of plaintext bytes of early
// data that will be accepted. This value should be slightly below
// kMaxEarlyDataSkipped in tls_record.c, which is measured in ciphertext.
static const size_t kMaxEarlyDataAccepted = 14336;

UniquePtr<CERT> ssl_cert_dup(CERT *cert);
void ssl_cert_clear_certs(CERT *cert);
int ssl_set_cert(CERT *cert, UniquePtr<CRYPTO_BUFFER> buffer);
int ssl_is_key_type_supported(int key_type);
// ssl_compare_public_and_private_key returns one if |pubkey| is the public
// counterpart to |privkey|. Otherwise it returns zero and pushes a helpful
// message on the error queue.
int ssl_compare_public_and_private_key(const EVP_PKEY *pubkey,
                                       const EVP_PKEY *privkey);
int ssl_cert_check_private_key(const CERT *cert, const EVP_PKEY *privkey);
int ssl_get_new_session(SSL_HANDSHAKE *hs, int is_server);
int ssl_encrypt_ticket(SSL_HANDSHAKE *hs, CBB *out, const SSL_SESSION *session);
int ssl_ctx_rotate_ticket_encryption_key(SSL_CTX *ctx);

// ssl_session_new returns a newly-allocated blank |SSL_SESSION| or nullptr on
// error.
UniquePtr<SSL_SESSION> ssl_session_new(const SSL_X509_METHOD *x509_method);

// ssl_hash_session_id returns a hash of |session_id|, suitable for a hash table
// keyed on session IDs.
uint32_t ssl_hash_session_id(Span<const uint8_t> session_id);

// SSL_SESSION_parse parses an |SSL_SESSION| from |cbs| and advances |cbs| over
// the parsed data.
OPENSSL_EXPORT UniquePtr<SSL_SESSION> SSL_SESSION_parse(
    CBS *cbs, const SSL_X509_METHOD *x509_method, CRYPTO_BUFFER_POOL *pool);

// ssl_session_serialize writes |in| to |cbb| as if it were serialising a
// session for Session-ID resumption. It returns one on success and zero on
// error.
OPENSSL_EXPORT int ssl_session_serialize(const SSL_SESSION *in, CBB *cbb);

// ssl_session_is_context_valid returns one if |session|'s session ID context
// matches the one set on |hs| and zero otherwise.
int ssl_session_is_context_valid(const SSL_HANDSHAKE *hs,
                                 const SSL_SESSION *session);

// ssl_session_is_time_valid returns one if |session| is still valid and zero if
// it has expired.
int ssl_session_is_time_valid(const SSL *ssl, const SSL_SESSION *session);

// ssl_session_is_resumable returns one if |session| is resumable for |hs| and
// zero otherwise.
int ssl_session_is_resumable(const SSL_HANDSHAKE *hs,
                             const SSL_SESSION *session);

// ssl_session_protocol_version returns the protocol version associated with
// |session|. Note that despite the name, this is not the same as
// |SSL_SESSION_get_protocol_version|. The latter is based on upstream's name.
uint16_t ssl_session_protocol_version(const SSL_SESSION *session);

// ssl_session_get_digest returns the digest used in |session|.
const EVP_MD *ssl_session_get_digest(const SSL_SESSION *session);

void ssl_set_session(SSL *ssl, SSL_SESSION *session);

// ssl_get_prev_session looks up the previous session based on |client_hello|.
// On success, it sets |*out_session| to the session or nullptr if none was
// found. If the session could not be looked up synchronously, it returns
// |ssl_hs_pending_session| and should be called again. If a ticket could not be
// decrypted immediately it returns |ssl_hs_pending_ticket| and should also
// be called again. Otherwise, it returns |ssl_hs_error|.
enum ssl_hs_wait_t ssl_get_prev_session(SSL_HANDSHAKE *hs,
                                        UniquePtr<SSL_SESSION> *out_session,
                                        bool *out_tickets_supported,
                                        bool *out_renew_ticket,
                                        const SSL_CLIENT_HELLO *client_hello);

// The following flags determine which parts of the session are duplicated.
#define SSL_SESSION_DUP_AUTH_ONLY 0x0
#define SSL_SESSION_INCLUDE_TICKET 0x1
#define SSL_SESSION_INCLUDE_NONAUTH 0x2
#define SSL_SESSION_DUP_ALL \
  (SSL_SESSION_INCLUDE_TICKET | SSL_SESSION_INCLUDE_NONAUTH)

// SSL_SESSION_dup returns a newly-allocated |SSL_SESSION| with a copy of the
// fields in |session| or nullptr on error. The new session is non-resumable and
// must be explicitly marked resumable once it has been filled in.
OPENSSL_EXPORT UniquePtr<SSL_SESSION> SSL_SESSION_dup(SSL_SESSION *session,
                                                      int dup_flags);

// ssl_session_rebase_time updates |session|'s start time to the current time,
// adjusting the timeout so the expiration time is unchanged.
void ssl_session_rebase_time(SSL *ssl, SSL_SESSION *session);

// ssl_session_renew_timeout calls |ssl_session_rebase_time| and renews
// |session|'s timeout to |timeout| (measured from the current time). The
// renewal is clamped to the session's auth_timeout.
void ssl_session_renew_timeout(SSL *ssl, SSL_SESSION *session,
                               uint32_t timeout);

void ssl_update_cache(SSL_HANDSHAKE *hs, int mode);

int ssl_send_alert(SSL *ssl, int level, int desc);
bool ssl3_get_message(SSL *ssl, SSLMessage *out);
ssl_open_record_t ssl3_open_handshake(SSL *ssl, size_t *out_consumed,
                                      uint8_t *out_alert, Span<uint8_t> in);
void ssl3_next_message(SSL *ssl);

int ssl3_dispatch_alert(SSL *ssl);
ssl_open_record_t ssl3_open_app_data(SSL *ssl, Span<uint8_t> *out,
                                     size_t *out_consumed, uint8_t *out_alert,
                                     Span<uint8_t> in);
ssl_open_record_t ssl3_open_change_cipher_spec(SSL *ssl, size_t *out_consumed,
                                               uint8_t *out_alert,
                                               Span<uint8_t> in);
int ssl3_write_app_data(SSL *ssl, bool *out_needs_handshake, const uint8_t *buf,
                        int len);

bool ssl3_new(SSL *ssl);
void ssl3_free(SSL *ssl);

bool ssl3_init_message(SSL *ssl, CBB *cbb, CBB *body, uint8_t type);
bool ssl3_finish_message(SSL *ssl, CBB *cbb, Array<uint8_t> *out_msg);
bool ssl3_add_message(SSL *ssl, Array<uint8_t> msg);
bool ssl3_add_change_cipher_spec(SSL *ssl);
bool ssl3_add_alert(SSL *ssl, uint8_t level, uint8_t desc);
int ssl3_flush_flight(SSL *ssl);

bool dtls1_init_message(SSL *ssl, CBB *cbb, CBB *body, uint8_t type);
bool dtls1_finish_message(SSL *ssl, CBB *cbb, Array<uint8_t> *out_msg);
bool dtls1_add_message(SSL *ssl, Array<uint8_t> msg);
bool dtls1_add_change_cipher_spec(SSL *ssl);
bool dtls1_add_alert(SSL *ssl, uint8_t level, uint8_t desc);
int dtls1_flush_flight(SSL *ssl);

// ssl_add_message_cbb finishes the handshake message in |cbb| and adds it to
// the pending flight. It returns true on success and false on error.
bool ssl_add_message_cbb(SSL *ssl, CBB *cbb);

// ssl_hash_message incorporates |msg| into the handshake hash. It returns true
// on success and false on allocation failure.
bool ssl_hash_message(SSL_HANDSHAKE *hs, const SSLMessage &msg);

ssl_open_record_t dtls1_open_app_data(SSL *ssl, Span<uint8_t> *out,
                                      size_t *out_consumed, uint8_t *out_alert,
                                      Span<uint8_t> in);
ssl_open_record_t dtls1_open_change_cipher_spec(SSL *ssl, size_t *out_consumed,
                                                uint8_t *out_alert,
                                                Span<uint8_t> in);

int dtls1_write_app_data(SSL *ssl, bool *out_needs_handshake,
                         const uint8_t *buf, int len);

// dtls1_write_record sends a record. It returns one on success and <= 0 on
// error.
int dtls1_write_record(SSL *ssl, int type, const uint8_t *buf, size_t len,
                       enum dtls1_use_epoch_t use_epoch);

int dtls1_retransmit_outgoing_messages(SSL *ssl);
bool dtls1_parse_fragment(CBS *cbs, struct hm_header_st *out_hdr,
                         CBS *out_body);
bool dtls1_check_timeout_num(SSL *ssl);

void dtls1_start_timer(SSL *ssl);
void dtls1_stop_timer(SSL *ssl);
bool dtls1_is_timer_expired(SSL *ssl);
unsigned int dtls1_min_mtu(void);

bool dtls1_new(SSL *ssl);
void dtls1_free(SSL *ssl);

bool dtls1_get_message(SSL *ssl, SSLMessage *out);
ssl_open_record_t dtls1_open_handshake(SSL *ssl, size_t *out_consumed,
                                       uint8_t *out_alert, Span<uint8_t> in);
void dtls1_next_message(SSL *ssl);
int dtls1_dispatch_alert(SSL *ssl);

// tls1_configure_aead configures either the read or write direction AEAD (as
// determined by |direction|) using the keys generated by the TLS KDF. The
// |key_block_cache| argument is used to store the generated key block, if
// empty. Otherwise it's assumed that the key block is already contained within
// it. Returns one on success or zero on error.
int tls1_configure_aead(SSL *ssl, evp_aead_direction_t direction,
                        Array<uint8_t> *key_block_cache,
                        const SSL_CIPHER *cipher,
                        Span<const uint8_t> iv_override);

int tls1_change_cipher_state(SSL_HANDSHAKE *hs, evp_aead_direction_t direction);
int tls1_generate_master_secret(SSL_HANDSHAKE *hs, uint8_t *out,
                                Span<const uint8_t> premaster);

// tls1_get_grouplist returns the locally-configured group preference list.
Span<const uint16_t> tls1_get_grouplist(const SSL_HANDSHAKE *ssl);

// tls1_check_group_id returns whether |group_id| is consistent with locally-
// configured group preferences.
bool tls1_check_group_id(const SSL_HANDSHAKE *ssl, uint16_t group_id);

// tls1_get_shared_group sets |*out_group_id| to the first preferred shared
// group between client and server preferences and returns true. If none may be
// found, it returns false.
bool tls1_get_shared_group(SSL_HANDSHAKE *hs, uint16_t *out_group_id);

// tls1_set_curves converts the array of NIDs in |curves| into a newly allocated
// array of TLS group IDs. On success, the function returns true and writes the
// array to |*out_group_ids|. Otherwise, it returns false.
bool tls1_set_curves(Array<uint16_t> *out_group_ids, Span<const int> curves);

// tls1_set_curves_list converts the string of curves pointed to by |curves|
// into a newly allocated array of TLS group IDs. On success, the function
// returns true and writes the array to |*out_group_ids|. Otherwise, it returns
// false.
bool tls1_set_curves_list(Array<uint16_t> *out_group_ids, const char *curves);

// ssl_add_clienthello_tlsext writes ClientHello extensions to |out|. It returns
// true on success and false on failure. The |header_len| argument is the length
// of the ClientHello written so far and is used to compute the padding length.
// (It does not include the record header.)
bool ssl_add_clienthello_tlsext(SSL_HANDSHAKE *hs, CBB *out, size_t header_len);

bool ssl_add_serverhello_tlsext(SSL_HANDSHAKE *hs, CBB *out);
bool ssl_parse_clienthello_tlsext(SSL_HANDSHAKE *hs,
                                  const SSL_CLIENT_HELLO *client_hello);
bool ssl_parse_serverhello_tlsext(SSL_HANDSHAKE *hs, CBS *cbs);

#define tlsext_tick_md EVP_sha256

// ssl_process_ticket processes a session ticket from the client. It returns
// one of:
//   |ssl_ticket_aead_success|: |*out_session| is set to the parsed session and
//       |*out_renew_ticket| is set to whether the ticket should be renewed.
//   |ssl_ticket_aead_ignore_ticket|: |*out_renew_ticket| is set to whether a
//       fresh ticket should be sent, but the given ticket cannot be used.
//   |ssl_ticket_aead_retry|: the ticket could not be immediately decrypted.
//       Retry later.
//   |ssl_ticket_aead_error|: an error occured that is fatal to the connection.
enum ssl_ticket_aead_result_t ssl_process_ticket(
    SSL_HANDSHAKE *hs, UniquePtr<SSL_SESSION> *out_session,
    bool *out_renew_ticket, const uint8_t *ticket, size_t ticket_len,
    const uint8_t *session_id, size_t session_id_len);

// tls1_verify_channel_id processes |msg| as a Channel ID message, and verifies
// the signature. If the key is valid, it saves the Channel ID and returns true.
// Otherwise, it returns false.
bool tls1_verify_channel_id(SSL_HANDSHAKE *hs, const SSLMessage &msg);

// tls1_write_channel_id generates a Channel ID message and puts the output in
// |cbb|. |ssl->channel_id_private| must already be set before calling.  This
// function returns true on success and false on error.
bool tls1_write_channel_id(SSL_HANDSHAKE *hs, CBB *cbb);

// tls1_channel_id_hash computes the hash to be signed by Channel ID and writes
// it to |out|, which must contain at least |EVP_MAX_MD_SIZE| bytes. It returns
// true on success and false on failure.
bool tls1_channel_id_hash(SSL_HANDSHAKE *hs, uint8_t *out, size_t *out_len);

// tls1_record_handshake_hashes_for_channel_id records the current handshake
// hashes in |hs->new_session| so that Channel ID resumptions can sign that
// data.
bool tls1_record_handshake_hashes_for_channel_id(SSL_HANDSHAKE *hs);

// ssl_do_channel_id_callback checks runs |hs->ssl->ctx->channel_id_cb| if
// necessary. It returns true on success and false on fatal error. Note that, on
// success, |hs->ssl->channel_id_private| may be unset, in which case the
// operation should be retried later.
bool ssl_do_channel_id_callback(SSL_HANDSHAKE *hs);

// ssl_can_write returns whether |ssl| is allowed to write.
bool ssl_can_write(const SSL *ssl);

// ssl_can_read returns wheter |ssl| is allowed to read.
bool ssl_can_read(const SSL *ssl);

void ssl_get_current_time(const SSL *ssl, struct OPENSSL_timeval *out_clock);
void ssl_ctx_get_current_time(const SSL_CTX *ctx,
                              struct OPENSSL_timeval *out_clock);

// ssl_reset_error_state resets state for |SSL_get_error|.
void ssl_reset_error_state(SSL *ssl);

// ssl_set_read_error sets |ssl|'s read half into an error state, saving the
// current state of the error queue.
void ssl_set_read_error(SSL* ssl);

}  // namespace bssl


// Opaque C types.
//
// The following types are exported to C code as public typedefs, so they must
// be defined outside of the namespace.

// ssl_method_st backs the public |SSL_METHOD| type. It is a compatibility
// structure to support the legacy version-locked methods.
struct ssl_method_st {
  // version, if non-zero, is the only protocol version acceptable to an
  // SSL_CTX initialized from this method.
  uint16_t version;
  // method is the underlying SSL_PROTOCOL_METHOD that initializes the
  // SSL_CTX.
  const bssl::SSL_PROTOCOL_METHOD *method;
  // x509_method contains pointers to functions that might deal with |X509|
  // compatibility, or might be a no-op, depending on the application.
  const bssl::SSL_X509_METHOD *x509_method;
};

struct ssl_ctx_st {
  explicit ssl_ctx_st(const SSL_METHOD *ssl_method);
  ssl_ctx_st(const ssl_ctx_st &) = delete;
  ssl_ctx_st &operator=(const ssl_ctx_st &) = delete;

  const bssl::SSL_PROTOCOL_METHOD *method = nullptr;
  const bssl::SSL_X509_METHOD *x509_method = nullptr;

  // lock is used to protect various operations on this object.
  CRYPTO_MUTEX lock;

  // conf_max_version is the maximum acceptable protocol version configured by
  // |SSL_CTX_set_max_proto_version|. Note this version is normalized in DTLS
  // and is further constrainted by |SSL_OP_NO_*|.
  uint16_t conf_max_version = 0;

  // conf_min_version is the minimum acceptable protocol version configured by
  // |SSL_CTX_set_min_proto_version|. Note this version is normalized in DTLS
  // and is further constrainted by |SSL_OP_NO_*|.
  uint16_t conf_min_version = 0;

  // tls13_variant is the variant of TLS 1.3 we are using for this
  // configuration.
  tls13_variant_t tls13_variant = tls13_default;

  bssl::UniquePtr<bssl::SSLCipherPreferenceList> cipher_list;

  X509_STORE *cert_store = nullptr;
  LHASH_OF(SSL_SESSION) *sessions = nullptr;
  // Most session-ids that will be cached, default is
  // SSL_SESSION_CACHE_MAX_SIZE_DEFAULT. 0 is unlimited.
  unsigned long session_cache_size = SSL_SESSION_CACHE_MAX_SIZE_DEFAULT;
  SSL_SESSION *session_cache_head = nullptr;
  SSL_SESSION *session_cache_tail = nullptr;

  // handshakes_since_cache_flush is the number of successful handshakes since
  // the last cache flush.
  int handshakes_since_cache_flush = 0;

  // This can have one of 2 values, ored together,
  // SSL_SESS_CACHE_CLIENT,
  // SSL_SESS_CACHE_SERVER,
  // Default is SSL_SESSION_CACHE_SERVER, which means only
  // SSL_accept which cache SSL_SESSIONS.
  int session_cache_mode = SSL_SESS_CACHE_SERVER;

  // session_timeout is the default lifetime for new sessions in TLS 1.2 and
  // earlier, in seconds.
  uint32_t session_timeout = SSL_DEFAULT_SESSION_TIMEOUT;

  // session_psk_dhe_timeout is the default lifetime for new sessions in TLS
  // 1.3, in seconds.
  uint32_t session_psk_dhe_timeout = SSL_DEFAULT_SESSION_PSK_DHE_TIMEOUT;

  // If this callback is not null, it will be called each time a session id is
  // added to the cache.  If this function returns 1, it means that the
  // callback will do a SSL_SESSION_free() when it has finished using it.
  // Otherwise, on 0, it means the callback has finished with it. If
  // remove_session_cb is not null, it will be called when a session-id is
  // removed from the cache.  After the call, OpenSSL will SSL_SESSION_free()
  // it.
  int (*new_session_cb)(SSL *ssl, SSL_SESSION *sess) = nullptr;
  void (*remove_session_cb)(SSL_CTX *ctx, SSL_SESSION *sess) = nullptr;
  SSL_SESSION *(*get_session_cb)(SSL *ssl, const uint8_t *data, int len,
                                 int *copy) = nullptr;

  CRYPTO_refcount_t references = 1;

  // if defined, these override the X509_verify_cert() calls
  int (*app_verify_callback)(X509_STORE_CTX *store_ctx, void *arg) = nullptr;
  void *app_verify_arg = nullptr;

  ssl_verify_result_t (*custom_verify_callback)(SSL *ssl,
                                                uint8_t *out_alert) = nullptr;

  // Default password callback.
  pem_password_cb *default_passwd_callback = nullptr;

  // Default password callback user data.
  void *default_passwd_callback_userdata = nullptr;

  // get client cert callback
  int (*client_cert_cb)(SSL *ssl, X509 **out_x509, EVP_PKEY **out_pkey) = nullptr;

  // get channel id callback
  void (*channel_id_cb)(SSL *ssl, EVP_PKEY **out_pkey) = nullptr;

  CRYPTO_EX_DATA ex_data;

  // Default values used when no per-SSL value is defined follow

  void (*info_callback)(const SSL *ssl, int type, int value) = nullptr;

  // what we put in client cert requests
  bssl::UniquePtr<STACK_OF(CRYPTO_BUFFER)> client_CA;

  // cached_x509_client_CA is a cache of parsed versions of the elements of
  // |client_CA|.
  STACK_OF(X509_NAME) *cached_x509_client_CA = nullptr;


  // Default values to use in SSL structures follow (these are copied by
  // SSL_new)

  uint32_t options = 0;
  // Disable the auto-chaining feature by default. wpa_supplicant relies on this
  // feature, but require callers opt into it.
  uint32_t mode = SSL_MODE_NO_AUTO_CHAIN;
  uint32_t max_cert_list = SSL_MAX_CERT_LIST_DEFAULT;

  bssl::UniquePtr<bssl::CERT> cert;

  // callback that allows applications to peek at protocol messages
  void (*msg_callback)(int write_p, int version, int content_type,
                       const void *buf, size_t len, SSL *ssl, void *arg) = nullptr;
  void *msg_callback_arg = nullptr;

  int verify_mode = SSL_VERIFY_NONE;
  int (*default_verify_callback)(int ok, X509_STORE_CTX *ctx) =
      nullptr;  // called 'verify_callback' in the SSL

  X509_VERIFY_PARAM *param = nullptr;

  // select_certificate_cb is called before most ClientHello processing and
  // before the decision whether to resume a session is made. See
  // |ssl_select_cert_result_t| for details of the return values.
  ssl_select_cert_result_t (*select_certificate_cb)(const SSL_CLIENT_HELLO *) =
      nullptr;

  // dos_protection_cb is called once the resumption decision for a ClientHello
  // has been made. It returns one to continue the handshake or zero to
  // abort.
  int (*dos_protection_cb) (const SSL_CLIENT_HELLO *) = nullptr;

  // Maximum amount of data to send in one fragment. actual record size can be
  // more than this due to padding and MAC overheads.
  uint16_t max_send_fragment = SSL3_RT_MAX_PLAIN_LENGTH;

  // TLS extensions servername callback
  int (*servername_callback)(SSL *, int *, void *) = nullptr;
  void *servername_arg = nullptr;

  // RFC 4507 session ticket keys. |ticket_key_current| may be NULL before the
  // first handshake and |ticket_key_prev| may be NULL at any time.
  // Automatically generated ticket keys are rotated as needed at handshake
  // time. Hence, all access must be synchronized through |lock|.
  bssl::UniquePtr<bssl::TicketKey> ticket_key_current;
  bssl::UniquePtr<bssl::TicketKey> ticket_key_prev;

  // Callback to support customisation of ticket key setting
  int (*ticket_key_cb)(SSL *ssl, uint8_t *name, uint8_t *iv,
                       EVP_CIPHER_CTX *ectx, HMAC_CTX *hctx, int enc) = nullptr;

  // Server-only: psk_identity_hint is the default identity hint to send in
  // PSK-based key exchanges.
  bssl::UniquePtr<char> psk_identity_hint;

  unsigned (*psk_client_callback)(SSL *ssl, const char *hint, char *identity,
                                  unsigned max_identity_len, uint8_t *psk,
                                  unsigned max_psk_len) = nullptr;
  unsigned (*psk_server_callback)(SSL *ssl, const char *identity, uint8_t *psk,
                                  unsigned max_psk_len) = nullptr;


  // Next protocol negotiation information
  // (for experimental NPN extension).

  // For a server, this contains a callback function by which the set of
  // advertised protocols can be provided.
  int (*next_protos_advertised_cb)(SSL *ssl, const uint8_t **out,
                                   unsigned *out_len, void *arg) = nullptr;
  void *next_protos_advertised_cb_arg = nullptr;
  // For a client, this contains a callback function that selects the
  // next protocol from the list provided by the server.
  int (*next_proto_select_cb)(SSL *ssl, uint8_t **out, uint8_t *out_len,
                              const uint8_t *in, unsigned in_len,
                              void *arg) = nullptr;
  void *next_proto_select_cb_arg = nullptr;

  // ALPN information
  // (we are in the process of transitioning from NPN to ALPN.)

  // For a server, this contains a callback function that allows the
  // server to select the protocol for the connection.
  //   out: on successful return, this must point to the raw protocol
  //        name (without the length prefix).
  //   outlen: on successful return, this contains the length of |*out|.
  //   in: points to the client's list of supported protocols in
  //       wire-format.
  //   inlen: the length of |in|.
  int (*alpn_select_cb)(SSL *ssl, const uint8_t **out, uint8_t *out_len,
                        const uint8_t *in, unsigned in_len,
                        void *arg) = nullptr;
  void *alpn_select_cb_arg = nullptr;

  // For a client, this contains the list of supported protocols in wire
  // format.
  bssl::Array<uint8_t> alpn_client_proto_list;

  // SRTP profiles we are willing to do from RFC 5764
  bssl::UniquePtr<STACK_OF(SRTP_PROTECTION_PROFILE)> srtp_profiles;

  // Defined compression algorithms for certificates.
  bssl::UniquePtr<STACK_OF(CertCompressionAlg)> cert_compression_algs;

  // Supported group values inherited by SSL structure
  bssl::Array<uint16_t> supported_group_list;

  // The client's Channel ID private key.
  bssl::UniquePtr<EVP_PKEY> channel_id_private;

  // keylog_callback, if not NULL, is the key logging callback. See
  // |SSL_CTX_set_keylog_callback|.
  void (*keylog_callback)(const SSL *ssl, const char *line) = nullptr;

  // current_time_cb, if not NULL, is the function to use to get the current
  // time. It sets |*out_clock| to the current time. The |ssl| argument is
  // always NULL. See |SSL_CTX_set_current_time_cb|.
  void (*current_time_cb)(const SSL *ssl, struct timeval *out_clock) = nullptr;

  // pool is used for all |CRYPTO_BUFFER|s in case we wish to share certificate
  // memory.
  CRYPTO_BUFFER_POOL *pool = nullptr;

  // ticket_aead_method contains function pointers for opening and sealing
  // session tickets.
  const SSL_TICKET_AEAD_METHOD *ticket_aead_method = nullptr;

  // legacy_ocsp_callback implements an OCSP-related callback for OpenSSL
  // compatibility.
  int (*legacy_ocsp_callback)(SSL *ssl, void *arg) = nullptr;
  void *legacy_ocsp_callback_arg = nullptr;

  // verify_sigalgs, if not empty, is the set of signature algorithms
  // accepted from the peer in decreasing order of preference.
  bssl::Array<uint16_t> verify_sigalgs;

  // retain_only_sha256_of_client_certs is true if we should compute the SHA256
  // hash of the peer's certificate and then discard it to save memory and
  // session space. Only effective on the server side.
  bool retain_only_sha256_of_client_certs:1;

  // quiet_shutdown is true if the connection should not send a close_notify on
  // shutdown.
  bool quiet_shutdown:1;

  // ocsp_stapling_enabled is only used by client connections and indicates
  // whether OCSP stapling will be requested.
  bool ocsp_stapling_enabled:1;

  // If true, a client will request certificate timestamps.
  bool signed_cert_timestamps_enabled:1;

  // channel_id_enabled is whether Channel ID is enabled. For a server, means
  // that we'll accept Channel IDs from clients.  For a client, means that we'll
  // advertise support.
  bool channel_id_enabled:1;

  // grease_enabled is whether draft-davidben-tls-grease-01 is enabled.
  bool grease_enabled:1;

  // allow_unknown_alpn_protos is whether the client allows unsolicited ALPN
  // protocols from the peer.
  bool allow_unknown_alpn_protos:1;

  // ed25519_enabled is whether Ed25519 is advertised in the handshake.
  bool ed25519_enabled:1;

  // rsa_pss_rsae_certs_enabled is whether rsa_pss_rsae_* are supported by the
  // certificate verifier.
  bool rsa_pss_rsae_certs_enabled:1;

  // false_start_allowed_without_alpn is whether False Start (if
  // |SSL_MODE_ENABLE_FALSE_START| is enabled) is allowed without ALPN.
  bool false_start_allowed_without_alpn:1;

  // handoff indicates that a server should stop after receiving the
  // ClientHello and pause the handshake in such a way that |SSL_get_error|
  // returns |SSL_HANDOFF|.
  bool handoff:1;

  // If enable_early_data is true, early data can be sent and accepted.
  bool enable_early_data : 1;

 private:
  ~ssl_ctx_st();
  friend void SSL_CTX_free(SSL_CTX *);
};

struct ssl_st {
  explicit ssl_st(SSL_CTX *ctx_arg);
  ssl_st(const ssl_st &) = delete;
  ssl_st &operator=(const ssl_st &) = delete;
  ~ssl_st();

  // method is the method table corresponding to the current protocol (DTLS or
  // TLS).
  const bssl::SSL_PROTOCOL_METHOD *method = nullptr;

  // config is a container for handshake configuration.  Accesses to this field
  // should check for nullptr, since configuration may be shed after the
  // handshake completes.  (If you have the |SSL_HANDSHAKE| object at hand, use
  // that instead, and skip the null check.)
  bssl::UniquePtr<bssl::SSL_CONFIG> config;

  // version is the protocol version.
  uint16_t version = 0;

  uint16_t max_send_fragment = 0;

  // There are 2 BIO's even though they are normally both the same. This is so
  // data can be read and written to different handlers

  bssl::UniquePtr<BIO> rbio;  // used by SSL_read
  bssl::UniquePtr<BIO> wbio;  // used by SSL_write

  // do_handshake runs the handshake. On completion, it returns |ssl_hs_ok|.
  // Otherwise, it returns a value corresponding to what operation is needed to
  // progress.
  bssl::ssl_hs_wait_t (*do_handshake)(bssl::SSL_HANDSHAKE *hs) = nullptr;

  bssl::SSL3_STATE *s3 = nullptr;   // TLS variables
  bssl::DTLS1_STATE *d1 = nullptr;  // DTLS variables

  // callback that allows applications to peek at protocol messages
  void (*msg_callback)(int write_p, int version, int content_type,
                       const void *buf, size_t len, SSL *ssl,
                       void *arg) = nullptr;
  void *msg_callback_arg = nullptr;

  // session info

  // initial_timeout_duration_ms is the default DTLS timeout duration in
  // milliseconds. It's used to initialize the timer any time it's restarted.
  //
  // RFC 6347 states that implementations SHOULD use an initial timer value of 1
  // second.
  unsigned initial_timeout_duration_ms = 1000;

  // tls13_variant is the variant of TLS 1.3 we are using for this
  // configuration.
  tls13_variant_t tls13_variant = tls13_default;

  // session is the configured session to be offered by the client. This session
  // is immutable.
  bssl::UniquePtr<SSL_SESSION> session;

  void (*info_callback)(const SSL *ssl, int type, int value) = nullptr;

  bssl::UniquePtr<SSL_CTX> ctx;

  // session_ctx is the |SSL_CTX| used for the session cache and related
  // settings.
  bssl::UniquePtr<SSL_CTX> session_ctx;

  // extra application data
  CRYPTO_EX_DATA ex_data;

  uint32_t options = 0;  // protocol behaviour
  uint32_t mode = 0;     // API behaviour
  uint32_t max_cert_list = 0;
  bssl::UniquePtr<char> hostname;

  // renegotiate_mode controls how peer renegotiation attempts are handled.
  ssl_renegotiate_mode_t renegotiate_mode = ssl_renegotiate_never;

  // server is true iff the this SSL* is the server half. Note: before the SSL*
  // is initialized by either SSL_set_accept_state or SSL_set_connect_state,
  // the side is not determined. In this state, server is always false.
  bool server : 1;

  // quiet_shutdown is true if the connection should not send a close_notify on
  // shutdown.
  bool quiet_shutdown : 1;

  // did_dummy_pq_padding is only valid for a client. In that context, it is
  // true iff the client observed the server echoing a dummy PQ padding
  // extension.
  bool did_dummy_pq_padding:1;

  // If enable_early_data is true, early data can be sent and accepted.
  bool enable_early_data : 1;
};

struct ssl_session_st {
  explicit ssl_session_st(const bssl::SSL_X509_METHOD *method);
  ssl_session_st(const ssl_session_st &) = delete;
  ssl_session_st &operator=(const ssl_session_st &) = delete;

  CRYPTO_refcount_t references = 1;
  uint16_t ssl_version = 0;  // what ssl version session info is being kept in here?

  // group_id is the ID of the ECDH group used to establish this session or zero
  // if not applicable or unknown.
  uint16_t group_id = 0;

  // peer_signature_algorithm is the signature algorithm used to authenticate
  // the peer, or zero if not applicable or unknown.
  uint16_t peer_signature_algorithm = 0;

  // master_key, in TLS 1.2 and below, is the master secret associated with the
  // session. In TLS 1.3 and up, it is the resumption secret.
  int master_key_length = 0;
  uint8_t master_key[SSL_MAX_MASTER_KEY_LENGTH] = {0};

  // session_id - valid?
  unsigned session_id_length = 0;
  uint8_t session_id[SSL_MAX_SSL_SESSION_ID_LENGTH] = {0};
  // this is used to determine whether the session is being reused in
  // the appropriate context. It is up to the application to set this,
  // via SSL_new
  uint8_t sid_ctx_length = 0;
  uint8_t sid_ctx[SSL_MAX_SID_CTX_LENGTH] = {0};

  bssl::UniquePtr<char> psk_identity;

  // certs contains the certificate chain from the peer, starting with the leaf
  // certificate.
  bssl::UniquePtr<STACK_OF(CRYPTO_BUFFER)> certs;

  const bssl::SSL_X509_METHOD *x509_method = nullptr;

  // x509_peer is the peer's certificate.
  X509 *x509_peer = nullptr;

  // x509_chain is the certificate chain sent by the peer. NOTE: for historical
  // reasons, when a client (so the peer is a server), the chain includes
  // |peer|, but when a server it does not.
  STACK_OF(X509) *x509_chain = nullptr;

  // x509_chain_without_leaf is a lazily constructed copy of |x509_chain| that
  // omits the leaf certificate. This exists because OpenSSL, historically,
  // didn't include the leaf certificate in the chain for a server, but did for
  // a client. The |x509_chain| always includes it and, if an API call requires
  // a chain without, it is stored here.
  STACK_OF(X509) *x509_chain_without_leaf = nullptr;

  // verify_result is the result of certificate verification in the case of
  // non-fatal certificate errors.
  long verify_result = X509_V_ERR_INVALID_CALL;

  // timeout is the lifetime of the session in seconds, measured from |time|.
  // This is renewable up to |auth_timeout|.
  uint32_t timeout = SSL_DEFAULT_SESSION_TIMEOUT;

  // auth_timeout is the non-renewable lifetime of the session in seconds,
  // measured from |time|.
  uint32_t auth_timeout = SSL_DEFAULT_SESSION_TIMEOUT;

  // time is the time the session was issued, measured in seconds from the UNIX
  // epoch.
  uint64_t time = 0;

  const SSL_CIPHER *cipher = nullptr;

  CRYPTO_EX_DATA ex_data;  // application specific data

  // These are used to make removal of session-ids more efficient and to
  // implement a maximum cache size.
  SSL_SESSION *prev = nullptr, *next = nullptr;

  bssl::Array<uint8_t> ticket;

  bssl::UniquePtr<CRYPTO_BUFFER> signed_cert_timestamp_list;

  // The OCSP response that came with the session.
  bssl::UniquePtr<CRYPTO_BUFFER> ocsp_response;

  // peer_sha256 contains the SHA-256 hash of the peer's certificate if
  // |peer_sha256_valid| is true.
  uint8_t peer_sha256[SHA256_DIGEST_LENGTH] = {0};

  // original_handshake_hash contains the handshake hash (either SHA-1+MD5 or
  // SHA-2, depending on TLS version) for the original, full handshake that
  // created a session. This is used by Channel IDs during resumption.
  uint8_t original_handshake_hash[EVP_MAX_MD_SIZE] = {0};
  uint8_t original_handshake_hash_len = 0;

  uint32_t ticket_lifetime_hint = 0;  // Session lifetime hint in seconds

  uint32_t ticket_age_add = 0;

  // ticket_max_early_data is the maximum amount of data allowed to be sent as
  // early data. If zero, 0-RTT is disallowed.
  uint32_t ticket_max_early_data = 0;

  // early_alpn is the ALPN protocol from the initial handshake. This is only
  // stored for TLS 1.3 and above in order to enforce ALPN matching for 0-RTT
  // resumptions.
  bssl::Array<uint8_t> early_alpn;

  // extended_master_secret is whether the master secret in this session was
  // generated using EMS and thus isn't vulnerable to the Triple Handshake
  // attack.
  bool extended_master_secret:1;

  // peer_sha256_valid is whether |peer_sha256| is valid.
  bool peer_sha256_valid:1;  // Non-zero if peer_sha256 is valid

  // not_resumable is used to indicate that session resumption is disallowed.
  bool not_resumable:1;

  // ticket_age_add_valid is whether |ticket_age_add| is valid.
  bool ticket_age_add_valid:1;

  // is_server is whether this session was created by a server.
  bool is_server:1;

 private:
  ~ssl_session_st();
  friend void SSL_SESSION_free(SSL_SESSION *);
};


#endif  // OPENSSL_HEADER_SSL_INTERNAL_H
