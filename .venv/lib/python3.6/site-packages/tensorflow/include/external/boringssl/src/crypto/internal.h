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
 * Copyright (c) 1998-2001 The OpenSSL Project.  All rights reserved.
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

#ifndef OPENSSL_HEADER_CRYPTO_INTERNAL_H
#define OPENSSL_HEADER_CRYPTO_INTERNAL_H

#include <openssl/ex_data.h>
#include <openssl/stack.h>
#include <openssl/thread.h>

#include <assert.h>
#include <string.h>

#if !defined(__cplusplus)
#if defined(__GNUC__) && \
    (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) < 40800
// |alignas| and |alignof| were added in C11. GCC added support in version 4.8.
// Testing for __STDC_VERSION__/__cplusplus doesn't work because 4.7 already
// reports support for C11.
#define alignas(x) __attribute__ ((aligned (x)))
#define alignof(x) __alignof__ (x)
#elif defined(_MSC_VER)
#define alignas(x) __declspec(align(x))
#define alignof __alignof
#else
#include <stdalign.h>
#endif
#endif

#if !defined(OPENSSL_NO_THREADS) && \
    (!defined(OPENSSL_WINDOWS) || defined(__MINGW32__))
#include <pthread.h>
#define OPENSSL_PTHREADS
#endif

#if !defined(OPENSSL_NO_THREADS) && !defined(OPENSSL_PTHREADS) && \
    defined(OPENSSL_WINDOWS)
#define OPENSSL_WINDOWS_THREADS
OPENSSL_MSVC_PRAGMA(warning(push, 3))
#include <windows.h>
OPENSSL_MSVC_PRAGMA(warning(pop))
#endif

#if defined(__cplusplus)
extern "C" {
#endif


#if defined(OPENSSL_X86) || defined(OPENSSL_X86_64) || defined(OPENSSL_ARM) || \
    defined(OPENSSL_AARCH64) || defined(OPENSSL_PPC64LE)
// OPENSSL_cpuid_setup initializes the platform-specific feature cache.
void OPENSSL_cpuid_setup(void);
#endif


#if (!defined(_MSC_VER) || defined(__clang__)) && defined(OPENSSL_64_BIT)
#define BORINGSSL_HAS_UINT128
typedef __int128_t int128_t;
typedef __uint128_t uint128_t;

// clang-cl supports __uint128_t but modulus and division don't work.
// https://crbug.com/787617.
#if !defined(_MSC_VER) || !defined(__clang__)
#define BORINGSSL_CAN_DIVIDE_UINT128
#endif
#endif

#define OPENSSL_ARRAY_SIZE(array) (sizeof(array) / sizeof((array)[0]))

// Have a generic fall-through for different versions of C/C++.
#if defined(__cplusplus) && __cplusplus >= 201703L
#define OPENSSL_FALLTHROUGH [[fallthrough]]
#elif defined(__cplusplus) && __cplusplus >= 201103L && defined(__clang__)
#define OPENSSL_FALLTHROUGH [[clang::fallthrough]]
#elif defined(__cplusplus) && __cplusplus >= 201103L && defined(__GNUC__) && \
    __GNUC__ >= 7
#define OPENSSL_FALLTHROUGH [[gnu::fallthrough]]
#elif defined(__GNUC__) && __GNUC__ >= 7 // gcc 7
#define OPENSSL_FALLTHROUGH __attribute__ ((fallthrough))
#else // C++11 on gcc 6, and all other cases
#define OPENSSL_FALLTHROUGH
#endif

// buffers_alias returns one if |a| and |b| alias and zero otherwise.
static inline int buffers_alias(const uint8_t *a, size_t a_len,
                                const uint8_t *b, size_t b_len) {
  // Cast |a| and |b| to integers. In C, pointer comparisons between unrelated
  // objects are undefined whereas pointer to integer conversions are merely
  // implementation-defined. We assume the implementation defined it in a sane
  // way.
  uintptr_t a_u = (uintptr_t)a;
  uintptr_t b_u = (uintptr_t)b;
  return a_u + a_len > b_u && b_u + b_len > a_u;
}


// Constant-time utility functions.
//
// The following methods return a bitmask of all ones (0xff...f) for true and 0
// for false. This is useful for choosing a value based on the result of a
// conditional in constant time. For example,
//
// if (a < b) {
//   c = a;
// } else {
//   c = b;
// }
//
// can be written as
//
// crypto_word_t lt = constant_time_lt_w(a, b);
// c = constant_time_select_w(lt, a, b);

// crypto_word_t is the type that most constant-time functions use. Ideally we
// would like it to be |size_t|, but NaCl builds in 64-bit mode with 32-bit
// pointers, which means that |size_t| can be 32 bits when |BN_ULONG| is 64
// bits. Since we want to be able to do constant-time operations on a
// |BN_ULONG|, |crypto_word_t| is defined as an unsigned value with the native
// word length.
#if defined(OPENSSL_64_BIT)
typedef uint64_t crypto_word_t;
#elif defined(OPENSSL_32_BIT)
typedef uint32_t crypto_word_t;
#else
#error "Must define either OPENSSL_32_BIT or OPENSSL_64_BIT"
#endif

#define CONSTTIME_TRUE_W ~((crypto_word_t)0)
#define CONSTTIME_FALSE_W ((crypto_word_t)0)
#define CONSTTIME_TRUE_8 ((uint8_t)0xff)
#define CONSTTIME_FALSE_8 ((uint8_t)0)

// constant_time_msb_w returns the given value with the MSB copied to all the
// other bits.
static inline crypto_word_t constant_time_msb_w(crypto_word_t a) {
  return 0u - (a >> (sizeof(a) * 8 - 1));
}

// constant_time_lt_w returns 0xff..f if a < b and 0 otherwise.
static inline crypto_word_t constant_time_lt_w(crypto_word_t a,
                                               crypto_word_t b) {
  // Consider the two cases of the problem:
  //   msb(a) == msb(b): a < b iff the MSB of a - b is set.
  //   msb(a) != msb(b): a < b iff the MSB of b is set.
  //
  // If msb(a) == msb(b) then the following evaluates as:
  //   msb(a^((a^b)|((a-b)^a))) ==
  //   msb(a^((a-b) ^ a))       ==   (because msb(a^b) == 0)
  //   msb(a^a^(a-b))           ==   (rearranging)
  //   msb(a-b)                      (because ∀x. x^x == 0)
  //
  // Else, if msb(a) != msb(b) then the following evaluates as:
  //   msb(a^((a^b)|((a-b)^a))) ==
  //   msb(a^(𝟙 | ((a-b)^a)))   ==   (because msb(a^b) == 1 and 𝟙
  //                                  represents a value s.t. msb(𝟙) = 1)
  //   msb(a^𝟙)                 ==   (because ORing with 1 results in 1)
  //   msb(b)
  //
  //
  // Here is an SMT-LIB verification of this formula:
  //
  // (define-fun lt ((a (_ BitVec 32)) (b (_ BitVec 32))) (_ BitVec 32)
  //   (bvxor a (bvor (bvxor a b) (bvxor (bvsub a b) a)))
  // )
  //
  // (declare-fun a () (_ BitVec 32))
  // (declare-fun b () (_ BitVec 32))
  //
  // (assert (not (= (= #x00000001 (bvlshr (lt a b) #x0000001f)) (bvult a b))))
  // (check-sat)
  // (get-model)
  return constant_time_msb_w(a^((a^b)|((a-b)^a)));
}

// constant_time_lt_8 acts like |constant_time_lt_w| but returns an 8-bit
// mask.
static inline uint8_t constant_time_lt_8(crypto_word_t a, crypto_word_t b) {
  return (uint8_t)(constant_time_lt_w(a, b));
}

// constant_time_ge_w returns 0xff..f if a >= b and 0 otherwise.
static inline crypto_word_t constant_time_ge_w(crypto_word_t a,
                                               crypto_word_t b) {
  return ~constant_time_lt_w(a, b);
}

// constant_time_ge_8 acts like |constant_time_ge_w| but returns an 8-bit
// mask.
static inline uint8_t constant_time_ge_8(crypto_word_t a, crypto_word_t b) {
  return (uint8_t)(constant_time_ge_w(a, b));
}

// constant_time_is_zero returns 0xff..f if a == 0 and 0 otherwise.
static inline crypto_word_t constant_time_is_zero_w(crypto_word_t a) {
  // Here is an SMT-LIB verification of this formula:
  //
  // (define-fun is_zero ((a (_ BitVec 32))) (_ BitVec 32)
  //   (bvand (bvnot a) (bvsub a #x00000001))
  // )
  //
  // (declare-fun a () (_ BitVec 32))
  //
  // (assert (not (= (= #x00000001 (bvlshr (is_zero a) #x0000001f)) (= a #x00000000))))
  // (check-sat)
  // (get-model)
  return constant_time_msb_w(~a & (a - 1));
}

// constant_time_is_zero_8 acts like |constant_time_is_zero_w| but returns an
// 8-bit mask.
static inline uint8_t constant_time_is_zero_8(crypto_word_t a) {
  return (uint8_t)(constant_time_is_zero_w(a));
}

// constant_time_eq_w returns 0xff..f if a == b and 0 otherwise.
static inline crypto_word_t constant_time_eq_w(crypto_word_t a,
                                               crypto_word_t b) {
  return constant_time_is_zero_w(a ^ b);
}

// constant_time_eq_8 acts like |constant_time_eq_w| but returns an 8-bit
// mask.
static inline uint8_t constant_time_eq_8(crypto_word_t a, crypto_word_t b) {
  return (uint8_t)(constant_time_eq_w(a, b));
}

// constant_time_eq_int acts like |constant_time_eq_w| but works on int
// values.
static inline crypto_word_t constant_time_eq_int(int a, int b) {
  return constant_time_eq_w((crypto_word_t)(a), (crypto_word_t)(b));
}

// constant_time_eq_int_8 acts like |constant_time_eq_int| but returns an 8-bit
// mask.
static inline uint8_t constant_time_eq_int_8(int a, int b) {
  return constant_time_eq_8((crypto_word_t)(a), (crypto_word_t)(b));
}

// constant_time_select_w returns (mask & a) | (~mask & b). When |mask| is all
// 1s or all 0s (as returned by the methods above), the select methods return
// either |a| (if |mask| is nonzero) or |b| (if |mask| is zero).
static inline crypto_word_t constant_time_select_w(crypto_word_t mask,
                                                   crypto_word_t a,
                                                   crypto_word_t b) {
  return (mask & a) | (~mask & b);
}

// constant_time_select_8 acts like |constant_time_select| but operates on
// 8-bit values.
static inline uint8_t constant_time_select_8(uint8_t mask, uint8_t a,
                                             uint8_t b) {
  return (uint8_t)(constant_time_select_w(mask, a, b));
}

// constant_time_select_int acts like |constant_time_select| but operates on
// ints.
static inline int constant_time_select_int(crypto_word_t mask, int a, int b) {
  return (int)(constant_time_select_w(mask, (crypto_word_t)(a),
                                      (crypto_word_t)(b)));
}


// Thread-safe initialisation.

#if defined(OPENSSL_NO_THREADS)
typedef uint32_t CRYPTO_once_t;
#define CRYPTO_ONCE_INIT 0
#elif defined(OPENSSL_WINDOWS_THREADS)
typedef INIT_ONCE CRYPTO_once_t;
#define CRYPTO_ONCE_INIT INIT_ONCE_STATIC_INIT
#elif defined(OPENSSL_PTHREADS)
typedef pthread_once_t CRYPTO_once_t;
#define CRYPTO_ONCE_INIT PTHREAD_ONCE_INIT
#else
#error "Unknown threading library"
#endif

// CRYPTO_once calls |init| exactly once per process. This is thread-safe: if
// concurrent threads call |CRYPTO_once| with the same |CRYPTO_once_t| argument
// then they will block until |init| completes, but |init| will have only been
// called once.
//
// The |once| argument must be a |CRYPTO_once_t| that has been initialised with
// the value |CRYPTO_ONCE_INIT|.
OPENSSL_EXPORT void CRYPTO_once(CRYPTO_once_t *once, void (*init)(void));


// Reference counting.

// CRYPTO_REFCOUNT_MAX is the value at which the reference count saturates.
#define CRYPTO_REFCOUNT_MAX 0xffffffff

// CRYPTO_refcount_inc atomically increments the value at |*count| unless the
// value would overflow. It's safe for multiple threads to concurrently call
// this or |CRYPTO_refcount_dec_and_test_zero| on the same
// |CRYPTO_refcount_t|.
OPENSSL_EXPORT void CRYPTO_refcount_inc(CRYPTO_refcount_t *count);

// CRYPTO_refcount_dec_and_test_zero tests the value at |*count|:
//   if it's zero, it crashes the address space.
//   if it's the maximum value, it returns zero.
//   otherwise, it atomically decrements it and returns one iff the resulting
//       value is zero.
//
// It's safe for multiple threads to concurrently call this or
// |CRYPTO_refcount_inc| on the same |CRYPTO_refcount_t|.
OPENSSL_EXPORT int CRYPTO_refcount_dec_and_test_zero(CRYPTO_refcount_t *count);


// Locks.
//
// Two types of locks are defined: |CRYPTO_MUTEX|, which can be used in
// structures as normal, and |struct CRYPTO_STATIC_MUTEX|, which can be used as
// a global lock. A global lock must be initialised to the value
// |CRYPTO_STATIC_MUTEX_INIT|.
//
// |CRYPTO_MUTEX| can appear in public structures and so is defined in
// thread.h as a structure large enough to fit the real type. The global lock is
// a different type so it may be initialized with platform initializer macros.

#if defined(OPENSSL_NO_THREADS)
struct CRYPTO_STATIC_MUTEX {
  char padding;  // Empty structs have different sizes in C and C++.
};
#define CRYPTO_STATIC_MUTEX_INIT { 0 }
#elif defined(OPENSSL_WINDOWS_THREADS)
struct CRYPTO_STATIC_MUTEX {
  SRWLOCK lock;
};
#define CRYPTO_STATIC_MUTEX_INIT { SRWLOCK_INIT }
#elif defined(OPENSSL_PTHREADS)
struct CRYPTO_STATIC_MUTEX {
  pthread_rwlock_t lock;
};
#define CRYPTO_STATIC_MUTEX_INIT { PTHREAD_RWLOCK_INITIALIZER }
#else
#error "Unknown threading library"
#endif

// CRYPTO_MUTEX_init initialises |lock|. If |lock| is a static variable, use a
// |CRYPTO_STATIC_MUTEX|.
OPENSSL_EXPORT void CRYPTO_MUTEX_init(CRYPTO_MUTEX *lock);

// CRYPTO_MUTEX_lock_read locks |lock| such that other threads may also have a
// read lock, but none may have a write lock.
OPENSSL_EXPORT void CRYPTO_MUTEX_lock_read(CRYPTO_MUTEX *lock);

// CRYPTO_MUTEX_lock_write locks |lock| such that no other thread has any type
// of lock on it.
OPENSSL_EXPORT void CRYPTO_MUTEX_lock_write(CRYPTO_MUTEX *lock);

// CRYPTO_MUTEX_unlock_read unlocks |lock| for reading.
OPENSSL_EXPORT void CRYPTO_MUTEX_unlock_read(CRYPTO_MUTEX *lock);

// CRYPTO_MUTEX_unlock_write unlocks |lock| for writing.
OPENSSL_EXPORT void CRYPTO_MUTEX_unlock_write(CRYPTO_MUTEX *lock);

// CRYPTO_MUTEX_cleanup releases all resources held by |lock|.
OPENSSL_EXPORT void CRYPTO_MUTEX_cleanup(CRYPTO_MUTEX *lock);

// CRYPTO_STATIC_MUTEX_lock_read locks |lock| such that other threads may also
// have a read lock, but none may have a write lock. The |lock| variable does
// not need to be initialised by any function, but must have been statically
// initialised with |CRYPTO_STATIC_MUTEX_INIT|.
OPENSSL_EXPORT void CRYPTO_STATIC_MUTEX_lock_read(
    struct CRYPTO_STATIC_MUTEX *lock);

// CRYPTO_STATIC_MUTEX_lock_write locks |lock| such that no other thread has
// any type of lock on it.  The |lock| variable does not need to be initialised
// by any function, but must have been statically initialised with
// |CRYPTO_STATIC_MUTEX_INIT|.
OPENSSL_EXPORT void CRYPTO_STATIC_MUTEX_lock_write(
    struct CRYPTO_STATIC_MUTEX *lock);

// CRYPTO_STATIC_MUTEX_unlock_read unlocks |lock| for reading.
OPENSSL_EXPORT void CRYPTO_STATIC_MUTEX_unlock_read(
    struct CRYPTO_STATIC_MUTEX *lock);

// CRYPTO_STATIC_MUTEX_unlock_write unlocks |lock| for writing.
OPENSSL_EXPORT void CRYPTO_STATIC_MUTEX_unlock_write(
    struct CRYPTO_STATIC_MUTEX *lock);

#if defined(__cplusplus)
extern "C++" {

namespace bssl {

namespace internal {

// MutexLockBase is a RAII helper for CRYPTO_MUTEX locking.
template <void (*LockFunc)(CRYPTO_MUTEX *), void (*ReleaseFunc)(CRYPTO_MUTEX *)>
class MutexLockBase {
 public:
  explicit MutexLockBase(CRYPTO_MUTEX *mu) : mu_(mu) {
    assert(mu_ != nullptr);
    LockFunc(mu_);
  }
  ~MutexLockBase() { ReleaseFunc(mu_); }
  MutexLockBase(const MutexLockBase<LockFunc, ReleaseFunc> &) = delete;
  MutexLockBase &operator=(const MutexLockBase<LockFunc, ReleaseFunc> &) =
      delete;

 private:
  CRYPTO_MUTEX *const mu_;
};

}  // namespace internal

using MutexWriteLock =
    internal::MutexLockBase<CRYPTO_MUTEX_lock_write, CRYPTO_MUTEX_unlock_write>;
using MutexReadLock =
    internal::MutexLockBase<CRYPTO_MUTEX_lock_read, CRYPTO_MUTEX_unlock_read>;

}  // namespace bssl

}  // extern "C++"
#endif  // defined(__cplusplus)


// Thread local storage.

// thread_local_data_t enumerates the types of thread-local data that can be
// stored.
typedef enum {
  OPENSSL_THREAD_LOCAL_ERR = 0,
  OPENSSL_THREAD_LOCAL_TEST,
  NUM_OPENSSL_THREAD_LOCALS,
} thread_local_data_t;

// thread_local_destructor_t is the type of a destructor function that will be
// called when a thread exits and its thread-local storage needs to be freed.
typedef void (*thread_local_destructor_t)(void *);

// CRYPTO_get_thread_local gets the pointer value that is stored for the
// current thread for the given index, or NULL if none has been set.
OPENSSL_EXPORT void *CRYPTO_get_thread_local(thread_local_data_t value);

// CRYPTO_set_thread_local sets a pointer value for the current thread at the
// given index. This function should only be called once per thread for a given
// |index|: rather than update the pointer value itself, update the data that
// is pointed to.
//
// The destructor function will be called when a thread exits to free this
// thread-local data. All calls to |CRYPTO_set_thread_local| with the same
// |index| should have the same |destructor| argument. The destructor may be
// called with a NULL argument if a thread that never set a thread-local
// pointer for |index|, exits. The destructor may be called concurrently with
// different arguments.
//
// This function returns one on success or zero on error. If it returns zero
// then |destructor| has been called with |value| already.
OPENSSL_EXPORT int CRYPTO_set_thread_local(
    thread_local_data_t index, void *value,
    thread_local_destructor_t destructor);


// ex_data

typedef struct crypto_ex_data_func_st CRYPTO_EX_DATA_FUNCS;

DECLARE_STACK_OF(CRYPTO_EX_DATA_FUNCS)

// CRYPTO_EX_DATA_CLASS tracks the ex_indices registered for a type which
// supports ex_data. It should defined as a static global within the module
// which defines that type.
typedef struct {
  struct CRYPTO_STATIC_MUTEX lock;
  STACK_OF(CRYPTO_EX_DATA_FUNCS) *meth;
  // num_reserved is one if the ex_data index zero is reserved for legacy
  // |TYPE_get_app_data| functions.
  uint8_t num_reserved;
} CRYPTO_EX_DATA_CLASS;

#define CRYPTO_EX_DATA_CLASS_INIT {CRYPTO_STATIC_MUTEX_INIT, NULL, 0}
#define CRYPTO_EX_DATA_CLASS_INIT_WITH_APP_DATA \
    {CRYPTO_STATIC_MUTEX_INIT, NULL, 1}

// CRYPTO_get_ex_new_index allocates a new index for |ex_data_class| and writes
// it to |*out_index|. Each class of object should provide a wrapper function
// that uses the correct |CRYPTO_EX_DATA_CLASS|. It returns one on success and
// zero otherwise.
OPENSSL_EXPORT int CRYPTO_get_ex_new_index(CRYPTO_EX_DATA_CLASS *ex_data_class,
                                           int *out_index, long argl,
                                           void *argp,
                                           CRYPTO_EX_free *free_func);

// CRYPTO_set_ex_data sets an extra data pointer on a given object. Each class
// of object should provide a wrapper function.
OPENSSL_EXPORT int CRYPTO_set_ex_data(CRYPTO_EX_DATA *ad, int index, void *val);

// CRYPTO_get_ex_data returns an extra data pointer for a given object, or NULL
// if no such index exists. Each class of object should provide a wrapper
// function.
OPENSSL_EXPORT void *CRYPTO_get_ex_data(const CRYPTO_EX_DATA *ad, int index);

// CRYPTO_new_ex_data initialises a newly allocated |CRYPTO_EX_DATA|.
OPENSSL_EXPORT void CRYPTO_new_ex_data(CRYPTO_EX_DATA *ad);

// CRYPTO_free_ex_data frees |ad|, which is embedded inside |obj|, which is an
// object of the given class.
OPENSSL_EXPORT void CRYPTO_free_ex_data(CRYPTO_EX_DATA_CLASS *ex_data_class,
                                        void *obj, CRYPTO_EX_DATA *ad);


// Endianness conversions.

#if defined(__GNUC__) && __GNUC__ >= 2
static inline uint32_t CRYPTO_bswap4(uint32_t x) {
  return __builtin_bswap32(x);
}

static inline uint64_t CRYPTO_bswap8(uint64_t x) {
  return __builtin_bswap64(x);
}
#elif defined(_MSC_VER)
OPENSSL_MSVC_PRAGMA(warning(push, 3))
#include <intrin.h>
OPENSSL_MSVC_PRAGMA(warning(pop))
#pragma intrinsic(_byteswap_uint64, _byteswap_ulong)
static inline uint32_t CRYPTO_bswap4(uint32_t x) {
  return _byteswap_ulong(x);
}

static inline uint64_t CRYPTO_bswap8(uint64_t x) {
  return _byteswap_uint64(x);
}
#else
static inline uint32_t CRYPTO_bswap4(uint32_t x) {
  x = (x >> 16) | (x << 16);
  x = ((x & 0xff00ff00) >> 8) | ((x & 0x00ff00ff) << 8);
  return x;
}

static inline uint64_t CRYPTO_bswap8(uint64_t x) {
  return CRYPTO_bswap4(x >> 32) | (((uint64_t)CRYPTO_bswap4(x)) << 32);
}
#endif


// Language bug workarounds.
//
// Most C standard library functions are undefined if passed NULL, even when the
// corresponding length is zero. This gives them (and, in turn, all functions
// which call them) surprising behavior on empty arrays. Some compilers will
// miscompile code due to this rule. See also
// https://www.imperialviolet.org/2016/06/26/nonnull.html
//
// These wrapper functions behave the same as the corresponding C standard
// functions, but behave as expected when passed NULL if the length is zero.
//
// Note |OPENSSL_memcmp| is a different function from |CRYPTO_memcmp|.

// C++ defines |memchr| as a const-correct overload.
#if defined(__cplusplus)
extern "C++" {

static inline const void *OPENSSL_memchr(const void *s, int c, size_t n) {
  if (n == 0) {
    return NULL;
  }

  return memchr(s, c, n);
}

static inline void *OPENSSL_memchr(void *s, int c, size_t n) {
  if (n == 0) {
    return NULL;
  }

  return memchr(s, c, n);
}

}  // extern "C++"
#else  // __cplusplus

static inline void *OPENSSL_memchr(const void *s, int c, size_t n) {
  if (n == 0) {
    return NULL;
  }

  return memchr(s, c, n);
}

#endif  // __cplusplus

static inline int OPENSSL_memcmp(const void *s1, const void *s2, size_t n) {
  if (n == 0) {
    return 0;
  }

  return memcmp(s1, s2, n);
}

static inline void *OPENSSL_memcpy(void *dst, const void *src, size_t n) {
  if (n == 0) {
    return dst;
  }

  return memcpy(dst, src, n);
}

static inline void *OPENSSL_memmove(void *dst, const void *src, size_t n) {
  if (n == 0) {
    return dst;
  }

  return memmove(dst, src, n);
}

static inline void *OPENSSL_memset(void *dst, int c, size_t n) {
  if (n == 0) {
    return dst;
  }

  return memset(dst, c, n);
}

#if defined(BORINGSSL_FIPS)
// BORINGSSL_FIPS_abort is called when a FIPS power-on or continuous test
// fails. It prevents any further cryptographic operations by the current
// process.
void BORINGSSL_FIPS_abort(void) __attribute__((noreturn));
#endif

#if defined(__cplusplus)
}  // extern C
#endif

#endif  // OPENSSL_HEADER_CRYPTO_INTERNAL_H
