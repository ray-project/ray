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

#ifndef OPENSSL_HEADER_STACK_H
#define OPENSSL_HEADER_STACK_H

#include <openssl/base.h>

#include <openssl/type_check.h>

#if defined(__cplusplus)
extern "C" {
#endif


// A stack, in OpenSSL, is an array of pointers. They are the most commonly
// used collection object.
//
// This file defines macros for type safe use of the stack functions. A stack
// of a specific type of object has type |STACK_OF(type)|. This can be defined
// (once) with |DEFINE_STACK_OF(type)| and declared where needed with
// |DECLARE_STACK_OF(type)|. For example:
//
//   typedef struct foo_st {
//     int bar;
//   } FOO;
//
//   DEFINE_STACK_OF(FOO);
//
// Although note that the stack will contain /pointers/ to |FOO|.
//
// A macro will be defined for each of the sk_* functions below. For
// STACK_OF(FOO), the macros would be sk_FOO_new, sk_FOO_pop etc.


// stack_cmp_func is a comparison function that returns a value < 0, 0 or > 0
// if |*a| is less than, equal to or greater than |*b|, respectively.  Note the
// extra indirection - the function is given a pointer to a pointer to the
// element. This differs from the usual qsort/bsearch comparison function.
typedef int (*stack_cmp_func)(const void **a, const void **b);

// stack_st contains an array of pointers. It is not designed to be used
// directly, rather the wrapper macros should be used.
typedef struct stack_st {
  // num contains the number of valid pointers in |data|.
  size_t num;
  void **data;
  // sorted is non-zero if the values pointed to by |data| are in ascending
  // order, based on |comp|.
  int sorted;
  // num_alloc contains the number of pointers allocated in the buffer pointed
  // to by |data|, which may be larger than |num|.
  size_t num_alloc;
  // comp is an optional comparison function.
  stack_cmp_func comp;
} _STACK;


#define STACK_OF(type) struct stack_st_##type

#define DECLARE_STACK_OF(type) STACK_OF(type);

// These are the raw stack functions, you shouldn't be using them. Rather you
// should be using the type stack macros implemented above.

// sk_new creates a new, empty stack with the given comparison function, which
// may be zero. It returns the new stack or NULL on allocation failure.
OPENSSL_EXPORT _STACK *sk_new(stack_cmp_func comp);

// sk_new_null creates a new, empty stack. It returns the new stack or NULL on
// allocation failure.
OPENSSL_EXPORT _STACK *sk_new_null(void);

// sk_num returns the number of elements in |s|.
OPENSSL_EXPORT size_t sk_num(const _STACK *sk);

// sk_zero resets |sk| to the empty state but does nothing to free the
// individual elements themselves.
OPENSSL_EXPORT void sk_zero(_STACK *sk);

// sk_value returns the |i|th pointer in |sk|, or NULL if |i| is out of
// range.
OPENSSL_EXPORT void *sk_value(const _STACK *sk, size_t i);

// sk_set sets the |i|th pointer in |sk| to |p| and returns |p|. If |i| is out
// of range, it returns NULL.
OPENSSL_EXPORT void *sk_set(_STACK *sk, size_t i, void *p);

// sk_free frees the given stack and array of pointers, but does nothing to
// free the individual elements. Also see |sk_pop_free|.
OPENSSL_EXPORT void sk_free(_STACK *sk);

// sk_pop_free calls |free_func| on each element in the stack and then frees
// the stack itself.
OPENSSL_EXPORT void sk_pop_free(_STACK *sk, void (*free_func)(void *));

// sk_insert inserts |p| into the stack at index |where|, moving existing
// elements if needed. It returns the length of the new stack, or zero on
// error.
OPENSSL_EXPORT size_t sk_insert(_STACK *sk, void *p, size_t where);

// sk_delete removes the pointer at index |where|, moving other elements down
// if needed. It returns the removed pointer, or NULL if |where| is out of
// range.
OPENSSL_EXPORT void *sk_delete(_STACK *sk, size_t where);

// sk_delete_ptr removes, at most, one instance of |p| from the stack based on
// pointer equality. If an instance of |p| is found then |p| is returned,
// otherwise it returns NULL.
OPENSSL_EXPORT void *sk_delete_ptr(_STACK *sk, void *p);

// sk_find returns the first value in the stack equal to |p|. If a comparison
// function has been set on the stack, equality is defined by it, otherwise
// pointer equality is used. If the stack is sorted, then a binary search is
// used, otherwise a linear search is performed. If a matching element is found,
// its index is written to
// |*out_index| (if |out_index| is not NULL) and one is returned. Otherwise zero
// is returned.
//
// Note this differs from OpenSSL. The type signature is slightly different, and
// OpenSSL's sk_find will implicitly sort |sk| if it has a comparison function
// defined.
OPENSSL_EXPORT int sk_find(const _STACK *sk, size_t *out_index, void *p);

// sk_shift removes and returns the first element in the stack, or returns NULL
// if the stack is empty.
OPENSSL_EXPORT void *sk_shift(_STACK *sk);

// sk_push appends |p| to the stack and returns the length of the new stack, or
// 0 on allocation failure.
OPENSSL_EXPORT size_t sk_push(_STACK *sk, void *p);

// sk_pop returns and removes the last element on the stack, or NULL if the
// stack is empty.
OPENSSL_EXPORT void *sk_pop(_STACK *sk);

// sk_dup performs a shallow copy of a stack and returns the new stack, or NULL
// on error.
OPENSSL_EXPORT _STACK *sk_dup(const _STACK *sk);

// sk_sort sorts the elements of |sk| into ascending order based on the
// comparison function. The stack maintains a |sorted| flag and sorting an
// already sorted stack is a no-op.
OPENSSL_EXPORT void sk_sort(_STACK *sk);

// sk_is_sorted returns one if |sk| is known to be sorted and zero
// otherwise.
OPENSSL_EXPORT int sk_is_sorted(const _STACK *sk);

// sk_set_cmp_func sets the comparison function to be used by |sk| and returns
// the previous one.
OPENSSL_EXPORT stack_cmp_func sk_set_cmp_func(_STACK *sk, stack_cmp_func comp);

// sk_deep_copy performs a copy of |sk| and of each of the non-NULL elements in
// |sk| by using |copy_func|. If an error occurs, |free_func| is used to free
// any copies already made and NULL is returned.
OPENSSL_EXPORT _STACK *sk_deep_copy(const _STACK *sk,
                                    void *(*copy_func)(void *),
                                    void (*free_func)(void *));


// Defining stack types.
//
// This set of macros is used to emit the typed functions that act on a
// |STACK_OF(T)|.

#if !defined(BORINGSSL_NO_CXX)
extern "C++" {
namespace bssl {
namespace internal {
template <typename T>
struct StackTraits {};
}
}
}

#define BORINGSSL_DEFINE_STACK_TRAITS(name, type, is_const) \
  extern "C++" {                                            \
  namespace bssl {                                          \
  namespace internal {                                      \
  template <>                                               \
  struct StackTraits<STACK_OF(name)> {                      \
    static constexpr bool kIsStack = true;                  \
    using Type = type;                                      \
    static constexpr bool kIsConst = is_const;              \
  };                                                        \
  }                                                         \
  }                                                         \
  }

#else
#define BORINGSSL_DEFINE_STACK_TRAITS(name, type, is_const)
#endif

// Stack functions must be tagged unused to support file-local stack types.
// Clang's -Wunused-function only allows unused static inline functions if they
// are defined in a header.

#define BORINGSSL_DEFINE_STACK_OF_IMPL(name, ptrtype, constptrtype)            \
  DECLARE_STACK_OF(name)                                                       \
                                                                               \
  typedef int (*stack_##name##_cmp_func)(constptrtype *a, constptrtype *b);    \
                                                                               \
  static inline OPENSSL_UNUSED STACK_OF(name) *                                \
      sk_##name##_new(stack_##name##_cmp_func comp) {                          \
    return (STACK_OF(name) *)sk_new((stack_cmp_func)comp);                     \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED STACK_OF(name) *sk_##name##_new_null(void) {    \
    return (STACK_OF(name) *)sk_new_null();                                    \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED size_t sk_##name##_num(                         \
      const STACK_OF(name) *sk) {                                              \
    return sk_num((const _STACK *)sk);                                         \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED void sk_##name##_zero(STACK_OF(name) *sk) {     \
    sk_zero((_STACK *)sk);                                                     \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED ptrtype sk_##name##_value(                      \
      const STACK_OF(name) *sk, size_t i) {                                    \
    return (ptrtype)sk_value((const _STACK *)sk, i);                           \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED ptrtype sk_##name##_set(STACK_OF(name) *sk,     \
                                                       size_t i, ptrtype p) {  \
    return (ptrtype)sk_set((_STACK *)sk, i, (void *)p);                        \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED void sk_##name##_free(STACK_OF(name) *sk) {     \
    sk_free((_STACK *)sk);                                                     \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED void sk_##name##_pop_free(                      \
      STACK_OF(name) *sk, void (*free_func)(ptrtype p)) {                      \
    sk_pop_free((_STACK *)sk, (void (*)(void *))free_func);                    \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED size_t sk_##name##_insert(                      \
      STACK_OF(name) *sk, ptrtype p, size_t where) {                           \
    return sk_insert((_STACK *)sk, (void *)p, where);                          \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED ptrtype sk_##name##_delete(STACK_OF(name) *sk,  \
                                                          size_t where) {      \
    return (ptrtype)sk_delete((_STACK *)sk, where);                            \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED ptrtype sk_##name##_delete_ptr(                 \
      STACK_OF(name) *sk, ptrtype p) {                                         \
    return (ptrtype)sk_delete_ptr((_STACK *)sk, (void *)p);                    \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED int sk_##name##_find(                           \
      const STACK_OF(name) *sk, size_t *out_index, ptrtype p) {                \
    return sk_find((const _STACK *)sk, out_index, (void *)p);                  \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED ptrtype sk_##name##_shift(STACK_OF(name) *sk) { \
    return (ptrtype)sk_shift((_STACK *)sk);                                    \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED size_t sk_##name##_push(STACK_OF(name) *sk,     \
                                                       ptrtype p) {            \
    return sk_push((_STACK *)sk, (void *)p);                                   \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED ptrtype sk_##name##_pop(STACK_OF(name) *sk) {   \
    return (ptrtype)sk_pop((_STACK *)sk);                                      \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED STACK_OF(name) *                                \
      sk_##name##_dup(const STACK_OF(name) *sk) {                              \
    return (STACK_OF(name) *)sk_dup((const _STACK *)sk);                       \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED void sk_##name##_sort(STACK_OF(name) *sk) {     \
    sk_sort((_STACK *)sk);                                                     \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED int sk_##name##_is_sorted(                      \
      const STACK_OF(name) *sk) {                                              \
    return sk_is_sorted((const _STACK *)sk);                                   \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED stack_##name##_cmp_func                         \
      sk_##name##_set_cmp_func(STACK_OF(name) *sk,                             \
                               stack_##name##_cmp_func comp) {                 \
    return (stack_##name##_cmp_func)sk_set_cmp_func((_STACK *)sk,              \
                                                    (stack_cmp_func)comp);     \
  }                                                                            \
                                                                               \
  static inline OPENSSL_UNUSED STACK_OF(name) *                                \
      sk_##name##_deep_copy(const STACK_OF(name) *sk,                          \
                            ptrtype(*copy_func)(ptrtype),                      \
                            void (*free_func)(ptrtype)) {                      \
    return (STACK_OF(name) *)sk_deep_copy((const _STACK *)sk,                  \
                                          (void *(*)(void *))copy_func,        \
                                          (void (*)(void *))free_func);        \
  }

// DEFINE_NAMED_STACK_OF defines |STACK_OF(name)| to be a stack whose elements
// are |type| *.
#define DEFINE_NAMED_STACK_OF(name, type)                    \
  BORINGSSL_DEFINE_STACK_OF_IMPL(name, type *, const type *) \
  BORINGSSL_DEFINE_STACK_TRAITS(name, type, false)

// DEFINE_STACK_OF defines |STACK_OF(type)| to be a stack whose elements are
// |type| *.
#define DEFINE_STACK_OF(type) DEFINE_NAMED_STACK_OF(type, type)

// DEFINE_CONST_STACK_OF defines |STACK_OF(type)| to be a stack whose elements
// are const |type| *.
#define DEFINE_CONST_STACK_OF(type)                                \
  BORINGSSL_DEFINE_STACK_OF_IMPL(type, const type *, const type *) \
  BORINGSSL_DEFINE_STACK_TRAITS(type, const type, true)

// DEFINE_SPECIAL_STACK_OF defines |STACK_OF(type)| to be a stack whose elements
// are |type|, where |type| must be a typedef for a pointer.
#define DEFINE_SPECIAL_STACK_OF(type)                          \
  OPENSSL_COMPILE_ASSERT(sizeof(type) == sizeof(void *),       \
                         special_stack_of_non_pointer_##type); \
  BORINGSSL_DEFINE_STACK_OF_IMPL(type, type, const type)


typedef char *OPENSSL_STRING;

DEFINE_STACK_OF(void)
DEFINE_SPECIAL_STACK_OF(OPENSSL_STRING)


#if defined(__cplusplus)
}  // extern C
#endif

#if !defined(BORINGSSL_NO_CXX)
extern "C++" {

#include <type_traits>

namespace bssl {

namespace internal {

// Stacks defined with |DEFINE_CONST_STACK_OF| are freed with |sk_free|.
template <typename Stack>
struct DeleterImpl<
    Stack, typename std::enable_if<StackTraits<Stack>::kIsConst>::type> {
  static void Free(Stack *sk) { sk_free(reinterpret_cast<_STACK *>(sk)); }
};

// Stacks defined with |DEFINE_STACK_OF| are freed with |sk_pop_free| and the
// corresponding type's deleter.
template <typename Stack>
struct DeleterImpl<
    Stack, typename std::enable_if<!StackTraits<Stack>::kIsConst>::type> {
  static void Free(Stack *sk) {
    sk_pop_free(
        reinterpret_cast<_STACK *>(sk),
        reinterpret_cast<void (*)(void *)>(
            DeleterImpl<typename StackTraits<Stack>::Type>::Free));
  }
};

template <typename Stack>
class StackIteratorImpl {
 public:
  using Type = typename StackTraits<Stack>::Type;
  // Iterators must be default-constructable.
  StackIteratorImpl() : sk_(nullptr), idx_(0) {}
  StackIteratorImpl(const Stack *sk, size_t idx) : sk_(sk), idx_(idx) {}

  bool operator==(StackIteratorImpl other) const {
    return sk_ == other.sk_ && idx_ == other.idx_;
  }
  bool operator!=(StackIteratorImpl other) const {
    return !(*this == other);
  }

  Type *operator*() const {
    return reinterpret_cast<Type *>(
        sk_value(reinterpret_cast<const _STACK *>(sk_), idx_));
  }

  StackIteratorImpl &operator++(/* prefix */) {
    idx_++;
    return *this;
  }

  StackIteratorImpl operator++(int /* postfix */) {
    StackIteratorImpl copy(*this);
    ++(*this);
    return copy;
  }

 private:
  const Stack *sk_;
  size_t idx_;
};

template <typename Stack>
using StackIterator = typename std::enable_if<StackTraits<Stack>::kIsStack,
                                              StackIteratorImpl<Stack>>::type;

}  // namespace internal

// PushToStack pushes |elem| to |sk|. It returns true on success and false on
// allocation failure.
template <typename Stack>
static inline
    typename std::enable_if<!internal::StackTraits<Stack>::kIsConst, bool>::type
    PushToStack(Stack *sk,
                UniquePtr<typename internal::StackTraits<Stack>::Type> elem) {
  if (!sk_push(reinterpret_cast<_STACK *>(sk), elem.get())) {
    return false;
  }
  // sk_push takes ownership on success.
  elem.release();
  return true;
}

}  // namespace bssl

// Define begin() and end() for stack types so C++ range for loops work.
template <typename Stack>
static inline bssl::internal::StackIterator<Stack> begin(const Stack *sk) {
  return bssl::internal::StackIterator<Stack>(sk, 0);
}

template <typename Stack>
static inline bssl::internal::StackIterator<Stack> end(const Stack *sk) {
  return bssl::internal::StackIterator<Stack>(
      sk, sk_num(reinterpret_cast<const _STACK *>(sk)));
}

}  // extern C++
#endif

#endif  // OPENSSL_HEADER_STACK_H
