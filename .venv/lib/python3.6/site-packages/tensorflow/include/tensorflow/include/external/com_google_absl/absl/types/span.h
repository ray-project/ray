//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// span.h
// -----------------------------------------------------------------------------
//
// This header file defines a `Span<T>` type for holding a view of an existing
// array of data. The `Span` object, much like the `absl::string_view` object,
// does not own such data itself. A span provides a lightweight way to pass
// around view of such data.
//
// Additionally, this header file defines `MakeSpan()` and `MakeConstSpan()`
// factory functions, for clearly creating spans of type `Span<T>` or read-only
// `Span<const T>` when such types may be difficult to identify due to issues
// with implicit conversion.
//
// The C++ standards committee currently has a proposal for a `std::span` type,
// (http://wg21.link/p0122), which is not yet part of the standard (though may
// become part of C++20). As of August 2017, the differences between
// `absl::Span` and this proposal are:
//    * `absl::Span` uses `size_t` for `size_type`
//    * `absl::Span` has no `operator()`
//    * `absl::Span` has no constructors for `std::unique_ptr` or
//      `std::shared_ptr`
//    * `absl::Span` has the factory functions `MakeSpan()` and
//      `MakeConstSpan()`
//    * `absl::Span` has `front()` and `back()` methods
//    * bounds-checked access to `absl::Span` is accomplished with `at()`
//    * `absl::Span` has compiler-provided move and copy constructors and
//      assignment. This is due to them being specified as `constexpr`, but that
//      implies const in C++11.
//    * `absl::Span` has no `element_type` or `index_type` typedefs
//    * A read-only `absl::Span<const T>` can be implicitly constructed from an
//      initializer list.
//    * `absl::Span` has no `bytes()`, `size_bytes()`, `as_bytes()`, or
//      `as_mutable_bytes()` methods
//    * `absl::Span` has no static extent template parameter, nor constructors
//      which exist only because of the static extent parameter.
//    * `absl::Span` has an explicit mutable-reference constructor
//
// For more information, see the class comments below.
#ifndef ABSL_TYPES_SPAN_H_
#define ABSL_TYPES_SPAN_H_

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/algorithm/algorithm.h"
#include "absl/base/internal/throw_delegate.h"
#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/base/port.h"
#include "absl/meta/type_traits.h"

namespace absl {

template <typename T>
class Span;

namespace span_internal {
// A constexpr min function
constexpr size_t Min(size_t a, size_t b) noexcept { return a < b ? a : b; }

// Wrappers for access to container data pointers.
template <typename C>
constexpr auto GetDataImpl(C& c, char) noexcept  // NOLINT(runtime/references)
    -> decltype(c.data()) {
  return c.data();
}

// Before C++17, string::data returns a const char* in all cases.
inline char* GetDataImpl(std::string& s,  // NOLINT(runtime/references)
                         int) noexcept {
  return &s[0];
}

template <typename C>
constexpr auto GetData(C& c) noexcept  // NOLINT(runtime/references)
    -> decltype(GetDataImpl(c, 0)) {
  return GetDataImpl(c, 0);
}

// Detection idioms for size() and data().
template <typename C>
using HasSize =
    std::is_integral<absl::decay_t<decltype(std::declval<C&>().size())>>;

// We want to enable conversion from vector<T*> to Span<const T* const> but
// disable conversion from vector<Derived> to Span<Base>. Here we use
// the fact that U** is convertible to Q* const* if and only if Q is the same
// type or a more cv-qualified version of U.  We also decay the result type of
// data() to avoid problems with classes which have a member function data()
// which returns a reference.
template <typename T, typename C>
using HasData =
    std::is_convertible<absl::decay_t<decltype(GetData(std::declval<C&>()))>*,
                        T* const*>;

// Extracts value type from a Container
template <typename C>
struct ElementType {
  using type = typename absl::remove_reference_t<C>::value_type;
};

template <typename T, size_t N>
struct ElementType<T (&)[N]> {
  using type = T;
};

template <typename C>
using ElementT = typename ElementType<C>::type;

template <typename T>
using EnableIfMutable =
    typename std::enable_if<!std::is_const<T>::value, int>::type;

template <typename T>
bool EqualImpl(Span<T> a, Span<T> b) {
  static_assert(std::is_const<T>::value, "");
  return absl::equal(a.begin(), a.end(), b.begin(), b.end());
}

template <typename T>
bool LessThanImpl(Span<T> a, Span<T> b) {
  static_assert(std::is_const<T>::value, "");
  return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

// The `IsConvertible` classes here are needed because of the
// `std::is_convertible` bug in libcxx when compiled with GCC. This build
// configuration is used by Android NDK toolchain. Reference link:
// https://bugs.llvm.org/show_bug.cgi?id=27538.
template <typename From, typename To>
struct IsConvertibleHelper {
 private:
  static std::true_type testval(To);
  static std::false_type testval(...);

 public:
  using type = decltype(testval(std::declval<From>()));
};

template <typename From, typename To>
struct IsConvertible : IsConvertibleHelper<From, To>::type {};

// TODO(zhangxy): replace `IsConvertible` with `std::is_convertible` once the
// older version of libcxx is not supported.
template <typename From, typename To>
using EnableIfConvertibleToSpanConst =
    typename std::enable_if<IsConvertible<From, Span<const To>>::value>::type;
}  // namespace span_internal

//------------------------------------------------------------------------------
// Span
//------------------------------------------------------------------------------
//
// A `Span` is an "array view" type for holding a view of a contiguous data
// array; the `Span` object does not and cannot own such data itself. A span
// provides an easy way to provide overloads for anything operating on
// contiguous sequences without needing to manage pointers and array lengths
// manually.

// A span is conceptually a pointer (ptr) and a length (size) into an already
// existing array of contiguous memory; the array it represents references the
// elements "ptr[0] .. ptr[size-1]". Passing a properly-constructed `Span`
// instead of raw pointers avoids many issues related to index out of bounds
// errors.
//
// Spans may also be constructed from containers holding contiguous sequences.
// Such containers must supply `data()` and `size() const` methods (e.g
// `std::vector<T>`, `absl::InlinedVector<T, N>`). All implicit conversions to
// `absl::Span` from such containers will create spans of type `const T`;
// spans which can mutate their values (of type `T`) must use explicit
// constructors.
//
// A `Span<T>` is somewhat analogous to an `absl::string_view`, but for an array
// of elements of type `T`. A user of `Span` must ensure that the data being
// pointed to outlives the `Span` itself.
//
// You can construct a `Span<T>` in several ways:
//
//   * Explicitly from a reference to a container type
//   * Explicitly from a pointer and size
//   * Implicitly from a container type (but only for spans of type `const T`)
//   * Using the `MakeSpan()` or `MakeConstSpan()` factory functions.
//
// Examples:
//
//   // Construct a Span explicitly from a container:
//   std::vector<int> v = {1, 2, 3, 4, 5};
//   auto span = absl::Span<const int>(v);
//
//   // Construct a Span explicitly from a C-style array:
//   int a[5] =  {1, 2, 3, 4, 5};
//   auto span = absl::Span<const int>(a);
//
//   // Construct a Span implicitly from a container
//   void MyRoutine(absl::Span<const int> a) {
//     ...
//   }
//   std::vector v = {1,2,3,4,5};
//   MyRoutine(v)                     // convert to Span<const T>
//
// Note that `Span` objects, in addition to requiring that the memory they
// point to remains alive, must also ensure that such memory does not get
// reallocated. Therefore, to avoid undefined behavior, containers with
// associated span views should not invoke operations that may reallocate memory
// (such as resizing) or invalidate iterators into the container.
//
// One common use for a `Span` is when passing arguments to a routine that can
// accept a variety of array types (e.g. a `std::vector`, `absl::InlinedVector`,
// a C-style array, etc.). Instead of creating overloads for each case, you
// can simply specify a `Span` as the argument to such a routine.
//
// Example:
//
//   void MyRoutine(absl::Span<const int> a) {
//     ...
//   }
//
//   std::vector v = {1,2,3,4,5};
//   MyRoutine(v);
//
//   absl::InlinedVector<int, 4> my_inline_vector;
//   MyRoutine(my_inline_vector);
//
//   // Explicit constructor from pointer,size
//   int* my_array = new int[10];
//   MyRoutine(absl::Span<const int>(my_array, 10));
template <typename T>
class Span {
 private:
  // Used to determine whether a Span can be constructed from a container of
  // type C.
  template <typename C>
  using EnableIfConvertibleFrom =
      typename std::enable_if<span_internal::HasData<T, C>::value &&
                              span_internal::HasSize<C>::value>::type;

  // Used to SFINAE-enable a function when the slice elements are const.
  template <typename U>
  using EnableIfConstView =
      typename std::enable_if<std::is_const<T>::value, U>::type;

  // Used to SFINAE-enable a function when the slice elements are mutable.
  template <typename U>
  using EnableIfMutableView =
      typename std::enable_if<!std::is_const<T>::value, U>::type;

 public:
  using value_type = absl::remove_cv_t<T>;
  using pointer = T*;
  using const_pointer = const T*;
  using reference = T&;
  using const_reference = const T&;
  using iterator = pointer;
  using const_iterator = const_pointer;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  static const size_type npos = ~(size_type(0));

  constexpr Span() noexcept : Span(nullptr, 0) {}
  constexpr Span(pointer array, size_type length) noexcept
      : ptr_(array), len_(length) {}

  // Implicit conversion constructors
  template <size_t N>
  constexpr Span(T (&a)[N]) noexcept  // NOLINT(runtime/explicit)
      : Span(a, N) {}

  // Explicit reference constructor for a mutable `Span<T>` type. Can be
  // replaced with MakeSpan() to infer the type parameter.
  template <typename V, typename = EnableIfConvertibleFrom<V>,
            typename = EnableIfMutableView<V>>
  explicit Span(V& v) noexcept  // NOLINT(runtime/references)
      : Span(span_internal::GetData(v), v.size()) {}

  // Implicit reference constructor for a read-only `Span<const T>` type
  template <typename V, typename = EnableIfConvertibleFrom<V>,
            typename = EnableIfConstView<V>>
  constexpr Span(const V& v) noexcept  // NOLINT(runtime/explicit)
      : Span(span_internal::GetData(v), v.size()) {}

  // Implicit constructor from an initializer list, making it possible to pass a
  // brace-enclosed initializer list to a function expecting a `Span`. Such
  // spans constructed from an initializer list must be of type `Span<const T>`.
  //
  //   void Process(absl::Span<const int> x);
  //   Process({1, 2, 3});
  //
  // Note that as always the array referenced by the span must outlive the span.
  // Since an initializer list constructor acts as if it is fed a temporary
  // array (cf. C++ standard [dcl.init.list]/5), it's safe to use this
  // constructor only when the `std::initializer_list` itself outlives the span.
  // In order to meet this requirement it's sufficient to ensure that neither
  // the span nor a copy of it is used outside of the expression in which it's
  // created:
  //
  //   // Assume that this function uses the array directly, not retaining any
  //   // copy of the span or pointer to any of its elements.
  //   void Process(absl::Span<const int> ints);
  //
  //   // Okay: the std::initializer_list<int> will reference a temporary array
  //   // that isn't destroyed until after the call to Process returns.
  //   Process({ 17, 19 });
  //
  //   // Not okay: the storage used by the std::initializer_list<int> is not
  //   // allowed to be referenced after the first line.
  //   absl::Span<const int> ints = { 17, 19 };
  //   Process(ints);
  //
  //   // Not okay for the same reason as above: even when the elements of the
  //   // initializer list expression are not temporaries the underlying array
  //   // is, so the initializer list must still outlive the span.
  //   const int foo = 17;
  //   absl::Span<const int> ints = { foo };
  //   Process(ints);
  //
  template <typename LazyT = T,
            typename = EnableIfConstView<LazyT>>
  Span(
      std::initializer_list<value_type> v) noexcept  // NOLINT(runtime/explicit)
      : Span(v.begin(), v.size()) {}

  // Accessors

  // Span::data()
  //
  // Returns a pointer to the span's underlying array of data (which is held
  // outside the span).
  constexpr pointer data() const noexcept { return ptr_; }

  // Span::size()
  //
  // Returns the size of this span.
  constexpr size_type size() const noexcept { return len_; }

  // Span::length()
  //
  // Returns the length (size) of this span.
  constexpr size_type length() const noexcept { return size(); }

  // Span::empty()
  //
  // Returns a boolean indicating whether or not this span is considered empty.
  constexpr bool empty() const noexcept { return size() == 0; }

  // Span::operator[]
  //
  // Returns a reference to the i'th element of this span.
  constexpr reference operator[](size_type i) const noexcept {
    // MSVC 2015 accepts this as constexpr, but not ptr_[i]
    return *(data() + i);
  }

  // Span::at()
  //
  // Returns a reference to the i'th element of this span.
  constexpr reference at(size_type i) const {
    return ABSL_PREDICT_TRUE(i < size())  //
               ? *(data() + i)
               : (base_internal::ThrowStdOutOfRange(
                      "Span::at failed bounds check"),
                  *(data() + i));
  }

  // Span::front()
  //
  // Returns a reference to the first element of this span.
  constexpr reference front() const noexcept {
    return ABSL_ASSERT(size() > 0), *data();
  }

  // Span::back()
  //
  // Returns a reference to the last element of this span.
  constexpr reference back() const noexcept {
    return ABSL_ASSERT(size() > 0), *(data() + size() - 1);
  }

  // Span::begin()
  //
  // Returns an iterator to the first element of this span.
  constexpr iterator begin() const noexcept { return data(); }

  // Span::cbegin()
  //
  // Returns a const iterator to the first element of this span.
  constexpr const_iterator cbegin() const noexcept { return begin(); }

  // Span::end()
  //
  // Returns an iterator to the last element of this span.
  constexpr iterator end() const noexcept { return data() + size(); }

  // Span::cend()
  //
  // Returns a const iterator to the last element of this span.
  constexpr const_iterator cend() const noexcept { return end(); }

  // Span::rbegin()
  //
  // Returns a reverse iterator starting at the last element of this span.
  constexpr reverse_iterator rbegin() const noexcept {
    return reverse_iterator(end());
  }

  // Span::crbegin()
  //
  // Returns a reverse const iterator starting at the last element of this span.
  constexpr const_reverse_iterator crbegin() const noexcept { return rbegin(); }

  // Span::rend()
  //
  // Returns a reverse iterator starting at the first element of this span.
  constexpr reverse_iterator rend() const noexcept {
    return reverse_iterator(begin());
  }

  // Span::crend()
  //
  // Returns a reverse iterator starting at the first element of this span.
  constexpr const_reverse_iterator crend() const noexcept { return rend(); }

  // Span mutations

  // Span::remove_prefix()
  //
  // Removes the first `n` elements from the span.
  void remove_prefix(size_type n) noexcept {
    assert(size() >= n);
    ptr_ += n;
    len_ -= n;
  }

  // Span::remove_suffix()
  //
  // Removes the last `n` elements from the span.
  void remove_suffix(size_type n) noexcept {
    assert(size() >= n);
    len_ -= n;
  }

  // Span::subspan()
  //
  // Returns a `Span` starting at element `pos` and of length `len`. Both `pos`
  // and `len` are of type `size_type` and thus non-negative. Parameter `pos`
  // must be <= size(). Any `len` value that points past the end of the span
  // will be trimmed to at most size() - `pos`. A default `len` value of `npos`
  // ensures the returned subspan continues until the end of the span.
  //
  // Examples:
  //
  //   std::vector<int> vec = {10, 11, 12, 13};
  //   absl::MakeSpan(vec).subspan(1, 2);  // {11, 12}
  //   absl::MakeSpan(vec).subspan(2, 8);  // {12, 13}
  //   absl::MakeSpan(vec).subspan(1);     // {11, 12, 13}
  //   absl::MakeSpan(vec).subspan(4);     // {}
  //   absl::MakeSpan(vec).subspan(5);     // throws std::out_of_range
  constexpr Span subspan(size_type pos = 0, size_type len = npos) const {
    return (pos <= size())
               ? Span(data() + pos, span_internal::Min(size() - pos, len))
               : (base_internal::ThrowStdOutOfRange("pos > size()"), Span());
  }

  // Support for absl::Hash.
  template <typename H>
  friend H AbslHashValue(H h, Span v) {
    return H::combine(H::combine_contiguous(std::move(h), v.data(), v.size()),
                      v.size());
  }

 private:
  pointer ptr_;
  size_type len_;
};

template <typename T>
const typename Span<T>::size_type Span<T>::npos;

// Span relationals

// Equality is compared element-by-element, while ordering is lexicographical.
// We provide three overloads for each operator to cover any combination on the
// left or right hand side of mutable Span<T>, read-only Span<const T>, and
// convertible-to-read-only Span<T>.
// TODO(zhangxy): Due to MSVC overload resolution bug with partial ordering
// template functions, 5 overloads per operator is needed as a workaround. We
// should update them to 3 overloads per operator using non-deduced context like
// string_view, i.e.
// - (Span<T>, Span<T>)
// - (Span<T>, non_deduced<Span<const T>>)
// - (non_deduced<Span<const T>>, Span<T>)

// operator==
template <typename T>
bool operator==(Span<T> a, Span<T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}
template <typename T>
bool operator==(Span<const T> a, Span<T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}
template <typename T>
bool operator==(Span<T> a, Span<const T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator==(const U& a, Span<T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator==(Span<T> a, const U& b) {
  return span_internal::EqualImpl<const T>(a, b);
}

// operator!=
template <typename T>
bool operator!=(Span<T> a, Span<T> b) {
  return !(a == b);
}
template <typename T>
bool operator!=(Span<const T> a, Span<T> b) {
  return !(a == b);
}
template <typename T>
bool operator!=(Span<T> a, Span<const T> b) {
  return !(a == b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator!=(const U& a, Span<T> b) {
  return !(a == b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator!=(Span<T> a, const U& b) {
  return !(a == b);
}

// operator<
template <typename T>
bool operator<(Span<T> a, Span<T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}
template <typename T>
bool operator<(Span<const T> a, Span<T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}
template <typename T>
bool operator<(Span<T> a, Span<const T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<(const U& a, Span<T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<(Span<T> a, const U& b) {
  return span_internal::LessThanImpl<const T>(a, b);
}

// operator>
template <typename T>
bool operator>(Span<T> a, Span<T> b) {
  return b < a;
}
template <typename T>
bool operator>(Span<const T> a, Span<T> b) {
  return b < a;
}
template <typename T>
bool operator>(Span<T> a, Span<const T> b) {
  return b < a;
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>(const U& a, Span<T> b) {
  return b < a;
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>(Span<T> a, const U& b) {
  return b < a;
}

// operator<=
template <typename T>
bool operator<=(Span<T> a, Span<T> b) {
  return !(b < a);
}
template <typename T>
bool operator<=(Span<const T> a, Span<T> b) {
  return !(b < a);
}
template <typename T>
bool operator<=(Span<T> a, Span<const T> b) {
  return !(b < a);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<=(const U& a, Span<T> b) {
  return !(b < a);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<=(Span<T> a, const U& b) {
  return !(b < a);
}

// operator>=
template <typename T>
bool operator>=(Span<T> a, Span<T> b) {
  return !(a < b);
}
template <typename T>
bool operator>=(Span<const T> a, Span<T> b) {
  return !(a < b);
}
template <typename T>
bool operator>=(Span<T> a, Span<const T> b) {
  return !(a < b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>=(const U& a, Span<T> b) {
  return !(a < b);
}
template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>=(Span<T> a, const U& b) {
  return !(a < b);
}

// MakeSpan()
//
// Constructs a mutable `Span<T>`, deducing `T` automatically from either a
// container or pointer+size.
//
// Because a read-only `Span<const T>` is implicitly constructed from container
// types regardless of whether the container itself is a const container,
// constructing mutable spans of type `Span<T>` from containers requires
// explicit constructors. The container-accepting version of `MakeSpan()`
// deduces the type of `T` by the constness of the pointer received from the
// container's `data()` member. Similarly, the pointer-accepting version returns
// a `Span<const T>` if `T` is `const`, and a `Span<T>` otherwise.
//
// Examples:
//
//   void MyRoutine(absl::Span<MyComplicatedType> a) {
//     ...
//   };
//   // my_vector is a container of non-const types
//   std::vector<MyComplicatedType> my_vector;
//
//   // Constructing a Span implicitly attempts to create a Span of type
//   // `Span<const T>`
//   MyRoutine(my_vector);                // error, type mismatch
//
//   // Explicitly constructing the Span is verbose
//   MyRoutine(absl::Span<MyComplicatedType>(my_vector));
//
//   // Use MakeSpan() to make an absl::Span<T>
//   MyRoutine(absl::MakeSpan(my_vector));
//
//   // Construct a span from an array ptr+size
//   absl::Span<T> my_span() {
//     return absl::MakeSpan(&array[0], num_elements_);
//   }
//
template <int&... ExplicitArgumentBarrier, typename T>
constexpr Span<T> MakeSpan(T* ptr, size_t size) noexcept {
  return Span<T>(ptr, size);
}

template <int&... ExplicitArgumentBarrier, typename T>
Span<T> MakeSpan(T* begin, T* end) noexcept {
  return ABSL_ASSERT(begin <= end), Span<T>(begin, end - begin);
}

template <int&... ExplicitArgumentBarrier, typename C>
constexpr auto MakeSpan(C& c) noexcept  // NOLINT(runtime/references)
    -> decltype(absl::MakeSpan(span_internal::GetData(c), c.size())) {
  return MakeSpan(span_internal::GetData(c), c.size());
}

template <int&... ExplicitArgumentBarrier, typename T, size_t N>
constexpr Span<T> MakeSpan(T (&array)[N]) noexcept {
  return Span<T>(array, N);
}

// MakeConstSpan()
//
// Constructs a `Span<const T>` as with `MakeSpan`, deducing `T` automatically,
// but always returning a `Span<const T>`.
//
// Examples:
//
//   void ProcessInts(absl::Span<const int> some_ints);
//
//   // Call with a pointer and size.
//   int array[3] = { 0, 0, 0 };
//   ProcessInts(absl::MakeConstSpan(&array[0], 3));
//
//   // Call with a [begin, end) pair.
//   ProcessInts(absl::MakeConstSpan(&array[0], &array[3]));
//
//   // Call directly with an array.
//   ProcessInts(absl::MakeConstSpan(array));
//
//   // Call with a contiguous container.
//   std::vector<int> some_ints = ...;
//   ProcessInts(absl::MakeConstSpan(some_ints));
//   ProcessInts(absl::MakeConstSpan(std::vector<int>{ 0, 0, 0 }));
//
template <int&... ExplicitArgumentBarrier, typename T>
constexpr Span<const T> MakeConstSpan(T* ptr, size_t size) noexcept {
  return Span<const T>(ptr, size);
}

template <int&... ExplicitArgumentBarrier, typename T>
Span<const T> MakeConstSpan(T* begin, T* end) noexcept {
  return ABSL_ASSERT(begin <= end), Span<const T>(begin, end - begin);
}

template <int&... ExplicitArgumentBarrier, typename C>
constexpr auto MakeConstSpan(const C& c) noexcept -> decltype(MakeSpan(c)) {
  return MakeSpan(c);
}

template <int&... ExplicitArgumentBarrier, typename T, size_t N>
constexpr Span<const T> MakeConstSpan(const T (&array)[N]) noexcept {
  return Span<const T>(array, N);
}
}  // namespace absl
#endif  // ABSL_TYPES_SPAN_H_
