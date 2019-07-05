// Copyright 2018 The Abseil Authors.
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
// Helper class to perform the Empty Base Optimization.
// Ts can contain classes and non-classes, empty or not. For the ones that
// are empty classes, we perform the optimization. If all types in Ts are empty
// classes, then CompressedTuple<Ts...> is itself an empty class.
//
// To access the members, use member get<N>() function.
//
// Eg:
//   absl::container_internal::CompressedTuple<int, T1, T2, T3> value(7, t1, t2,
//                                                                    t3);
//   assert(value.get<0>() == 7);
//   T1& t1 = value.get<1>();
//   const T2& t2 = value.get<2>();
//   ...
//
// http://en.cppreference.com/w/cpp/language/ebo

#ifndef ABSL_CONTAINER_INTERNAL_COMPRESSED_TUPLE_H_
#define ABSL_CONTAINER_INTERNAL_COMPRESSED_TUPLE_H_

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/utility/utility.h"

#ifdef _MSC_VER
// We need to mark these classes with this declspec to ensure that
// CompressedTuple happens.
#define ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC __declspec(empty_bases)
#else  // _MSC_VER
#define ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC
#endif  // _MSC_VER

namespace absl {
namespace container_internal {

template <typename... Ts>
class CompressedTuple;

namespace internal_compressed_tuple {

template <typename D, size_t I>
struct Elem;
template <typename... B, size_t I>
struct Elem<CompressedTuple<B...>, I>
    : std::tuple_element<I, std::tuple<B...>> {};
template <typename D, size_t I>
using ElemT = typename Elem<D, I>::type;

// Use the __is_final intrinsic if available. Where it's not available, classes
// declared with the 'final' specifier cannot be used as CompressedTuple
// elements.
// TODO(sbenza): Replace this with std::is_final in C++14.
template <typename T>
constexpr bool IsFinal() {
#if defined(__clang__) || defined(__GNUC__)
  return __is_final(T);
#else
  return false;
#endif
}

template <typename T>
constexpr bool ShouldUseBase() {
  return std::is_class<T>::value && std::is_empty<T>::value && !IsFinal<T>();
}

// The storage class provides two specializations:
//  - For empty classes, it stores T as a base class.
//  - For everything else, it stores T as a member.
template <typename D, size_t I, bool = ShouldUseBase<ElemT<D, I>>()>
struct Storage {
  using T = ElemT<D, I>;
  T value;
  constexpr Storage() = default;
  explicit constexpr Storage(T&& v) : value(absl::forward<T>(v)) {}
  constexpr const T& get() const& { return value; }
  T& get() & { return value; }
  constexpr const T&& get() const&& { return absl::move(*this).value; }
  T&& get() && { return std::move(*this).value; }
};

template <typename D, size_t I>
struct ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC Storage<D, I, true>
    : ElemT<D, I> {
  using T = internal_compressed_tuple::ElemT<D, I>;
  constexpr Storage() = default;
  explicit constexpr Storage(T&& v) : T(absl::forward<T>(v)) {}
  constexpr const T& get() const& { return *this; }
  T& get() & { return *this; }
  constexpr const T&& get() const&& { return absl::move(*this); }
  T&& get() && { return std::move(*this); }
};

template <typename D, typename I>
struct ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC CompressedTupleImpl;

template <typename... Ts, size_t... I>
struct ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC
    CompressedTupleImpl<CompressedTuple<Ts...>, absl::index_sequence<I...>>
    // We use the dummy identity function through std::integral_constant to
    // convince MSVC of accepting and expanding I in that context. Without it
    // you would get:
    //   error C3548: 'I': parameter pack cannot be used in this context
    : Storage<CompressedTuple<Ts...>,
              std::integral_constant<size_t, I>::value>... {
  constexpr CompressedTupleImpl() = default;
  explicit constexpr CompressedTupleImpl(Ts&&... args)
      : Storage<CompressedTuple<Ts...>, I>(absl::forward<Ts>(args))... {}
};

}  // namespace internal_compressed_tuple

// Helper class to perform the Empty Base Class Optimization.
// Ts can contain classes and non-classes, empty or not. For the ones that
// are empty classes, we perform the CompressedTuple. If all types in Ts are
// empty classes, then CompressedTuple<Ts...> is itself an empty class.
//
// To access the members, use member .get<N>() function.
//
// Eg:
//   absl::container_internal::CompressedTuple<int, T1, T2, T3> value(7, t1, t2,
//                                                                    t3);
//   assert(value.get<0>() == 7);
//   T1& t1 = value.get<1>();
//   const T2& t2 = value.get<2>();
//   ...
//
// http://en.cppreference.com/w/cpp/language/ebo
template <typename... Ts>
class ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC CompressedTuple
    : private internal_compressed_tuple::CompressedTupleImpl<
          CompressedTuple<Ts...>, absl::index_sequence_for<Ts...>> {
 private:
  template <int I>
  using ElemT = internal_compressed_tuple::ElemT<CompressedTuple, I>;

 public:
  constexpr CompressedTuple() = default;
  explicit constexpr CompressedTuple(Ts... base)
      : CompressedTuple::CompressedTupleImpl(absl::forward<Ts>(base)...) {}

  template <int I>
  ElemT<I>& get() & {
    return internal_compressed_tuple::Storage<CompressedTuple, I>::get();
  }

  template <int I>
  constexpr const ElemT<I>& get() const& {
    return internal_compressed_tuple::Storage<CompressedTuple, I>::get();
  }

  template <int I>
  ElemT<I>&& get() && {
    return std::move(*this)
        .internal_compressed_tuple::template Storage<CompressedTuple, I>::get();
  }

  template <int I>
  constexpr const ElemT<I>&& get() const&& {
    return absl::move(*this)
        .internal_compressed_tuple::template Storage<CompressedTuple, I>::get();
  }
};

// Explicit specialization for a zero-element tuple
// (needed to avoid ambiguous overloads for the default constructor).
template <>
class ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC CompressedTuple<> {};

}  // namespace container_internal
}  // namespace absl

#undef ABSL_INTERNAL_COMPRESSED_TUPLE_DECLSPEC

#endif  // ABSL_CONTAINER_INTERNAL_COMPRESSED_TUPLE_H_
