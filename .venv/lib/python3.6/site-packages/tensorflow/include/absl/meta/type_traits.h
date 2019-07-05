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
// type_traits.h
// -----------------------------------------------------------------------------
//
// This file contains C++11-compatible versions of standard <type_traits> API
// functions for determining the characteristics of types. Such traits can
// support type inference, classification, and transformation, as well as
// make it easier to write templates based on generic type behavior.
//
// See http://en.cppreference.com/w/cpp/header/type_traits
//
// WARNING: use of many of the constructs in this header will count as "complex
// template metaprogramming", so before proceeding, please carefully consider
// https://google.github.io/styleguide/cppguide.html#Template_metaprogramming
//
// WARNING: using template metaprogramming to detect or depend on API
// features is brittle and not guaranteed. Neither the standard library nor
// Abseil provides any guarantee that APIs are stable in the face of template
// metaprogramming. Use with caution.
#ifndef ABSL_META_TYPE_TRAITS_H_
#define ABSL_META_TYPE_TRAITS_H_

#include <stddef.h>
#include <functional>
#include <type_traits>

#include "absl/base/config.h"

namespace absl {

namespace type_traits_internal {

template <typename... Ts>
struct VoidTImpl {
  using type = void;
};

// This trick to retrieve a default alignment is necessary for our
// implementation of aligned_storage_t to be consistent with any implementation
// of std::aligned_storage.
template <size_t Len, typename T = std::aligned_storage<Len>>
struct default_alignment_of_aligned_storage;

template <size_t Len, size_t Align>
struct default_alignment_of_aligned_storage<Len,
                                            std::aligned_storage<Len, Align>> {
  static constexpr size_t value = Align;
};

////////////////////////////////
// Library Fundamentals V2 TS //
////////////////////////////////

// NOTE: The `is_detected` family of templates here differ from the library
// fundamentals specification in that for library fundamentals, `Op<Args...>` is
// evaluated as soon as the type `is_detected<Op, Args...>` undergoes
// substitution, regardless of whether or not the `::value` is accessed. That
// is inconsistent with all other standard traits and prevents lazy evaluation
// in larger contexts (such as if the `is_detected` check is a trailing argument
// of a `conjunction`. This implementation opts to instead be lazy in the same
// way that the standard traits are (this "defect" of the detection idiom
// specifications has been reported).

template <class Enabler, template <class...> class Op, class... Args>
struct is_detected_impl {
  using type = std::false_type;
};

template <template <class...> class Op, class... Args>
struct is_detected_impl<typename VoidTImpl<Op<Args...>>::type, Op, Args...> {
  using type = std::true_type;
};

template <template <class...> class Op, class... Args>
struct is_detected : is_detected_impl<void, Op, Args...>::type {};

template <class Enabler, class To, template <class...> class Op, class... Args>
struct is_detected_convertible_impl {
  using type = std::false_type;
};

template <class To, template <class...> class Op, class... Args>
struct is_detected_convertible_impl<
    typename std::enable_if<std::is_convertible<Op<Args...>, To>::value>::type,
    To, Op, Args...> {
  using type = std::true_type;
};

template <class To, template <class...> class Op, class... Args>
struct is_detected_convertible
    : is_detected_convertible_impl<void, To, Op, Args...>::type {};

template <typename T>
using IsCopyAssignableImpl =
    decltype(std::declval<T&>() = std::declval<const T&>());

template <typename T>
using IsMoveAssignableImpl = decltype(std::declval<T&>() = std::declval<T&&>());

}  // namespace type_traits_internal

template <typename T>
struct is_copy_assignable : type_traits_internal::is_detected<
                                type_traits_internal::IsCopyAssignableImpl, T> {
};

template <typename T>
struct is_move_assignable : type_traits_internal::is_detected<
                                type_traits_internal::IsMoveAssignableImpl, T> {
};

// void_t()
//
// Ignores the type of any its arguments and returns `void`. In general, this
// metafunction allows you to create a general case that maps to `void` while
// allowing specializations that map to specific types.
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::void_t` metafunction.
//
// NOTE: `absl::void_t` does not use the standard-specified implementation so
// that it can remain compatible with gcc < 5.1. This can introduce slightly
// different behavior, such as when ordering partial specializations.
template <typename... Ts>
using void_t = typename type_traits_internal::VoidTImpl<Ts...>::type;

// conjunction
//
// Performs a compile-time logical AND operation on the passed types (which
// must have  `::value` members convertible to `bool`. Short-circuits if it
// encounters any `false` members (and does not compare the `::value` members
// of any remaining arguments).
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::conjunction` metafunction.
template <typename... Ts>
struct conjunction;

template <typename T, typename... Ts>
struct conjunction<T, Ts...>
    : std::conditional<T::value, conjunction<Ts...>, T>::type {};

template <typename T>
struct conjunction<T> : T {};

template <>
struct conjunction<> : std::true_type {};

// disjunction
//
// Performs a compile-time logical OR operation on the passed types (which
// must have  `::value` members convertible to `bool`. Short-circuits if it
// encounters any `true` members (and does not compare the `::value` members
// of any remaining arguments).
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::disjunction` metafunction.
template <typename... Ts>
struct disjunction;

template <typename T, typename... Ts>
struct disjunction<T, Ts...> :
      std::conditional<T::value, T, disjunction<Ts...>>::type {};

template <typename T>
struct disjunction<T> : T {};

template <>
struct disjunction<> : std::false_type {};

// negation
//
// Performs a compile-time logical NOT operation on the passed type (which
// must have  `::value` members convertible to `bool`.
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::negation` metafunction.
template <typename T>
struct negation : std::integral_constant<bool, !T::value> {};

// is_trivially_destructible()
//
// Determines whether the passed type `T` is trivially destructable.
//
// This metafunction is designed to be a drop-in replacement for the C++11
// `std::is_trivially_destructible()` metafunction for platforms that have
// incomplete C++11 support (such as libstdc++ 4.x). On any platforms that do
// fully support C++11, we check whether this yields the same result as the std
// implementation.
//
// NOTE: the extensions (__has_trivial_xxx) are implemented in gcc (version >=
// 4.3) and clang. Since we are supporting libstdc++ > 4.7, they should always
// be present. These  extensions are documented at
// https://gcc.gnu.org/onlinedocs/gcc/Type-Traits.html#Type-Traits.
template <typename T>
struct is_trivially_destructible
    : std::integral_constant<bool, __has_trivial_destructor(T) &&
                                   std::is_destructible<T>::value> {
#ifdef ABSL_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE
 private:
  static constexpr bool compliant = std::is_trivially_destructible<T>::value ==
                                    is_trivially_destructible::value;
  static_assert(compliant || std::is_trivially_destructible<T>::value,
                "Not compliant with std::is_trivially_destructible; "
                "Standard: false, Implementation: true");
  static_assert(compliant || !std::is_trivially_destructible<T>::value,
                "Not compliant with std::is_trivially_destructible; "
                "Standard: true, Implementation: false");
#endif  // ABSL_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE
};

// is_trivially_default_constructible()
//
// Determines whether the passed type `T` is trivially default constructible.
//
// This metafunction is designed to be a drop-in replacement for the C++11
// `std::is_trivially_default_constructible()` metafunction for platforms that
// have incomplete C++11 support (such as libstdc++ 4.x). On any platforms that
// do fully support C++11, we check whether this yields the same result as the
// std implementation.
//
// NOTE: according to the C++ standard, Section: 20.15.4.3 [meta.unary.prop]
// "The predicate condition for a template specialization is_constructible<T,
// Args...> shall be satisfied if and only if the following variable
// definition would be well-formed for some invented variable t:
//
// T t(declval<Args>()...);
//
// is_trivially_constructible<T, Args...> additionally requires that the
// variable definition does not call any operation that is not trivial.
// For the purposes of this check, the call to std::declval is considered
// trivial."
//
// Notes from http://en.cppreference.com/w/cpp/types/is_constructible:
// In many implementations, is_nothrow_constructible also checks if the
// destructor throws because it is effectively noexcept(T(arg)). Same
// applies to is_trivially_constructible, which, in these implementations, also
// requires that the destructor is trivial.
// GCC bug 51452: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=51452
// LWG issue 2116: http://cplusplus.github.io/LWG/lwg-active.html#2116.
//
// "T obj();" need to be well-formed and not call any nontrivial operation.
// Nontrivially destructible types will cause the expression to be nontrivial.
template <typename T>
struct is_trivially_default_constructible
    : std::integral_constant<bool, __has_trivial_constructor(T) &&
                                   std::is_default_constructible<T>::value &&
                                   is_trivially_destructible<T>::value> {
#ifdef ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE
 private:
  static constexpr bool compliant =
      std::is_trivially_default_constructible<T>::value ==
      is_trivially_default_constructible::value;
  static_assert(compliant || std::is_trivially_default_constructible<T>::value,
                "Not compliant with std::is_trivially_default_constructible; "
                "Standard: false, Implementation: true");
  static_assert(compliant || !std::is_trivially_default_constructible<T>::value,
                "Not compliant with std::is_trivially_default_constructible; "
                "Standard: true, Implementation: false");
#endif  // ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE
};

// is_trivially_copy_constructible()
//
// Determines whether the passed type `T` is trivially copy constructible.
//
// This metafunction is designed to be a drop-in replacement for the C++11
// `std::is_trivially_copy_constructible()` metafunction for platforms that have
// incomplete C++11 support (such as libstdc++ 4.x). On any platforms that do
// fully support C++11, we check whether this yields the same result as the std
// implementation.
//
// NOTE: `T obj(declval<const T&>());` needs to be well-formed and not call any
// nontrivial operation.  Nontrivially destructible types will cause the
// expression to be nontrivial.
template <typename T>
struct is_trivially_copy_constructible
    : std::integral_constant<bool, __has_trivial_copy(T) &&
                                   std::is_copy_constructible<T>::value &&
                                   is_trivially_destructible<T>::value> {
#ifdef ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE
 private:
  static constexpr bool compliant =
      std::is_trivially_copy_constructible<T>::value ==
      is_trivially_copy_constructible::value;
  static_assert(compliant || std::is_trivially_copy_constructible<T>::value,
                "Not compliant with std::is_trivially_copy_constructible; "
                "Standard: false, Implementation: true");
  static_assert(compliant || !std::is_trivially_copy_constructible<T>::value,
                "Not compliant with std::is_trivially_copy_constructible; "
                "Standard: true, Implementation: false");
#endif  // ABSL_HAVE_STD_IS_TRIVIALLY_CONSTRUCTIBLE
};

// is_trivially_copy_assignable()
//
// Determines whether the passed type `T` is trivially copy assignable.
//
// This metafunction is designed to be a drop-in replacement for the C++11
// `std::is_trivially_copy_assignable()` metafunction for platforms that have
// incomplete C++11 support (such as libstdc++ 4.x). On any platforms that do
// fully support C++11, we check whether this yields the same result as the std
// implementation.
//
// NOTE: `is_assignable<T, U>::value` is `true` if the expression
// `declval<T>() = declval<U>()` is well-formed when treated as an unevaluated
// operand. `is_trivially_assignable<T, U>` requires the assignment to call no
// operation that is not trivial. `is_trivially_copy_assignable<T>` is simply
// `is_trivially_assignable<T&, const T&>`.
template <typename T>
struct is_trivially_copy_assignable
    : std::integral_constant<
          bool, __has_trivial_assign(typename std::remove_reference<T>::type) &&
                    absl::is_copy_assignable<T>::value> {
#ifdef ABSL_HAVE_STD_IS_TRIVIALLY_ASSIGNABLE
 private:
  static constexpr bool compliant =
      std::is_trivially_copy_assignable<T>::value ==
      is_trivially_copy_assignable::value;
  static_assert(compliant || std::is_trivially_copy_assignable<T>::value,
                "Not compliant with std::is_trivially_copy_assignable; "
                "Standard: false, Implementation: true");
  static_assert(compliant || !std::is_trivially_copy_assignable<T>::value,
                "Not compliant with std::is_trivially_copy_assignable; "
                "Standard: true, Implementation: false");
#endif  // ABSL_HAVE_STD_IS_TRIVIALLY_ASSIGNABLE
};

// -----------------------------------------------------------------------------
// C++14 "_t" trait aliases
// -----------------------------------------------------------------------------

template <typename T>
using remove_cv_t = typename std::remove_cv<T>::type;

template <typename T>
using remove_const_t = typename std::remove_const<T>::type;

template <typename T>
using remove_volatile_t = typename std::remove_volatile<T>::type;

template <typename T>
using add_cv_t = typename std::add_cv<T>::type;

template <typename T>
using add_const_t = typename std::add_const<T>::type;

template <typename T>
using add_volatile_t = typename std::add_volatile<T>::type;

template <typename T>
using remove_reference_t = typename std::remove_reference<T>::type;

template <typename T>
using add_lvalue_reference_t = typename std::add_lvalue_reference<T>::type;

template <typename T>
using add_rvalue_reference_t = typename std::add_rvalue_reference<T>::type;

template <typename T>
using remove_pointer_t = typename std::remove_pointer<T>::type;

template <typename T>
using add_pointer_t = typename std::add_pointer<T>::type;

template <typename T>
using make_signed_t = typename std::make_signed<T>::type;

template <typename T>
using make_unsigned_t = typename std::make_unsigned<T>::type;

template <typename T>
using remove_extent_t = typename std::remove_extent<T>::type;

template <typename T>
using remove_all_extents_t = typename std::remove_all_extents<T>::type;

template <size_t Len, size_t Align = type_traits_internal::
                          default_alignment_of_aligned_storage<Len>::value>
using aligned_storage_t = typename std::aligned_storage<Len, Align>::type;

template <typename T>
using decay_t = typename std::decay<T>::type;

template <bool B, typename T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

template <bool B, typename T, typename F>
using conditional_t = typename std::conditional<B, T, F>::type;

template <typename... T>
using common_type_t = typename std::common_type<T...>::type;

template <typename T>
using underlying_type_t = typename std::underlying_type<T>::type;

template <typename T>
using result_of_t = typename std::result_of<T>::type;

namespace type_traits_internal {
template <typename Key, typename = size_t>
struct IsHashable : std::false_type {};

template <typename Key>
struct IsHashable<Key,
                  decltype(std::declval<std::hash<Key>>()(std::declval<Key>()))>
    : std::true_type {};

template <typename Key>
struct IsHashEnabled
    : absl::conjunction<std::is_default_constructible<std::hash<Key>>,
                        std::is_copy_constructible<std::hash<Key>>,
                        std::is_destructible<std::hash<Key>>,
                        absl::is_copy_assignable<std::hash<Key>>,
                        IsHashable<Key>> {};

}  // namespace type_traits_internal

}  // namespace absl

#endif  // ABSL_META_TYPE_TRAITS_H_
