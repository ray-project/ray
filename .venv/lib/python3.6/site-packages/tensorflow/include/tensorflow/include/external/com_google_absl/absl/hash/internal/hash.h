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
// -----------------------------------------------------------------------------
// File: hash.h
// -----------------------------------------------------------------------------
//
#ifndef ABSL_HASH_INTERNAL_HASH_H_
#define ABSL_HASH_INTERNAL_HASH_H_

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <deque>
#include <forward_list>
#include <functional>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/internal/endian.h"
#include "absl/base/port.h"
#include "absl/container/fixed_array.h"
#include "absl/meta/type_traits.h"
#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "absl/utility/utility.h"
#include "absl/hash/internal/city.h"

namespace absl {
namespace hash_internal {

// HashStateBase
//
// A hash state object represents an intermediate state in the computation
// of an unspecified hash algorithm. `HashStateBase` provides a CRTP style
// base class for hash state implementations. Developers adding type support
// for `absl::Hash` should not rely on any parts of the state object other than
// the following member functions:
//
//   * HashStateBase::combine()
//   * HashStateBase::combine_contiguous()
//
// A derived hash state class of type `H` must provide a static member function
// with a signature similar to the following:
//
//    `static H combine_contiguous(H state, const unsigned char*, size_t)`.
//
// `HashStateBase` will provide a complete implementations for a hash state
// object in terms of this method.
//
// Example:
//
//   // Use CRTP to define your derived class.
//   struct MyHashState : HashStateBase<MyHashState> {
//       static H combine_contiguous(H state, const unsigned char*, size_t);
//       using MyHashState::HashStateBase::combine;
//       using MyHashState::HashStateBase::combine_contiguous;
//   };
template <typename H>
class HashStateBase {
 public:
  // HashStateBase::combine()
  //
  // Combines an arbitrary number of values into a hash state, returning the
  // updated state.
  //
  // Each of the value types `T` must be separately hashable by the Abseil
  // hashing framework.
  //
  // NOTE:
  //
  //   state = H::combine(std::move(state), value1, value2, value3);
  //
  // is guaranteed to produce the same hash expansion as:
  //
  //   state = H::combine(std::move(state), value1);
  //   state = H::combine(std::move(state), value2);
  //   state = H::combine(std::move(state), value3);
  template <typename T, typename... Ts>
  static H combine(H state, const T& value, const Ts&... values);
  static H combine(H state) { return state; }

  // HashStateBase::combine_contiguous()
  //
  // Combines a contiguous array of `size` elements into a hash state, returning
  // the updated state.
  //
  // NOTE:
  //
  //   state = H::combine_contiguous(std::move(state), data, size);
  //
  // is NOT guaranteed to produce the same hash expansion as a for-loop (it may
  // perform internal optimizations).  If you need this guarantee, use the
  // for-loop instead.
  template <typename T>
  static H combine_contiguous(H state, const T* data, size_t size);
};

// is_uniquely_represented
//
// `is_uniquely_represented<T>` is a trait class that indicates whether `T`
// is uniquely represented.
//
// A type is "uniquely represented" if two equal values of that type are
// guaranteed to have the same bytes in their underlying storage. In other
// words, if `a == b`, then `memcmp(&a, &b, sizeof(T))` is guaranteed to be
// zero. This property cannot be detected automatically, so this trait is false
// by default, but can be specialized by types that wish to assert that they are
// uniquely represented. This makes them eligible for certain optimizations.
//
// If you have any doubt whatsoever, do not specialize this template.
// The default is completely safe, and merely disables some optimizations
// that will not matter for most types. Specializing this template,
// on the other hand, can be very hazardous.
//
// To be uniquely represented, a type must not have multiple ways of
// representing the same value; for example, float and double are not
// uniquely represented, because they have distinct representations for
// +0 and -0. Furthermore, the type's byte representation must consist
// solely of user-controlled data, with no padding bits and no compiler-
// controlled data such as vptrs or sanitizer metadata. This is usually
// very difficult to guarantee, because in most cases the compiler can
// insert data and padding bits at its own discretion.
//
// If you specialize this template for a type `T`, you must do so in the file
// that defines that type (or in this file). If you define that specialization
// anywhere else, `is_uniquely_represented<T>` could have different meanings
// in different places.
//
// The Enable parameter is meaningless; it is provided as a convenience,
// to support certain SFINAE techniques when defining specializations.
template <typename T, typename Enable = void>
struct is_uniquely_represented : std::false_type {};

// is_uniquely_represented<unsigned char>
//
// unsigned char is a synonym for "byte", so it is guaranteed to be
// uniquely represented.
template <>
struct is_uniquely_represented<unsigned char> : std::true_type {};

// is_uniquely_represented for non-standard integral types
//
// Integral types other than bool should be uniquely represented on any
// platform that this will plausibly be ported to.
template <typename Integral>
struct is_uniquely_represented<
    Integral, typename std::enable_if<std::is_integral<Integral>::value>::type>
    : std::true_type {};

// is_uniquely_represented<bool>
//
//
template <>
struct is_uniquely_represented<bool> : std::false_type {};

// hash_bytes()
//
// Convenience function that combines `hash_state` with the byte representation
// of `value`.
template <typename H, typename T>
H hash_bytes(H hash_state, const T& value) {
  const unsigned char* start = reinterpret_cast<const unsigned char*>(&value);
  return H::combine_contiguous(std::move(hash_state), start, sizeof(value));
}

// -----------------------------------------------------------------------------
// AbslHashValue for Basic Types
// -----------------------------------------------------------------------------

// Note: Default `AbslHashValue` implementations live in `hash_internal`. This
// allows us to block lexical scope lookup when doing an unqualified call to
// `AbslHashValue` below. User-defined implementations of `AbslHashValue` can
// only be found via ADL.

// AbslHashValue() for hashing bool values
//
// We use SFINAE to ensure that this overload only accepts bool, not types that
// are convertible to bool.
template <typename H, typename B>
typename std::enable_if<std::is_same<B, bool>::value, H>::type AbslHashValue(
    H hash_state, B value) {
  return H::combine(std::move(hash_state),
                    static_cast<unsigned char>(value ? 1 : 0));
}

// AbslHashValue() for hashing enum values
template <typename H, typename Enum>
typename std::enable_if<std::is_enum<Enum>::value, H>::type AbslHashValue(
    H hash_state, Enum e) {
  // In practice, we could almost certainly just invoke hash_bytes directly,
  // but it's possible that a sanitizer might one day want to
  // store data in the unused bits of an enum. To avoid that risk, we
  // convert to the underlying type before hashing. Hopefully this will get
  // optimized away; if not, we can reopen discussion with c-toolchain-team.
  return H::combine(std::move(hash_state),
                    static_cast<typename std::underlying_type<Enum>::type>(e));
}
// AbslHashValue() for hashing floating-point values
template <typename H, typename Float>
typename std::enable_if<std::is_floating_point<Float>::value, H>::type
AbslHashValue(H hash_state, Float value) {
  return hash_internal::hash_bytes(std::move(hash_state),
                                   value == 0 ? 0 : value);
}

// Long double has the property that it might have extra unused bytes in it.
// For example, in x86 sizeof(long double)==16 but it only really uses 80-bits
// of it. This means we can't use hash_bytes on a long double and have to
// convert it to something else first.
template <typename H>
H AbslHashValue(H hash_state, long double value) {
  const int category = std::fpclassify(value);
  switch (category) {
    case FP_INFINITE:
      // Add the sign bit to differentiate between +Inf and -Inf
      hash_state = H::combine(std::move(hash_state), std::signbit(value));
      break;

    case FP_NAN:
    case FP_ZERO:
    default:
      // Category is enough for these.
      break;

    case FP_NORMAL:
    case FP_SUBNORMAL:
      // We can't convert `value` directly to double because this would have
      // undefined behavior if the value is out of range.
      // std::frexp gives us a value in the range (-1, -.5] or [.5, 1) that is
      // guaranteed to be in range for `double`. The truncation is
      // implementation defined, but that works as long as it is deterministic.
      int exp;
      auto mantissa = static_cast<double>(std::frexp(value, &exp));
      hash_state = H::combine(std::move(hash_state), mantissa, exp);
  }

  return H::combine(std::move(hash_state), category);
}

// AbslHashValue() for hashing pointers
template <typename H, typename T>
H AbslHashValue(H hash_state, T* ptr) {
  return hash_internal::hash_bytes(std::move(hash_state), ptr);
}

// AbslHashValue() for hashing nullptr_t
template <typename H>
H AbslHashValue(H hash_state, std::nullptr_t) {
  return H::combine(std::move(hash_state), static_cast<void*>(nullptr));
}

// -----------------------------------------------------------------------------
// AbslHashValue for Composite Types
// -----------------------------------------------------------------------------

// is_hashable()
//
// Trait class which returns true if T is hashable by the absl::Hash framework.
// Used for the AbslHashValue implementations for composite types below.
template <typename T>
struct is_hashable;

// AbslHashValue() for hashing pairs
template <typename H, typename T1, typename T2>
typename std::enable_if<is_hashable<T1>::value && is_hashable<T2>::value,
                        H>::type
AbslHashValue(H hash_state, const std::pair<T1, T2>& p) {
  return H::combine(std::move(hash_state), p.first, p.second);
}

// hash_tuple()
//
// Helper function for hashing a tuple. The third argument should
// be an index_sequence running from 0 to tuple_size<Tuple> - 1.
template <typename H, typename Tuple, size_t... Is>
H hash_tuple(H hash_state, const Tuple& t, absl::index_sequence<Is...>) {
  return H::combine(std::move(hash_state), std::get<Is>(t)...);
}

// AbslHashValue for hashing tuples
template <typename H, typename... Ts>
#if defined(_MSC_VER)
// This SFINAE gets MSVC confused under some conditions. Let's just disable it
// for now.
H
#else  // _MSC_VER
typename std::enable_if<absl::conjunction<is_hashable<Ts>...>::value, H>::type
#endif  // _MSC_VER
AbslHashValue(H hash_state, const std::tuple<Ts...>& t) {
  return hash_internal::hash_tuple(std::move(hash_state), t,
                                   absl::make_index_sequence<sizeof...(Ts)>());
}

// -----------------------------------------------------------------------------
// AbslHashValue for Pointers
// -----------------------------------------------------------------------------

// AbslHashValue for hashing unique_ptr
template <typename H, typename T, typename D>
H AbslHashValue(H hash_state, const std::unique_ptr<T, D>& ptr) {
  return H::combine(std::move(hash_state), ptr.get());
}

// AbslHashValue for hashing shared_ptr
template <typename H, typename T>
H AbslHashValue(H hash_state, const std::shared_ptr<T>& ptr) {
  return H::combine(std::move(hash_state), ptr.get());
}

// -----------------------------------------------------------------------------
// AbslHashValue for String-Like Types
// -----------------------------------------------------------------------------

// AbslHashValue for hashing strings
//
// All the string-like types supported here provide the same hash expansion for
// the same character sequence. These types are:
//
//  - `std::string` (and std::basic_string<char, std::char_traits<char>, A> for
//      any allocator A)
//  - `absl::string_view` and `std::string_view`
//
// For simplicity, we currently support only `char` strings. This support may
// be broadened, if necessary, but with some caution - this overload would
// misbehave in cases where the traits' `eq()` member isn't equivalent to `==`
// on the underlying character type.
template <typename H>
H AbslHashValue(H hash_state, absl::string_view str) {
  return H::combine(
      H::combine_contiguous(std::move(hash_state), str.data(), str.size()),
      str.size());
}

// -----------------------------------------------------------------------------
// AbslHashValue for Sequence Containers
// -----------------------------------------------------------------------------

// AbslHashValue for hashing std::array
template <typename H, typename T, size_t N>
typename std::enable_if<is_hashable<T>::value, H>::type AbslHashValue(
    H hash_state, const std::array<T, N>& array) {
  return H::combine_contiguous(std::move(hash_state), array.data(),
                               array.size());
}

// AbslHashValue for hashing std::deque
template <typename H, typename T, typename Allocator>
typename std::enable_if<is_hashable<T>::value, H>::type AbslHashValue(
    H hash_state, const std::deque<T, Allocator>& deque) {
  // TODO(gromer): investigate a more efficient implementation taking
  // advantage of the chunk structure.
  for (const auto& t : deque) {
    hash_state = H::combine(std::move(hash_state), t);
  }
  return H::combine(std::move(hash_state), deque.size());
}

// AbslHashValue for hashing std::forward_list
template <typename H, typename T, typename Allocator>
typename std::enable_if<is_hashable<T>::value, H>::type AbslHashValue(
    H hash_state, const std::forward_list<T, Allocator>& list) {
  size_t size = 0;
  for (const T& t : list) {
    hash_state = H::combine(std::move(hash_state), t);
    ++size;
  }
  return H::combine(std::move(hash_state), size);
}

// AbslHashValue for hashing std::list
template <typename H, typename T, typename Allocator>
typename std::enable_if<is_hashable<T>::value, H>::type AbslHashValue(
    H hash_state, const std::list<T, Allocator>& list) {
  for (const auto& t : list) {
    hash_state = H::combine(std::move(hash_state), t);
  }
  return H::combine(std::move(hash_state), list.size());
}

// AbslHashValue for hashing std::vector
//
// Do not use this for vector<bool>. It does not have a .data(), and a fallback
// for std::hash<> is most likely faster.
template <typename H, typename T, typename Allocator>
typename std::enable_if<is_hashable<T>::value && !std::is_same<T, bool>::value,
                        H>::type
AbslHashValue(H hash_state, const std::vector<T, Allocator>& vector) {
  return H::combine(H::combine_contiguous(std::move(hash_state), vector.data(),
                                          vector.size()),
                    vector.size());
}

// -----------------------------------------------------------------------------
// AbslHashValue for Ordered Associative Containers
// -----------------------------------------------------------------------------

// AbslHashValue for hashing std::map
template <typename H, typename Key, typename T, typename Compare,
          typename Allocator>
typename std::enable_if<is_hashable<Key>::value && is_hashable<T>::value,
                        H>::type
AbslHashValue(H hash_state, const std::map<Key, T, Compare, Allocator>& map) {
  for (const auto& t : map) {
    hash_state = H::combine(std::move(hash_state), t);
  }
  return H::combine(std::move(hash_state), map.size());
}

// AbslHashValue for hashing std::multimap
template <typename H, typename Key, typename T, typename Compare,
          typename Allocator>
typename std::enable_if<is_hashable<Key>::value && is_hashable<T>::value,
                        H>::type
AbslHashValue(H hash_state,
              const std::multimap<Key, T, Compare, Allocator>& map) {
  for (const auto& t : map) {
    hash_state = H::combine(std::move(hash_state), t);
  }
  return H::combine(std::move(hash_state), map.size());
}

// AbslHashValue for hashing std::set
template <typename H, typename Key, typename Compare, typename Allocator>
typename std::enable_if<is_hashable<Key>::value, H>::type AbslHashValue(
    H hash_state, const std::set<Key, Compare, Allocator>& set) {
  for (const auto& t : set) {
    hash_state = H::combine(std::move(hash_state), t);
  }
  return H::combine(std::move(hash_state), set.size());
}

// AbslHashValue for hashing std::multiset
template <typename H, typename Key, typename Compare, typename Allocator>
typename std::enable_if<is_hashable<Key>::value, H>::type AbslHashValue(
    H hash_state, const std::multiset<Key, Compare, Allocator>& set) {
  for (const auto& t : set) {
    hash_state = H::combine(std::move(hash_state), t);
  }
  return H::combine(std::move(hash_state), set.size());
}

// -----------------------------------------------------------------------------
// AbslHashValue for Wrapper Types
// -----------------------------------------------------------------------------

// AbslHashValue for hashing absl::optional
template <typename H, typename T>
typename std::enable_if<is_hashable<T>::value, H>::type AbslHashValue(
    H hash_state, const absl::optional<T>& opt) {
  if (opt) hash_state = H::combine(std::move(hash_state), *opt);
  return H::combine(std::move(hash_state), opt.has_value());
}

// VariantVisitor
template <typename H>
struct VariantVisitor {
  H&& hash_state;
  template <typename T>
  H operator()(const T& t) const {
    return H::combine(std::move(hash_state), t);
  }
};

// AbslHashValue for hashing absl::variant
template <typename H, typename... T>
typename std::enable_if<conjunction<is_hashable<T>...>::value, H>::type
AbslHashValue(H hash_state, const absl::variant<T...>& v) {
  if (!v.valueless_by_exception()) {
    hash_state = absl::visit(VariantVisitor<H>{std::move(hash_state)}, v);
  }
  return H::combine(std::move(hash_state), v.index());
}

// -----------------------------------------------------------------------------
// AbslHashValue for Other Types
// -----------------------------------------------------------------------------

// AbslHashValue for hashing std::bitset is not defined, for the same reason as
// for vector<bool> (see std::vector above): It does not expose the raw bytes,
// and a fallback to std::hash<> is most likely faster.

// -----------------------------------------------------------------------------

// hash_range_or_bytes()
//
// Mixes all values in the range [data, data+size) into the hash state.
// This overload accepts only uniquely-represented types, and hashes them by
// hashing the entire range of bytes.
template <typename H, typename T>
typename std::enable_if<is_uniquely_represented<T>::value, H>::type
hash_range_or_bytes(H hash_state, const T* data, size_t size) {
  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  return H::combine_contiguous(std::move(hash_state), bytes, sizeof(T) * size);
}

// hash_range_or_bytes()
template <typename H, typename T>
typename std::enable_if<!is_uniquely_represented<T>::value, H>::type
hash_range_or_bytes(H hash_state, const T* data, size_t size) {
  for (const auto end = data + size; data < end; ++data) {
    hash_state = H::combine(std::move(hash_state), *data);
  }
  return hash_state;
}

// InvokeHashTag
//
// InvokeHash(H, const T&) invokes the appropriate hash implementation for a
// hasher of type `H` and a value of type `T`. If `T` is not hashable, there
// will be no matching overload of InvokeHash().
// Note: Some platforms (eg MSVC) do not support the detect idiom on
// std::hash. In those platforms the last fallback will be std::hash and
// InvokeHash() will always have a valid overload even if std::hash<T> is not
// valid.
//
// We try the following options in order:
//   * If is_uniquely_represented, hash bytes directly.
//   * ADL AbslHashValue(H, const T&) call.
//   * std::hash<T>

// In MSVC we can't probe std::hash or stdext::hash because it triggers a
// static_assert instead of failing substitution.
#if defined(_MSC_VER)
#define ABSL_HASH_INTERNAL_CAN_POISON_ 0
#else   // _MSC_VER
#define ABSL_HASH_INTERNAL_CAN_POISON_ 1
#endif  // _MSC_VER

#if defined(ABSL_INTERNAL_LEGACY_HASH_NAMESPACE) && \
    ABSL_HASH_INTERNAL_CAN_POISON_
#define ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_ 1
#else
#define ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_ 0
#endif

enum class InvokeHashTag {
  kUniquelyRepresented,
  kHashValue,
#if ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_
  kLegacyHash,
#endif  // ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_
  kStdHash,
  kNone
};

// HashSelect
//
// Type trait to select the appropriate hash implementation to use.
// HashSelect<T>::value is an instance of InvokeHashTag that indicates the best
// available hashing mechanism.
// See `Note` above about MSVC.
template <typename T>
struct HashSelect {
 private:
  struct State : HashStateBase<State> {
    static State combine_contiguous(State hash_state, const unsigned char*,
                                    size_t);
    using State::HashStateBase::combine_contiguous;
  };

  // `Probe<V, Tag>::value` evaluates to `V<T>::value` if it is a valid
  // expression, and `false` otherwise.
  // `Probe<V, Tag>::tag` always evaluates to `Tag`.
  template <template <typename> class V, InvokeHashTag Tag>
  struct Probe {
   private:
    template <typename U, typename std::enable_if<V<U>::value, int>::type = 0>
    static std::true_type Test(int);
    template <typename U>
    static std::false_type Test(char);

   public:
    static constexpr InvokeHashTag kTag = Tag;
    static constexpr bool value = decltype(
        Test<absl::remove_const_t<absl::remove_reference_t<T>>>(0))::value;
  };

  template <typename U>
  using ProbeUniquelyRepresented = is_uniquely_represented<U>;

  template <typename U>
  using ProbeHashValue =
      std::is_same<State, decltype(AbslHashValue(std::declval<State>(),
                                                 std::declval<const U&>()))>;

#if ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_
  template <typename U>
  using ProbeLegacyHash =
      std::is_convertible<decltype(ABSL_INTERNAL_LEGACY_HASH_NAMESPACE::hash<
                                   U>()(std::declval<const U&>())),
                          size_t>;
#endif  // ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_

  template <typename U>
  using ProbeStdHash =
#if ABSL_HASH_INTERNAL_CAN_POISON_
      std::is_convertible<decltype(std::hash<U>()(std::declval<const U&>())),
                          size_t>;
#else   // ABSL_HASH_INTERNAL_CAN_POISON_
      std::true_type;
#endif  // ABSL_HASH_INTERNAL_CAN_POISON_

  template <typename U>
  using ProbeNone = std::true_type;

 public:
  // Probe each implementation in order.
  // disjunction provides short circuting wrt instantiation.
  static constexpr InvokeHashTag value = absl::disjunction<
      Probe<ProbeUniquelyRepresented, InvokeHashTag::kUniquelyRepresented>,
      Probe<ProbeHashValue, InvokeHashTag::kHashValue>,
#if ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_
      Probe<ProbeLegacyHash, InvokeHashTag::kLegacyHash>,
#endif  // ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_
      Probe<ProbeStdHash, InvokeHashTag::kStdHash>,
      Probe<ProbeNone, InvokeHashTag::kNone>>::kTag;
};

template <typename T>
struct is_hashable : std::integral_constant<bool, HashSelect<T>::value !=
                                                      InvokeHashTag::kNone> {};

template <typename H, typename T>
absl::enable_if_t<HashSelect<T>::value == InvokeHashTag::kUniquelyRepresented,
                  H>
InvokeHash(H state, const T& value) {
  return hash_internal::hash_bytes(std::move(state), value);
}

template <typename H, typename T>
absl::enable_if_t<HashSelect<T>::value == InvokeHashTag::kHashValue, H>
InvokeHash(H state, const T& value) {
  return AbslHashValue(std::move(state), value);
}

#if ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_
template <typename H, typename T>
absl::enable_if_t<HashSelect<T>::value == InvokeHashTag::kLegacyHash, H>
InvokeHash(H state, const T& value) {
  return hash_internal::hash_bytes(
      std::move(state), ABSL_INTERNAL_LEGACY_HASH_NAMESPACE::hash<T>{}(value));
}
#endif  // ABSL_HASH_INTERNAL_SUPPORT_LEGACY_HASH_

template <typename H, typename T>
absl::enable_if_t<HashSelect<T>::value == InvokeHashTag::kStdHash, H>
InvokeHash(H state, const T& value) {
  return hash_internal::hash_bytes(std::move(state), std::hash<T>{}(value));
}

// CityHashState
class CityHashState : public HashStateBase<CityHashState> {
  // absl::uint128 is not an alias or a thin wrapper around the intrinsic.
  // We use the intrinsic when available to improve performance.
#ifdef ABSL_HAVE_INTRINSIC_INT128
  using uint128 = __uint128_t;
#else   // ABSL_HAVE_INTRINSIC_INT128
  using uint128 = absl::uint128;
#endif  // ABSL_HAVE_INTRINSIC_INT128

  static constexpr uint64_t kMul =
      sizeof(size_t) == 4 ? uint64_t{0xcc9e2d51} : uint64_t{0x9ddfea08eb382d69};

  template <typename T>
  using IntegralFastPath =
      conjunction<std::is_integral<T>, is_uniquely_represented<T>>;

 public:
  // Move only
  CityHashState(CityHashState&&) = default;
  CityHashState& operator=(CityHashState&&) = default;

  // CityHashState::combine_contiguous()
  //
  // Fundamental base case for hash recursion: mixes the given range of bytes
  // into the hash state.
  static CityHashState combine_contiguous(CityHashState hash_state,
                                          const unsigned char* first,
                                          size_t size) {
    return CityHashState(
        CombineContiguousImpl(hash_state.state_, first, size,
                              std::integral_constant<int, sizeof(size_t)>{}));
  }
  using CityHashState::HashStateBase::combine_contiguous;

  // CityHashState::hash()
  //
  // For performance reasons in non-opt mode, we specialize this for
  // integral types.
  // Otherwise we would be instantiating and calling dozens of functions for
  // something that is just one multiplication and a couple xor's.
  // The result should be the same as running the whole algorithm, but faster.
  template <typename T, absl::enable_if_t<IntegralFastPath<T>::value, int> = 0>
  static size_t hash(T value) {
    return static_cast<size_t>(Mix(Seed(), static_cast<uint64_t>(value)));
  }

  // Overload of CityHashState::hash()
  template <typename T, absl::enable_if_t<!IntegralFastPath<T>::value, int> = 0>
  static size_t hash(const T& value) {
    return static_cast<size_t>(combine(CityHashState{}, value).state_);
  }

 private:
  // Invoked only once for a given argument; that plus the fact that this is
  // move-only ensures that there is only one non-moved-from object.
  CityHashState() : state_(Seed()) {}

  // Workaround for MSVC bug.
  // We make the type copyable to fix the calling convention, even though we
  // never actually copy it. Keep it private to not affect the public API of the
  // type.
  CityHashState(const CityHashState&) = default;

  explicit CityHashState(uint64_t state) : state_(state) {}

  // Implementation of the base case for combine_contiguous where we actually
  // mix the bytes into the state.
  // Dispatch to different implementations of the combine_contiguous depending
  // on the value of `sizeof(size_t)`.
  static uint64_t CombineContiguousImpl(uint64_t state,
                                        const unsigned char* first, size_t len,
                                        std::integral_constant<int, 4>
                                        /* sizeof_size_t */);
  static uint64_t CombineContiguousImpl(uint64_t state,
                                        const unsigned char* first, size_t len,
                                        std::integral_constant<int, 8>
                                        /* sizeof_size_t*/);

  // Reads 9 to 16 bytes from p.
  // The first 8 bytes are in .first, the rest (zero padded) bytes are in
  // .second.
  static std::pair<uint64_t, uint64_t> Read9To16(const unsigned char* p,
                                                 size_t len) {
    uint64_t high = little_endian::Load64(p + len - 8);
    return {little_endian::Load64(p), high >> (128 - len * 8)};
  }

  // Reads 4 to 8 bytes from p. Zero pads to fill uint64_t.
  static uint64_t Read4To8(const unsigned char* p, size_t len) {
    return (static_cast<uint64_t>(little_endian::Load32(p + len - 4))
            << (len - 4) * 8) |
           little_endian::Load32(p);
  }

  // Reads 1 to 3 bytes from p. Zero pads to fill uint32_t.
  static uint32_t Read1To3(const unsigned char* p, size_t len) {
    return static_cast<uint32_t>((p[0]) |                         //
                                 (p[len / 2] << (len / 2 * 8)) |  //
                                 (p[len - 1] << ((len - 1) * 8)));
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE static uint64_t Mix(uint64_t state, uint64_t v) {
    using MultType =
        absl::conditional_t<sizeof(size_t) == 4, uint64_t, uint128>;
    // We do the addition in 64-bit space to make sure the 128-bit
    // multiplication is fast. If we were to do it as MultType the compiler has
    // to assume that the high word is non-zero and needs to perform 2
    // multiplications instead of one.
    MultType m = state + v;
    m *= kMul;
    return static_cast<uint64_t>(m ^ (m >> (sizeof(m) * 8 / 2)));
  }

  // Seed()
  //
  // A non-deterministic seed.
  //
  // The current purpose of this seed is to generate non-deterministic results
  // and prevent having users depend on the particular hash values.
  // It is not meant as a security feature right now, but it leaves the door
  // open to upgrade it to a true per-process random seed. A true random seed
  // costs more and we don't need to pay for that right now.
  //
  // On platforms with ASLR, we take advantage of it to make a per-process
  // random value.
  // See https://en.wikipedia.org/wiki/Address_space_layout_randomization
  //
  // On other platforms this is still going to be non-deterministic but most
  // probably per-build and not per-process.
  ABSL_ATTRIBUTE_ALWAYS_INLINE static uint64_t Seed() {
    return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(kSeed));
  }
  static const void* const kSeed;

  uint64_t state_;
};

// CityHashState::CombineContiguousImpl()
inline uint64_t CityHashState::CombineContiguousImpl(
    uint64_t state, const unsigned char* first, size_t len,
    std::integral_constant<int, 4> /* sizeof_size_t */) {
  // For large values we use CityHash, for small ones we just use a
  // multiplicative hash.
  uint64_t v;
  if (len > 8) {
    v = absl::hash_internal::CityHash32(reinterpret_cast<const char*>(first), len);
  } else if (len >= 4) {
    v = Read4To8(first, len);
  } else if (len > 0) {
    v = Read1To3(first, len);
  } else {
    // Empty ranges have no effect.
    return state;
  }
  return Mix(state, v);
}

// Overload of CityHashState::CombineContiguousImpl()
inline uint64_t CityHashState::CombineContiguousImpl(
    uint64_t state, const unsigned char* first, size_t len,
    std::integral_constant<int, 8> /* sizeof_size_t */) {
  // For large values we use CityHash, for small ones we just use a
  // multiplicative hash.
  uint64_t v;
  if (len > 16) {
    v = absl::hash_internal::CityHash64(reinterpret_cast<const char*>(first), len);
  } else if (len > 8) {
    auto p = Read9To16(first, len);
    state = Mix(state, p.first);
    v = p.second;
  } else if (len >= 4) {
    v = Read4To8(first, len);
  } else if (len > 0) {
    v = Read1To3(first, len);
  } else {
    // Empty ranges have no effect.
    return state;
  }
  return Mix(state, v);
}


struct AggregateBarrier {};

// HashImpl

// Add a private base class to make sure this type is not an aggregate.
// Aggregates can be aggregate initialized even if the default constructor is
// deleted.
struct PoisonedHash : private AggregateBarrier {
  PoisonedHash() = delete;
  PoisonedHash(const PoisonedHash&) = delete;
  PoisonedHash& operator=(const PoisonedHash&) = delete;
};

template <typename T>
struct HashImpl {
  size_t operator()(const T& value) const { return CityHashState::hash(value); }
};

template <typename T>
struct Hash
    : absl::conditional_t<is_hashable<T>::value, HashImpl<T>, PoisonedHash> {};

template <typename H>
template <typename T, typename... Ts>
H HashStateBase<H>::combine(H state, const T& value, const Ts&... values) {
  return H::combine(hash_internal::InvokeHash(std::move(state), value),
                    values...);
}

// HashStateBase::combine_contiguous()
template <typename H>
template <typename T>
H HashStateBase<H>::combine_contiguous(H state, const T* data, size_t size) {
  return hash_internal::hash_range_or_bytes(std::move(state), data, size);
}
}  // namespace hash_internal
}  // namespace absl

#endif  // ABSL_HASH_INTERNAL_HASH_H_
