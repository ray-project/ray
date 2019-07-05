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
// An open-addressing
// hashtable with quadratic probing.
//
// This is a low level hashtable on top of which different interfaces can be
// implemented, like flat_hash_set, node_hash_set, string_hash_set, etc.
//
// The table interface is similar to that of std::unordered_set. Notable
// differences are that most member functions support heterogeneous keys when
// BOTH the hash and eq functions are marked as transparent. They do so by
// providing a typedef called `is_transparent`.
//
// When heterogeneous lookup is enabled, functions that take key_type act as if
// they have an overload set like:
//
//   iterator find(const key_type& key);
//   template <class K>
//   iterator find(const K& key);
//
//   size_type erase(const key_type& key);
//   template <class K>
//   size_type erase(const K& key);
//
//   std::pair<iterator, iterator> equal_range(const key_type& key);
//   template <class K>
//   std::pair<iterator, iterator> equal_range(const K& key);
//
// When heterogeneous lookup is disabled, only the explicit `key_type` overloads
// exist.
//
// find() also supports passing the hash explicitly:
//
//   iterator find(const key_type& key, size_t hash);
//   template <class U>
//   iterator find(const U& key, size_t hash);
//
// In addition the pointer to element and iterator stability guarantees are
// weaker: all iterators and pointers are invalidated after a new element is
// inserted.
//
// IMPLEMENTATION DETAILS
//
// The table stores elements inline in a slot array. In addition to the slot
// array the table maintains some control state per slot. The extra state is one
// byte per slot and stores empty or deleted marks, or alternatively 7 bits from
// the hash of an occupied slot. The table is split into logical groups of
// slots, like so:
//
//      Group 1         Group 2        Group 3
// +---------------+---------------+---------------+
// | | | | | | | | | | | | | | | | | | | | | | | | |
// +---------------+---------------+---------------+
//
// On lookup the hash is split into two parts:
// - H2: 7 bits (those stored in the control bytes)
// - H1: the rest of the bits
// The groups are probed using H1. For each group the slots are matched to H2 in
// parallel. Because H2 is 7 bits (128 states) and the number of slots per group
// is low (8 or 16) in almost all cases a match in H2 is also a lookup hit.
//
// On insert, once the right group is found (as in lookup), its slots are
// filled in order.
//
// On erase a slot is cleared. In case the group did not have any empty slots
// before the erase, the erased slot is marked as deleted.
//
// Groups without empty slots (but maybe with deleted slots) extend the probe
// sequence. The probing algorithm is quadratic. Given N the number of groups,
// the probing function for the i'th probe is:
//
//   P(0) = H1 % N
//
//   P(i) = (P(i - 1) + i) % N
//
// This probing function guarantees that after N probes, all the groups of the
// table will be probed exactly once.

#ifndef ABSL_CONTAINER_INTERNAL_RAW_HASH_SET_H_
#define ABSL_CONTAINER_INTERNAL_RAW_HASH_SET_H_

#ifndef SWISSTABLE_HAVE_SSE2
#if defined(__SSE2__) ||  \
    (defined(_MSC_VER) && \
     (defined(_M_X64) || (defined(_M_IX86) && _M_IX86_FP >= 2)))
#define SWISSTABLE_HAVE_SSE2 1
#else
#define SWISSTABLE_HAVE_SSE2 0
#endif
#endif

#ifndef SWISSTABLE_HAVE_SSSE3
#ifdef __SSSE3__
#define SWISSTABLE_HAVE_SSSE3 1
#else
#define SWISSTABLE_HAVE_SSSE3 0
#endif
#endif

#if SWISSTABLE_HAVE_SSSE3 && !SWISSTABLE_HAVE_SSE2
#error "Bad configuration!"
#endif

#if SWISSTABLE_HAVE_SSE2
#include <emmintrin.h>
#endif

#if SWISSTABLE_HAVE_SSSE3
#include <tmmintrin.h>
#endif

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/internal/bits.h"
#include "absl/base/internal/endian.h"
#include "absl/base/port.h"
#include "absl/container/internal/compressed_tuple.h"
#include "absl/container/internal/container_memory.h"
#include "absl/container/internal/hash_policy_traits.h"
#include "absl/container/internal/hashtable_debug_hooks.h"
#include "absl/container/internal/layout.h"
#include "absl/memory/memory.h"
#include "absl/meta/type_traits.h"
#include "absl/types/optional.h"
#include "absl/utility/utility.h"

namespace absl {
namespace container_internal {

template <size_t Width>
class probe_seq {
 public:
  probe_seq(size_t hash, size_t mask) {
    assert(((mask + 1) & mask) == 0 && "not a mask");
    mask_ = mask;
    offset_ = hash & mask_;
  }
  size_t offset() const { return offset_; }
  size_t offset(size_t i) const { return (offset_ + i) & mask_; }

  void next() {
    index_ += Width;
    offset_ += index_;
    offset_ &= mask_;
  }
  // 0-based probe index. The i-th probe in the probe sequence.
  size_t index() const { return index_; }

 private:
  size_t mask_;
  size_t offset_;
  size_t index_ = 0;
};

template <class ContainerKey, class Hash, class Eq>
struct RequireUsableKey {
  template <class PassedKey, class... Args>
  std::pair<
      decltype(std::declval<const Hash&>()(std::declval<const PassedKey&>())),
      decltype(std::declval<const Eq&>()(std::declval<const ContainerKey&>(),
                                         std::declval<const PassedKey&>()))>*
  operator()(const PassedKey&, const Args&...) const;
};

template <class E, class Policy, class Hash, class Eq, class... Ts>
struct IsDecomposable : std::false_type {};

template <class Policy, class Hash, class Eq, class... Ts>
struct IsDecomposable<
    absl::void_t<decltype(
        Policy::apply(RequireUsableKey<typename Policy::key_type, Hash, Eq>(),
                      std::declval<Ts>()...))>,
    Policy, Hash, Eq, Ts...> : std::true_type {};

template <class, class = void>
struct IsTransparent : std::false_type {};
template <class T>
struct IsTransparent<T, absl::void_t<typename T::is_transparent>>
    : std::true_type {};

// TODO(alkis): Switch to std::is_nothrow_swappable when gcc/clang supports it.
template <class T>
constexpr bool IsNoThrowSwappable() {
  using std::swap;
  return noexcept(swap(std::declval<T&>(), std::declval<T&>()));
}

template <typename T>
int TrailingZeros(T x) {
  return sizeof(T) == 8 ? base_internal::CountTrailingZerosNonZero64(
                              static_cast<uint64_t>(x))
                        : base_internal::CountTrailingZerosNonZero32(
                              static_cast<uint32_t>(x));
}

template <typename T>
int LeadingZeros(T x) {
  return sizeof(T) == 8
             ? base_internal::CountLeadingZeros64(static_cast<uint64_t>(x))
             : base_internal::CountLeadingZeros32(static_cast<uint32_t>(x));
}

// An abstraction over a bitmask. It provides an easy way to iterate through the
// indexes of the set bits of a bitmask.  When Shift=0 (platforms with SSE),
// this is a true bitmask.  On non-SSE, platforms the arithematic used to
// emulate the SSE behavior works in bytes (Shift=3) and leaves each bytes as
// either 0x00 or 0x80.
//
// For example:
//   for (int i : BitMask<uint32_t, 16>(0x5)) -> yields 0, 2
//   for (int i : BitMask<uint64_t, 8, 3>(0x0000000080800000)) -> yields 2, 3
template <class T, int SignificantBits, int Shift = 0>
class BitMask {
  static_assert(std::is_unsigned<T>::value, "");
  static_assert(Shift == 0 || Shift == 3, "");

 public:
  // These are useful for unit tests (gunit).
  using value_type = int;
  using iterator = BitMask;
  using const_iterator = BitMask;

  explicit BitMask(T mask) : mask_(mask) {}
  BitMask& operator++() {
    mask_ &= (mask_ - 1);
    return *this;
  }
  explicit operator bool() const { return mask_ != 0; }
  int operator*() const { return LowestBitSet(); }
  int LowestBitSet() const {
    return container_internal::TrailingZeros(mask_) >> Shift;
  }
  int HighestBitSet() const {
    return (sizeof(T) * CHAR_BIT - container_internal::LeadingZeros(mask_) -
            1) >>
           Shift;
  }

  BitMask begin() const { return *this; }
  BitMask end() const { return BitMask(0); }

  int TrailingZeros() const {
    return container_internal::TrailingZeros(mask_) >> Shift;
  }

  int LeadingZeros() const {
    constexpr int total_significant_bits = SignificantBits << Shift;
    constexpr int extra_bits = sizeof(T) * 8 - total_significant_bits;
    return container_internal::LeadingZeros(mask_ << extra_bits) >> Shift;
  }

 private:
  friend bool operator==(const BitMask& a, const BitMask& b) {
    return a.mask_ == b.mask_;
  }
  friend bool operator!=(const BitMask& a, const BitMask& b) {
    return a.mask_ != b.mask_;
  }

  T mask_;
};

using ctrl_t = signed char;
using h2_t = uint8_t;

// The values here are selected for maximum performance. See the static asserts
// below for details.
enum Ctrl : ctrl_t {
  kEmpty = -128,   // 0b10000000
  kDeleted = -2,   // 0b11111110
  kSentinel = -1,  // 0b11111111
};
static_assert(
    kEmpty & kDeleted & kSentinel & 0x80,
    "Special markers need to have the MSB to make checking for them efficient");
static_assert(kEmpty < kSentinel && kDeleted < kSentinel,
              "kEmpty and kDeleted must be smaller than kSentinel to make the "
              "SIMD test of IsEmptyOrDeleted() efficient");
static_assert(kSentinel == -1,
              "kSentinel must be -1 to elide loading it from memory into SIMD "
              "registers (pcmpeqd xmm, xmm)");
static_assert(kEmpty == -128,
              "kEmpty must be -128 to make the SIMD check for its "
              "existence efficient (psignb xmm, xmm)");
static_assert(~kEmpty & ~kDeleted & kSentinel & 0x7F,
              "kEmpty and kDeleted must share an unset bit that is not shared "
              "by kSentinel to make the scalar test for MatchEmptyOrDeleted() "
              "efficient");
static_assert(kDeleted == -2,
              "kDeleted must be -2 to make the implementation of "
              "ConvertSpecialToEmptyAndFullToDeleted efficient");

// A single block of empty control bytes for tables without any slots allocated.
// This enables removing a branch in the hot path of find().
inline ctrl_t* EmptyGroup() {
  alignas(16) static constexpr ctrl_t empty_group[] = {
      kSentinel, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty,
      kEmpty,    kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty};
  return const_cast<ctrl_t*>(empty_group);
}

// Mixes a randomly generated per-process seed with `hash` and `ctrl` to
// randomize insertion order within groups.
bool ShouldInsertBackwards(size_t hash, ctrl_t* ctrl);

// Returns a hash seed.
//
// The seed consists of the ctrl_ pointer, which adds enough entropy to ensure
// non-determinism of iteration order in most cases.
inline size_t HashSeed(const ctrl_t* ctrl) {
  // The low bits of the pointer have little or no entropy because of
  // alignment. We shift the pointer to try to use higher entropy bits. A
  // good number seems to be 12 bits, because that aligns with page size.
  return reinterpret_cast<uintptr_t>(ctrl) >> 12;
}

inline size_t H1(size_t hash, const ctrl_t* ctrl) {
  return (hash >> 7) ^ HashSeed(ctrl);
}
inline ctrl_t H2(size_t hash) { return hash & 0x7F; }

inline bool IsEmpty(ctrl_t c) { return c == kEmpty; }
inline bool IsFull(ctrl_t c) { return c >= 0; }
inline bool IsDeleted(ctrl_t c) { return c == kDeleted; }
inline bool IsEmptyOrDeleted(ctrl_t c) { return c < kSentinel; }

#if SWISSTABLE_HAVE_SSE2

// https://github.com/abseil/abseil-cpp/issues/209
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=87853
// _mm_cmpgt_epi8 is broken under GCC with -funsigned-char
// Work around this by using the portable implementation of Group
// when using -funsigned-char under GCC.
inline __m128i _mm_cmpgt_epi8_fixed(__m128i a, __m128i b) {
#if defined(__GNUC__) && !defined(__clang__)
  if (std::is_unsigned<char>::value) {
    const __m128i mask = _mm_set1_epi8(0x80);
    const __m128i diff = _mm_subs_epi8(b, a);
    return _mm_cmpeq_epi8(_mm_and_si128(diff, mask), mask);
  }
#endif
  return _mm_cmpgt_epi8(a, b);
}

struct GroupSse2Impl {
  static constexpr size_t kWidth = 16;  // the number of slots per group

  explicit GroupSse2Impl(const ctrl_t* pos) {
    ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
  }

  // Returns a bitmask representing the positions of slots that match hash.
  BitMask<uint32_t, kWidth> Match(h2_t hash) const {
    auto match = _mm_set1_epi8(hash);
    return BitMask<uint32_t, kWidth>(
        _mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
  }

  // Returns a bitmask representing the positions of empty slots.
  BitMask<uint32_t, kWidth> MatchEmpty() const {
#if SWISSTABLE_HAVE_SSSE3
    // This only works because kEmpty is -128.
    return BitMask<uint32_t, kWidth>(
        _mm_movemask_epi8(_mm_sign_epi8(ctrl, ctrl)));
#else
    return Match(kEmpty);
#endif
  }

  // Returns a bitmask representing the positions of empty or deleted slots.
  BitMask<uint32_t, kWidth> MatchEmptyOrDeleted() const {
    auto special = _mm_set1_epi8(kSentinel);
    return BitMask<uint32_t, kWidth>(
        _mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)));
  }

  // Returns the number of trailing empty or deleted elements in the group.
  uint32_t CountLeadingEmptyOrDeleted() const {
    auto special = _mm_set1_epi8(kSentinel);
    return TrailingZeros(
        _mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)) + 1);
  }

  void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t* dst) const {
    auto msbs = _mm_set1_epi8(static_cast<char>(-128));
    auto x126 = _mm_set1_epi8(126);
#if SWISSTABLE_HAVE_SSSE3
    auto res = _mm_or_si128(_mm_shuffle_epi8(x126, ctrl), msbs);
#else
    auto zero = _mm_setzero_si128();
    auto special_mask = _mm_cmpgt_epi8_fixed(zero, ctrl);
    auto res = _mm_or_si128(msbs, _mm_andnot_si128(special_mask, x126));
#endif
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), res);
  }

  __m128i ctrl;
};
#endif  // SWISSTABLE_HAVE_SSE2

struct GroupPortableImpl {
  static constexpr size_t kWidth = 8;

  explicit GroupPortableImpl(const ctrl_t* pos)
      : ctrl(little_endian::Load64(pos)) {}

  BitMask<uint64_t, kWidth, 3> Match(h2_t hash) const {
    // For the technique, see:
    // http://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
    // (Determine if a word has a byte equal to n).
    //
    // Caveat: there are false positives but:
    // - they only occur if there is a real match
    // - they never occur on kEmpty, kDeleted, kSentinel
    // - they will be handled gracefully by subsequent checks in code
    //
    // Example:
    //   v = 0x1716151413121110
    //   hash = 0x12
    //   retval = (v - lsbs) & ~v & msbs = 0x0000000080800000
    constexpr uint64_t msbs = 0x8080808080808080ULL;
    constexpr uint64_t lsbs = 0x0101010101010101ULL;
    auto x = ctrl ^ (lsbs * hash);
    return BitMask<uint64_t, kWidth, 3>((x - lsbs) & ~x & msbs);
  }

  BitMask<uint64_t, kWidth, 3> MatchEmpty() const {
    constexpr uint64_t msbs = 0x8080808080808080ULL;
    return BitMask<uint64_t, kWidth, 3>((ctrl & (~ctrl << 6)) & msbs);
  }

  BitMask<uint64_t, kWidth, 3> MatchEmptyOrDeleted() const {
    constexpr uint64_t msbs = 0x8080808080808080ULL;
    return BitMask<uint64_t, kWidth, 3>((ctrl & (~ctrl << 7)) & msbs);
  }

  uint32_t CountLeadingEmptyOrDeleted() const {
    constexpr uint64_t gaps = 0x00FEFEFEFEFEFEFEULL;
    return (TrailingZeros(((~ctrl & (ctrl >> 7)) | gaps) + 1) + 7) >> 3;
  }

  void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t* dst) const {
    constexpr uint64_t msbs = 0x8080808080808080ULL;
    constexpr uint64_t lsbs = 0x0101010101010101ULL;
    auto x = ctrl & msbs;
    auto res = (~x + (x >> 7)) & ~lsbs;
    little_endian::Store64(dst, res);
  }

  uint64_t ctrl;
};

#if SWISSTABLE_HAVE_SSE2
using Group = GroupSse2Impl;
#else
using Group = GroupPortableImpl;
#endif

template <class Policy, class Hash, class Eq, class Alloc>
class raw_hash_set;

inline bool IsValidCapacity(size_t n) {
  return ((n + 1) & n) == 0 && n >= Group::kWidth - 1;
}

// PRECONDITION:
//   IsValidCapacity(capacity)
//   ctrl[capacity] == kSentinel
//   ctrl[i] != kSentinel for all i < capacity
// Applies mapping for every byte in ctrl:
//   DELETED -> EMPTY
//   EMPTY -> EMPTY
//   FULL -> DELETED
inline void ConvertDeletedToEmptyAndFullToDeleted(
    ctrl_t* ctrl, size_t capacity) {
  assert(ctrl[capacity] == kSentinel);
  assert(IsValidCapacity(capacity));
  for (ctrl_t* pos = ctrl; pos != ctrl + capacity + 1; pos += Group::kWidth) {
    Group{pos}.ConvertSpecialToEmptyAndFullToDeleted(pos);
  }
  // Copy the cloned ctrl bytes.
  std::memcpy(ctrl + capacity + 1, ctrl, Group::kWidth);
  ctrl[capacity] = kSentinel;
}

// Rounds up the capacity to the next power of 2 minus 1 and ensures it is
// greater or equal to Group::kWidth - 1.
inline size_t NormalizeCapacity(size_t n) {
  constexpr size_t kMinCapacity = Group::kWidth - 1;
  return n <= kMinCapacity
             ? kMinCapacity
             : (std::numeric_limits<size_t>::max)() >> LeadingZeros(n);
}

// The node_handle concept from C++17.
// We specialize node_handle for sets and maps. node_handle_base holds the
// common API of both.
template <typename Policy, typename Alloc>
class node_handle_base {
 protected:
  using PolicyTraits = hash_policy_traits<Policy>;
  using slot_type = typename PolicyTraits::slot_type;

 public:
  using allocator_type = Alloc;

  constexpr node_handle_base() {}
  node_handle_base(node_handle_base&& other) noexcept {
    *this = std::move(other);
  }
  ~node_handle_base() { destroy(); }
  node_handle_base& operator=(node_handle_base&& other) {
    destroy();
    if (!other.empty()) {
      alloc_ = other.alloc_;
      PolicyTraits::transfer(alloc(), slot(), other.slot());
      other.reset();
    }
    return *this;
  }

  bool empty() const noexcept { return !alloc_; }
  explicit operator bool() const noexcept { return !empty(); }
  allocator_type get_allocator() const { return *alloc_; }

 protected:
  template <typename, typename, typename, typename>
  friend class raw_hash_set;

  node_handle_base(const allocator_type& a, slot_type* s) : alloc_(a) {
    PolicyTraits::transfer(alloc(), slot(), s);
  }

  void destroy() {
    if (!empty()) {
      PolicyTraits::destroy(alloc(), slot());
      reset();
    }
  }

  void reset() {
    assert(alloc_.has_value());
    alloc_ = absl::nullopt;
  }

  slot_type* slot() const {
    assert(!empty());
    return reinterpret_cast<slot_type*>(std::addressof(slot_space_));
  }
  allocator_type* alloc() { return std::addressof(*alloc_); }

 private:
  absl::optional<allocator_type> alloc_;
  mutable absl::aligned_storage_t<sizeof(slot_type), alignof(slot_type)>
      slot_space_;
};

// For sets.
template <typename Policy, typename Alloc, typename = void>
class node_handle : public node_handle_base<Policy, Alloc> {
  using Base = typename node_handle::node_handle_base;

 public:
  using value_type = typename Base::PolicyTraits::value_type;

  constexpr node_handle() {}

  value_type& value() const {
    return Base::PolicyTraits::element(this->slot());
  }

 private:
  template <typename, typename, typename, typename>
  friend class raw_hash_set;

  node_handle(const Alloc& a, typename Base::slot_type* s) : Base(a, s) {}
};

// For maps.
template <typename Policy, typename Alloc>
class node_handle<Policy, Alloc, absl::void_t<typename Policy::mapped_type>>
    : public node_handle_base<Policy, Alloc> {
  using Base = typename node_handle::node_handle_base;

 public:
  using key_type = typename Policy::key_type;
  using mapped_type = typename Policy::mapped_type;

  constexpr node_handle() {}

  auto key() const -> decltype(Base::PolicyTraits::key(this->slot())) {
    return Base::PolicyTraits::key(this->slot());
  }

  mapped_type& mapped() const {
    return Base::PolicyTraits::value(
        &Base::PolicyTraits::element(this->slot()));
  }

 private:
  template <typename, typename, typename, typename>
  friend class raw_hash_set;

  node_handle(const Alloc& a, typename Base::slot_type* s) : Base(a, s) {}
};

// Implement the insert_return_type<> concept of C++17.
template <class Iterator, class NodeType>
struct insert_return_type {
  Iterator position;
  bool inserted;
  NodeType node;
};

// Helper trait to allow or disallow arbitrary keys when the hash and
// eq functions are transparent.
// It is very important that the inner template is an alias and that the type it
// produces is not a dependent type. Otherwise, type deduction would fail.
template <bool is_transparent>
struct KeyArg {
  // Transparent. Forward `K`.
  template <typename K, typename key_type>
  using type = K;
};

template <>
struct KeyArg<false> {
  // Not transparent. Always use `key_type`.
  template <typename K, typename key_type>
  using type = key_type;
};

// Policy: a policy defines how to perform different operations on
// the slots of the hashtable (see hash_policy_traits.h for the full interface
// of policy).
//
// Hash: a (possibly polymorphic) functor that hashes keys of the hashtable. The
// functor should accept a key and return size_t as hash. For best performance
// it is important that the hash function provides high entropy across all bits
// of the hash.
//
// Eq: a (possibly polymorphic) functor that compares two keys for equality. It
// should accept two (of possibly different type) keys and return a bool: true
// if they are equal, false if they are not. If two keys compare equal, then
// their hash values as defined by Hash MUST be equal.
//
// Allocator: an Allocator [http://devdocs.io/cpp/concept/allocator] with which
// the storage of the hashtable will be allocated and the elements will be
// constructed and destroyed.
template <class Policy, class Hash, class Eq, class Alloc>
class raw_hash_set {
  using PolicyTraits = hash_policy_traits<Policy>;
  using KeyArgImpl = container_internal::KeyArg<IsTransparent<Eq>::value &&
                                                IsTransparent<Hash>::value>;

 public:
  using init_type = typename PolicyTraits::init_type;
  using key_type = typename PolicyTraits::key_type;
  // TODO(sbenza): Hide slot_type as it is an implementation detail. Needs user
  // code fixes!
  using slot_type = typename PolicyTraits::slot_type;
  using allocator_type = Alloc;
  using size_type = size_t;
  using difference_type = ptrdiff_t;
  using hasher = Hash;
  using key_equal = Eq;
  using policy_type = Policy;
  using value_type = typename PolicyTraits::value_type;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = typename absl::allocator_traits<
      allocator_type>::template rebind_traits<value_type>::pointer;
  using const_pointer = typename absl::allocator_traits<
      allocator_type>::template rebind_traits<value_type>::const_pointer;

  // Alias used for heterogeneous lookup functions.
  // `key_arg<K>` evaluates to `K` when the functors are transparent and to
  // `key_type` otherwise. It permits template argument deduction on `K` for the
  // transparent case.
  template <class K>
  using key_arg = typename KeyArgImpl::template type<K, key_type>;

 private:
  // Give an early error when key_type is not hashable/eq.
  auto KeyTypeCanBeHashed(const Hash& h, const key_type& k) -> decltype(h(k));
  auto KeyTypeCanBeEq(const Eq& eq, const key_type& k) -> decltype(eq(k, k));

  using Layout = absl::container_internal::Layout<ctrl_t, slot_type>;

  static Layout MakeLayout(size_t capacity) {
    assert(IsValidCapacity(capacity));
    return Layout(capacity + Group::kWidth + 1, capacity);
  }

  using AllocTraits = absl::allocator_traits<allocator_type>;
  using SlotAlloc = typename absl::allocator_traits<
      allocator_type>::template rebind_alloc<slot_type>;
  using SlotAllocTraits = typename absl::allocator_traits<
      allocator_type>::template rebind_traits<slot_type>;

  static_assert(std::is_lvalue_reference<reference>::value,
                "Policy::element() must return a reference");

  template <typename T>
  struct SameAsElementReference
      : std::is_same<typename std::remove_cv<
                         typename std::remove_reference<reference>::type>::type,
                     typename std::remove_cv<
                         typename std::remove_reference<T>::type>::type> {};

  // An enabler for insert(T&&): T must be convertible to init_type or be the
  // same as [cv] value_type [ref].
  // Note: we separate SameAsElementReference into its own type to avoid using
  // reference unless we need to. MSVC doesn't seem to like it in some
  // cases.
  template <class T>
  using RequiresInsertable = typename std::enable_if<
      absl::disjunction<std::is_convertible<T, init_type>,
                        SameAsElementReference<T>>::value,
      int>::type;

  // RequiresNotInit is a workaround for gcc prior to 7.1.
  // See https://godbolt.org/g/Y4xsUh.
  template <class T>
  using RequiresNotInit =
      typename std::enable_if<!std::is_same<T, init_type>::value, int>::type;

  template <class... Ts>
  using IsDecomposable = IsDecomposable<void, PolicyTraits, Hash, Eq, Ts...>;

 public:
  static_assert(std::is_same<pointer, value_type*>::value,
                "Allocators with custom pointer types are not supported");
  static_assert(std::is_same<const_pointer, const value_type*>::value,
                "Allocators with custom pointer types are not supported");

  class iterator {
    friend class raw_hash_set;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename raw_hash_set::value_type;
    using reference =
        absl::conditional_t<PolicyTraits::constant_iterators::value,
                            const value_type&, value_type&>;
    using pointer = absl::remove_reference_t<reference>*;
    using difference_type = typename raw_hash_set::difference_type;

    iterator() {}

    // PRECONDITION: not an end() iterator.
    reference operator*() const { return PolicyTraits::element(slot_); }

    // PRECONDITION: not an end() iterator.
    pointer operator->() const { return &operator*(); }

    // PRECONDITION: not an end() iterator.
    iterator& operator++() {
      ++ctrl_;
      ++slot_;
      skip_empty_or_deleted();
      return *this;
    }
    // PRECONDITION: not an end() iterator.
    iterator operator++(int) {
      auto tmp = *this;
      ++*this;
      return tmp;
    }

    friend bool operator==(const iterator& a, const iterator& b) {
      return a.ctrl_ == b.ctrl_;
    }
    friend bool operator!=(const iterator& a, const iterator& b) {
      return !(a == b);
    }

   private:
    iterator(ctrl_t* ctrl) : ctrl_(ctrl) {}  // for end()
    iterator(ctrl_t* ctrl, slot_type* slot) : ctrl_(ctrl), slot_(slot) {}

    void skip_empty_or_deleted() {
      while (IsEmptyOrDeleted(*ctrl_)) {
        // ctrl is not necessarily aligned to Group::kWidth. It is also likely
        // to read past the space for ctrl bytes and into slots. This is ok
        // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
        // is no way to read outside the combined slot array.
        uint32_t shift = Group{ctrl_}.CountLeadingEmptyOrDeleted();
        ctrl_ += shift;
        slot_ += shift;
      }
    }

    ctrl_t* ctrl_ = nullptr;
    slot_type* slot_;
  };

  class const_iterator {
    friend class raw_hash_set;

   public:
    using iterator_category = typename iterator::iterator_category;
    using value_type = typename raw_hash_set::value_type;
    using reference = typename raw_hash_set::const_reference;
    using pointer = typename raw_hash_set::const_pointer;
    using difference_type = typename raw_hash_set::difference_type;

    const_iterator() {}
    // Implicit construction from iterator.
    const_iterator(iterator i) : inner_(std::move(i)) {}

    reference operator*() const { return *inner_; }
    pointer operator->() const { return inner_.operator->(); }

    const_iterator& operator++() {
      ++inner_;
      return *this;
    }
    const_iterator operator++(int) { return inner_++; }

    friend bool operator==(const const_iterator& a, const const_iterator& b) {
      return a.inner_ == b.inner_;
    }
    friend bool operator!=(const const_iterator& a, const const_iterator& b) {
      return !(a == b);
    }

   private:
    const_iterator(const ctrl_t* ctrl, const slot_type* slot)
        : inner_(const_cast<ctrl_t*>(ctrl), const_cast<slot_type*>(slot)) {}

    iterator inner_;
  };

  using node_type = container_internal::node_handle<Policy, Alloc>;

  raw_hash_set() noexcept(
      std::is_nothrow_default_constructible<hasher>::value&&
          std::is_nothrow_default_constructible<key_equal>::value&&
              std::is_nothrow_default_constructible<allocator_type>::value) {}

  explicit raw_hash_set(size_t bucket_count, const hasher& hash = hasher(),
                        const key_equal& eq = key_equal(),
                        const allocator_type& alloc = allocator_type())
      : ctrl_(EmptyGroup()), settings_(0, hash, eq, alloc) {
    if (bucket_count) {
      capacity_ = NormalizeCapacity(bucket_count);
      growth_left() = static_cast<size_t>(capacity_ * kMaxLoadFactor);
      initialize_slots();
    }
  }

  raw_hash_set(size_t bucket_count, const hasher& hash,
               const allocator_type& alloc)
      : raw_hash_set(bucket_count, hash, key_equal(), alloc) {}

  raw_hash_set(size_t bucket_count, const allocator_type& alloc)
      : raw_hash_set(bucket_count, hasher(), key_equal(), alloc) {}

  explicit raw_hash_set(const allocator_type& alloc)
      : raw_hash_set(0, hasher(), key_equal(), alloc) {}

  template <class InputIter>
  raw_hash_set(InputIter first, InputIter last, size_t bucket_count = 0,
               const hasher& hash = hasher(), const key_equal& eq = key_equal(),
               const allocator_type& alloc = allocator_type())
      : raw_hash_set(bucket_count, hash, eq, alloc) {
    insert(first, last);
  }

  template <class InputIter>
  raw_hash_set(InputIter first, InputIter last, size_t bucket_count,
               const hasher& hash, const allocator_type& alloc)
      : raw_hash_set(first, last, bucket_count, hash, key_equal(), alloc) {}

  template <class InputIter>
  raw_hash_set(InputIter first, InputIter last, size_t bucket_count,
               const allocator_type& alloc)
      : raw_hash_set(first, last, bucket_count, hasher(), key_equal(), alloc) {}

  template <class InputIter>
  raw_hash_set(InputIter first, InputIter last, const allocator_type& alloc)
      : raw_hash_set(first, last, 0, hasher(), key_equal(), alloc) {}

  // Instead of accepting std::initializer_list<value_type> as the first
  // argument like std::unordered_set<value_type> does, we have two overloads
  // that accept std::initializer_list<T> and std::initializer_list<init_type>.
  // This is advantageous for performance.
  //
  //   // Turns {"abc", "def"} into std::initializer_list<std::string>, then copies
  //   // the strings into the set.
  //   std::unordered_set<std::string> s = {"abc", "def"};
  //
  //   // Turns {"abc", "def"} into std::initializer_list<const char*>, then
  //   // copies the strings into the set.
  //   absl::flat_hash_set<std::string> s = {"abc", "def"};
  //
  // The same trick is used in insert().
  //
  // The enabler is necessary to prevent this constructor from triggering where
  // the copy constructor is meant to be called.
  //
  //   absl::flat_hash_set<int> a, b{a};
  //
  // RequiresNotInit<T> is a workaround for gcc prior to 7.1.
  template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
  raw_hash_set(std::initializer_list<T> init, size_t bucket_count = 0,
               const hasher& hash = hasher(), const key_equal& eq = key_equal(),
               const allocator_type& alloc = allocator_type())
      : raw_hash_set(init.begin(), init.end(), bucket_count, hash, eq, alloc) {}

  raw_hash_set(std::initializer_list<init_type> init, size_t bucket_count = 0,
               const hasher& hash = hasher(), const key_equal& eq = key_equal(),
               const allocator_type& alloc = allocator_type())
      : raw_hash_set(init.begin(), init.end(), bucket_count, hash, eq, alloc) {}

  template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
  raw_hash_set(std::initializer_list<T> init, size_t bucket_count,
               const hasher& hash, const allocator_type& alloc)
      : raw_hash_set(init, bucket_count, hash, key_equal(), alloc) {}

  raw_hash_set(std::initializer_list<init_type> init, size_t bucket_count,
               const hasher& hash, const allocator_type& alloc)
      : raw_hash_set(init, bucket_count, hash, key_equal(), alloc) {}

  template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
  raw_hash_set(std::initializer_list<T> init, size_t bucket_count,
               const allocator_type& alloc)
      : raw_hash_set(init, bucket_count, hasher(), key_equal(), alloc) {}

  raw_hash_set(std::initializer_list<init_type> init, size_t bucket_count,
               const allocator_type& alloc)
      : raw_hash_set(init, bucket_count, hasher(), key_equal(), alloc) {}

  template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
  raw_hash_set(std::initializer_list<T> init, const allocator_type& alloc)
      : raw_hash_set(init, 0, hasher(), key_equal(), alloc) {}

  raw_hash_set(std::initializer_list<init_type> init,
               const allocator_type& alloc)
      : raw_hash_set(init, 0, hasher(), key_equal(), alloc) {}

  raw_hash_set(const raw_hash_set& that)
      : raw_hash_set(that, AllocTraits::select_on_container_copy_construction(
                               that.alloc_ref())) {}

  raw_hash_set(const raw_hash_set& that, const allocator_type& a)
      : raw_hash_set(0, that.hash_ref(), that.eq_ref(), a) {
    reserve(that.size());
    // Because the table is guaranteed to be empty, we can do something faster
    // than a full `insert`.
    for (const auto& v : that) {
      const size_t hash = PolicyTraits::apply(HashElement{hash_ref()}, v);
      const size_t i = find_first_non_full(hash);
      set_ctrl(i, H2(hash));
      emplace_at(i, v);
    }
    size_ = that.size();
    growth_left() -= that.size();
  }

  raw_hash_set(raw_hash_set&& that) noexcept(
      std::is_nothrow_copy_constructible<hasher>::value&&
          std::is_nothrow_copy_constructible<key_equal>::value&&
              std::is_nothrow_copy_constructible<allocator_type>::value)
      : ctrl_(absl::exchange(that.ctrl_, EmptyGroup())),
        slots_(absl::exchange(that.slots_, nullptr)),
        size_(absl::exchange(that.size_, 0)),
        capacity_(absl::exchange(that.capacity_, 0)),
        // Hash, equality and allocator are copied instead of moved because
        // `that` must be left valid. If Hash is std::function<Key>, moving it
        // would create a nullptr functor that cannot be called.
        settings_(that.settings_) {
    // growth_left was copied above, reset the one from `that`.
    that.growth_left() = 0;
  }

  raw_hash_set(raw_hash_set&& that, const allocator_type& a)
      : ctrl_(EmptyGroup()),
        slots_(nullptr),
        size_(0),
        capacity_(0),
        settings_(0, that.hash_ref(), that.eq_ref(), a) {
    if (a == that.alloc_ref()) {
      std::swap(ctrl_, that.ctrl_);
      std::swap(slots_, that.slots_);
      std::swap(size_, that.size_);
      std::swap(capacity_, that.capacity_);
      std::swap(growth_left(), that.growth_left());
    } else {
      reserve(that.size());
      // Note: this will copy elements of dense_set and unordered_set instead of
      // moving them. This can be fixed if it ever becomes an issue.
      for (auto& elem : that) insert(std::move(elem));
    }
  }

  raw_hash_set& operator=(const raw_hash_set& that) {
    raw_hash_set tmp(that,
                     AllocTraits::propagate_on_container_copy_assignment::value
                         ? that.alloc_ref()
                         : alloc_ref());
    swap(tmp);
    return *this;
  }

  raw_hash_set& operator=(raw_hash_set&& that) noexcept(
      absl::allocator_traits<allocator_type>::is_always_equal::value&&
          std::is_nothrow_move_assignable<hasher>::value&&
              std::is_nothrow_move_assignable<key_equal>::value) {
    // TODO(sbenza): We should only use the operations from the noexcept clause
    // to make sure we actually adhere to that contract.
    return move_assign(
        std::move(that),
        typename AllocTraits::propagate_on_container_move_assignment());
  }

  ~raw_hash_set() { destroy_slots(); }

  iterator begin() {
    auto it = iterator_at(0);
    it.skip_empty_or_deleted();
    return it;
  }
  iterator end() { return {ctrl_ + capacity_}; }

  const_iterator begin() const {
    return const_cast<raw_hash_set*>(this)->begin();
  }
  const_iterator end() const { return const_cast<raw_hash_set*>(this)->end(); }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  bool empty() const { return !size(); }
  size_t size() const { return size_; }
  size_t capacity() const { return capacity_; }
  size_t max_size() const { return (std::numeric_limits<size_t>::max)(); }

  void clear() {
    // Iterating over this container is O(bucket_count()). When bucket_count()
    // is much greater than size(), iteration becomes prohibitively expensive.
    // For clear() it is more important to reuse the allocated array when the
    // container is small because allocation takes comparatively long time
    // compared to destruction of the elements of the container. So we pick the
    // largest bucket_count() threshold for which iteration is still fast and
    // past that we simply deallocate the array.
    if (capacity_ > 127) {
      destroy_slots();
    } else if (capacity_) {
      for (size_t i = 0; i != capacity_; ++i) {
        if (IsFull(ctrl_[i])) {
          PolicyTraits::destroy(&alloc_ref(), slots_ + i);
        }
      }
      size_ = 0;
      reset_ctrl();
      growth_left() = static_cast<size_t>(capacity_ * kMaxLoadFactor);
    }
    assert(empty());
  }

  // This overload kicks in when the argument is an rvalue of insertable and
  // decomposable type other than init_type.
  //
  //   flat_hash_map<std::string, int> m;
  //   m.insert(std::make_pair("abc", 42));
  template <class T, RequiresInsertable<T> = 0,
            typename std::enable_if<IsDecomposable<T>::value, int>::type = 0,
            T* = nullptr>
  std::pair<iterator, bool> insert(T&& value) {
    return emplace(std::forward<T>(value));
  }

  // This overload kicks in when the argument is a bitfield or an lvalue of
  // insertable and decomposable type.
  //
  //   union { int n : 1; };
  //   flat_hash_set<int> s;
  //   s.insert(n);
  //
  //   flat_hash_set<std::string> s;
  //   const char* p = "hello";
  //   s.insert(p);
  //
  // TODO(romanp): Once we stop supporting gcc 5.1 and below, replace
  // RequiresInsertable<T> with RequiresInsertable<const T&>.
  // We are hitting this bug: https://godbolt.org/g/1Vht4f.
  template <
      class T, RequiresInsertable<T> = 0,
      typename std::enable_if<IsDecomposable<const T&>::value, int>::type = 0>
  std::pair<iterator, bool> insert(const T& value) {
    return emplace(value);
  }

  // This overload kicks in when the argument is an rvalue of init_type. Its
  // purpose is to handle brace-init-list arguments.
  //
  //   flat_hash_set<std::string, int> s;
  //   s.insert({"abc", 42});
  std::pair<iterator, bool> insert(init_type&& value) {
    return emplace(std::move(value));
  }

  template <class T, RequiresInsertable<T> = 0,
            typename std::enable_if<IsDecomposable<T>::value, int>::type = 0,
            T* = nullptr>
  iterator insert(const_iterator, T&& value) {
    return insert(std::forward<T>(value)).first;
  }

  // TODO(romanp): Once we stop supporting gcc 5.1 and below, replace
  // RequiresInsertable<T> with RequiresInsertable<const T&>.
  // We are hitting this bug: https://godbolt.org/g/1Vht4f.
  template <
      class T, RequiresInsertable<T> = 0,
      typename std::enable_if<IsDecomposable<const T&>::value, int>::type = 0>
  iterator insert(const_iterator, const T& value) {
    return insert(value).first;
  }

  iterator insert(const_iterator, init_type&& value) {
    return insert(std::move(value)).first;
  }

  template <class InputIt>
  void insert(InputIt first, InputIt last) {
    for (; first != last; ++first) insert(*first);
  }

  template <class T, RequiresNotInit<T> = 0, RequiresInsertable<const T&> = 0>
  void insert(std::initializer_list<T> ilist) {
    insert(ilist.begin(), ilist.end());
  }

  void insert(std::initializer_list<init_type> ilist) {
    insert(ilist.begin(), ilist.end());
  }

  insert_return_type<iterator, node_type> insert(node_type&& node) {
    if (!node) return {end(), false, node_type()};
    const auto& elem = PolicyTraits::element(node.slot());
    auto res = PolicyTraits::apply(
        InsertSlot<false>{*this, std::move(*node.slot())}, elem);
    if (res.second) {
      node.reset();
      return {res.first, true, node_type()};
    } else {
      return {res.first, false, std::move(node)};
    }
  }

  iterator insert(const_iterator, node_type&& node) {
    return insert(std::move(node)).first;
  }

  // This overload kicks in if we can deduce the key from args. This enables us
  // to avoid constructing value_type if an entry with the same key already
  // exists.
  //
  // For example:
  //
  //   flat_hash_map<std::string, std::string> m = {{"abc", "def"}};
  //   // Creates no std::string copies and makes no heap allocations.
  //   m.emplace("abc", "xyz");
  template <class... Args, typename std::enable_if<
                               IsDecomposable<Args...>::value, int>::type = 0>
  std::pair<iterator, bool> emplace(Args&&... args) {
    return PolicyTraits::apply(EmplaceDecomposable{*this},
                               std::forward<Args>(args)...);
  }

  // This overload kicks in if we cannot deduce the key from args. It constructs
  // value_type unconditionally and then either moves it into the table or
  // destroys.
  template <class... Args, typename std::enable_if<
                               !IsDecomposable<Args...>::value, int>::type = 0>
  std::pair<iterator, bool> emplace(Args&&... args) {
    typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type
        raw;
    slot_type* slot = reinterpret_cast<slot_type*>(&raw);

    PolicyTraits::construct(&alloc_ref(), slot, std::forward<Args>(args)...);
    const auto& elem = PolicyTraits::element(slot);
    return PolicyTraits::apply(InsertSlot<true>{*this, std::move(*slot)}, elem);
  }

  template <class... Args>
  iterator emplace_hint(const_iterator, Args&&... args) {
    return emplace(std::forward<Args>(args)...).first;
  }

  // Extension API: support for lazy emplace.
  //
  // Looks up key in the table. If found, returns the iterator to the element.
  // Otherwise calls f with one argument of type raw_hash_set::constructor. f
  // MUST call raw_hash_set::constructor with arguments as if a
  // raw_hash_set::value_type is constructed, otherwise the behavior is
  // undefined.
  //
  // For example:
  //
  //   std::unordered_set<ArenaString> s;
  //   // Makes ArenaStr even if "abc" is in the map.
  //   s.insert(ArenaString(&arena, "abc"));
  //
  //   flat_hash_set<ArenaStr> s;
  //   // Makes ArenaStr only if "abc" is not in the map.
  //   s.lazy_emplace("abc", [&](const constructor& ctor) {
  //     ctor(&arena, "abc");
  //   });
  //
  // WARNING: This API is currently experimental. If there is a way to implement
  // the same thing with the rest of the API, prefer that.
  class constructor {
    friend class raw_hash_set;

   public:
    template <class... Args>
    void operator()(Args&&... args) const {
      assert(*slot_);
      PolicyTraits::construct(alloc_, *slot_, std::forward<Args>(args)...);
      *slot_ = nullptr;
    }

   private:
    constructor(allocator_type* a, slot_type** slot) : alloc_(a), slot_(slot) {}

    allocator_type* alloc_;
    slot_type** slot_;
  };

  template <class K = key_type, class F>
  iterator lazy_emplace(const key_arg<K>& key, F&& f) {
    auto res = find_or_prepare_insert(key);
    if (res.second) {
      slot_type* slot = slots_ + res.first;
      std::forward<F>(f)(constructor(&alloc_ref(), &slot));
      assert(!slot);
    }
    return iterator_at(res.first);
  }

  // Extension API: support for heterogeneous keys.
  //
  //   std::unordered_set<std::string> s;
  //   // Turns "abc" into std::string.
  //   s.erase("abc");
  //
  //   flat_hash_set<std::string> s;
  //   // Uses "abc" directly without copying it into std::string.
  //   s.erase("abc");
  template <class K = key_type>
  size_type erase(const key_arg<K>& key) {
    auto it = find(key);
    if (it == end()) return 0;
    erase(it);
    return 1;
  }

  // Erases the element pointed to by `it`.  Unlike `std::unordered_set::erase`,
  // this method returns void to reduce algorithmic complexity to O(1).  In
  // order to erase while iterating across a map, use the following idiom (which
  // also works for standard containers):
  //
  // for (auto it = m.begin(), end = m.end(); it != end;) {
  //   if (<pred>) {
  //     m.erase(it++);
  //   } else {
  //     ++it;
  //   }
  // }
  void erase(const_iterator cit) { erase(cit.inner_); }

  // This overload is necessary because otherwise erase<K>(const K&) would be
  // a better match if non-const iterator is passed as an argument.
  void erase(iterator it) {
    assert(it != end());
    PolicyTraits::destroy(&alloc_ref(), it.slot_);
    erase_meta_only(it);
  }

  iterator erase(const_iterator first, const_iterator last) {
    while (first != last) {
      erase(first++);
    }
    return last.inner_;
  }

  // Moves elements from `src` into `this`.
  // If the element already exists in `this`, it is left unmodified in `src`.
  template <typename H, typename E>
  void merge(raw_hash_set<Policy, H, E, Alloc>& src) {  // NOLINT
    assert(this != &src);
    for (auto it = src.begin(), e = src.end(); it != e; ++it) {
      if (PolicyTraits::apply(InsertSlot<false>{*this, std::move(*it.slot_)},
                              PolicyTraits::element(it.slot_))
              .second) {
        src.erase_meta_only(it);
      }
    }
  }

  template <typename H, typename E>
  void merge(raw_hash_set<Policy, H, E, Alloc>&& src) {
    merge(src);
  }

  node_type extract(const_iterator position) {
    node_type node(alloc_ref(), position.inner_.slot_);
    erase_meta_only(position);
    return node;
  }

  template <
      class K = key_type,
      typename std::enable_if<!std::is_same<K, iterator>::value, int>::type = 0>
  node_type extract(const key_arg<K>& key) {
    auto it = find(key);
    return it == end() ? node_type() : extract(const_iterator{it});
  }

  void swap(raw_hash_set& that) noexcept(
      IsNoThrowSwappable<hasher>() && IsNoThrowSwappable<key_equal>() &&
      (!AllocTraits::propagate_on_container_swap::value ||
       IsNoThrowSwappable<allocator_type>())) {
    using std::swap;
    swap(ctrl_, that.ctrl_);
    swap(slots_, that.slots_);
    swap(size_, that.size_);
    swap(capacity_, that.capacity_);
    swap(growth_left(), that.growth_left());
    swap(hash_ref(), that.hash_ref());
    swap(eq_ref(), that.eq_ref());
    if (AllocTraits::propagate_on_container_swap::value) {
      swap(alloc_ref(), that.alloc_ref());
    } else {
      // If the allocators do not compare equal it is officially undefined
      // behavior. We choose to do nothing.
    }
  }

  void rehash(size_t n) {
    if (n == 0 && capacity_ == 0) return;
    if (n == 0 && size_ == 0) return destroy_slots();
    auto m = NormalizeCapacity((std::max)(n, NumSlotsFast(size())));
    // n == 0 unconditionally rehashes as per the standard.
    if (n == 0 || m > capacity_) {
      resize(m);
    }
  }

  void reserve(size_t n) {
    rehash(NumSlotsFast(n));
  }

  // Extension API: support for heterogeneous keys.
  //
  //   std::unordered_set<std::string> s;
  //   // Turns "abc" into std::string.
  //   s.count("abc");
  //
  //   ch_set<std::string> s;
  //   // Uses "abc" directly without copying it into std::string.
  //   s.count("abc");
  template <class K = key_type>
  size_t count(const key_arg<K>& key) const {
    return find(key) == end() ? 0 : 1;
  }

  // Issues CPU prefetch instructions for the memory needed to find or insert
  // a key.  Like all lookup functions, this support heterogeneous keys.
  //
  // NOTE: This is a very low level operation and should not be used without
  // specific benchmarks indicating its importance.
  template <class K = key_type>
  void prefetch(const key_arg<K>& key) const {
    (void)key;
#if defined(__GNUC__)
    auto seq = probe(hash_ref()(key));
    __builtin_prefetch(static_cast<const void*>(ctrl_ + seq.offset()));
    __builtin_prefetch(static_cast<const void*>(slots_ + seq.offset()));
#endif  // __GNUC__
  }

  // The API of find() has two extensions.
  //
  // 1. The hash can be passed by the user. It must be equal to the hash of the
  // key.
  //
  // 2. The type of the key argument doesn't have to be key_type. This is so
  // called heterogeneous key support.
  template <class K = key_type>
  iterator find(const key_arg<K>& key, size_t hash) {
    auto seq = probe(hash);
    while (true) {
      Group g{ctrl_ + seq.offset()};
      for (int i : g.Match(H2(hash))) {
        if (ABSL_PREDICT_TRUE(PolicyTraits::apply(
                EqualElement<K>{key, eq_ref()},
                PolicyTraits::element(slots_ + seq.offset(i)))))
          return iterator_at(seq.offset(i));
      }
      if (ABSL_PREDICT_TRUE(g.MatchEmpty())) return end();
      seq.next();
    }
  }
  template <class K = key_type>
  iterator find(const key_arg<K>& key) {
    return find(key, hash_ref()(key));
  }

  template <class K = key_type>
  const_iterator find(const key_arg<K>& key, size_t hash) const {
    return const_cast<raw_hash_set*>(this)->find(key, hash);
  }
  template <class K = key_type>
  const_iterator find(const key_arg<K>& key) const {
    return find(key, hash_ref()(key));
  }

  template <class K = key_type>
  bool contains(const key_arg<K>& key) const {
    return find(key) != end();
  }

  template <class K = key_type>
  std::pair<iterator, iterator> equal_range(const key_arg<K>& key) {
    auto it = find(key);
    if (it != end()) return {it, std::next(it)};
    return {it, it};
  }
  template <class K = key_type>
  std::pair<const_iterator, const_iterator> equal_range(
      const key_arg<K>& key) const {
    auto it = find(key);
    if (it != end()) return {it, std::next(it)};
    return {it, it};
  }

  size_t bucket_count() const { return capacity_; }
  float load_factor() const {
    return capacity_ ? static_cast<double>(size()) / capacity_ : 0.0;
  }
  float max_load_factor() const { return 1.0f; }
  void max_load_factor(float) {
    // Does nothing.
  }

  hasher hash_function() const { return hash_ref(); }
  key_equal key_eq() const { return eq_ref(); }
  allocator_type get_allocator() const { return alloc_ref(); }

  friend bool operator==(const raw_hash_set& a, const raw_hash_set& b) {
    if (a.size() != b.size()) return false;
    const raw_hash_set* outer = &a;
    const raw_hash_set* inner = &b;
    if (outer->capacity() > inner->capacity()) std::swap(outer, inner);
    for (const value_type& elem : *outer)
      if (!inner->has_element(elem)) return false;
    return true;
  }

  friend bool operator!=(const raw_hash_set& a, const raw_hash_set& b) {
    return !(a == b);
  }

  friend void swap(raw_hash_set& a,
                   raw_hash_set& b) noexcept(noexcept(a.swap(b))) {
    a.swap(b);
  }

 private:
  template <class Container, typename Enabler>
  friend struct absl::container_internal::hashtable_debug_internal::
      HashtableDebugAccess;

  struct FindElement {
    template <class K, class... Args>
    const_iterator operator()(const K& key, Args&&...) const {
      return s.find(key);
    }
    const raw_hash_set& s;
  };

  struct HashElement {
    template <class K, class... Args>
    size_t operator()(const K& key, Args&&...) const {
      return h(key);
    }
    const hasher& h;
  };

  template <class K1>
  struct EqualElement {
    template <class K2, class... Args>
    bool operator()(const K2& lhs, Args&&...) const {
      return eq(lhs, rhs);
    }
    const K1& rhs;
    const key_equal& eq;
  };

  struct EmplaceDecomposable {
    template <class K, class... Args>
    std::pair<iterator, bool> operator()(const K& key, Args&&... args) const {
      auto res = s.find_or_prepare_insert(key);
      if (res.second) {
        s.emplace_at(res.first, std::forward<Args>(args)...);
      }
      return {s.iterator_at(res.first), res.second};
    }
    raw_hash_set& s;
  };

  template <bool do_destroy>
  struct InsertSlot {
    template <class K, class... Args>
    std::pair<iterator, bool> operator()(const K& key, Args&&...) && {
      auto res = s.find_or_prepare_insert(key);
      if (res.second) {
        PolicyTraits::transfer(&s.alloc_ref(), s.slots_ + res.first, &slot);
      } else if (do_destroy) {
        PolicyTraits::destroy(&s.alloc_ref(), &slot);
      }
      return {s.iterator_at(res.first), res.second};
    }
    raw_hash_set& s;
    // Constructed slot. Either moved into place or destroyed.
    slot_type&& slot;
  };

  // Computes std::ceil(n / kMaxLoadFactor). Faster than calling std::ceil.
  static inline size_t NumSlotsFast(size_t n) {
    return static_cast<size_t>(
        (n * kMaxLoadFactorDenominator + (kMaxLoadFactorNumerator - 1)) /
        kMaxLoadFactorNumerator);
  }

  // "erases" the object from the container, except that it doesn't actually
  // destroy the object. It only updates all the metadata of the class.
  // This can be used in conjunction with Policy::transfer to move the object to
  // another place.
  void erase_meta_only(const_iterator it) {
    assert(IsFull(*it.inner_.ctrl_) && "erasing a dangling iterator");
    --size_;
    const size_t index = it.inner_.ctrl_ - ctrl_;
    const size_t index_before = (index - Group::kWidth) & capacity_;
    const auto empty_after = Group(it.inner_.ctrl_).MatchEmpty();
    const auto empty_before = Group(ctrl_ + index_before).MatchEmpty();

    // We count how many consecutive non empties we have to the right and to the
    // left of `it`. If the sum is >= kWidth then there is at least one probe
    // window that might have seen a full group.
    bool was_never_full =
        empty_before && empty_after &&
        static_cast<size_t>(empty_after.TrailingZeros() +
                            empty_before.LeadingZeros()) < Group::kWidth;

    set_ctrl(index, was_never_full ? kEmpty : kDeleted);
    growth_left() += was_never_full;
  }

  void initialize_slots() {
    assert(capacity_);
    auto layout = MakeLayout(capacity_);
    char* mem = static_cast<char*>(
        Allocate<Layout::Alignment()>(&alloc_ref(), layout.AllocSize()));
    ctrl_ = reinterpret_cast<ctrl_t*>(layout.template Pointer<0>(mem));
    slots_ = layout.template Pointer<1>(mem);
    reset_ctrl();
    growth_left() = static_cast<size_t>(capacity_ * kMaxLoadFactor) - size_;
  }

  void destroy_slots() {
    if (!capacity_) return;
    for (size_t i = 0; i != capacity_; ++i) {
      if (IsFull(ctrl_[i])) {
        PolicyTraits::destroy(&alloc_ref(), slots_ + i);
      }
    }
    auto layout = MakeLayout(capacity_);
    // Unpoison before returning the memory to the allocator.
    SanitizerUnpoisonMemoryRegion(slots_, sizeof(slot_type) * capacity_);
    Deallocate<Layout::Alignment()>(&alloc_ref(), ctrl_, layout.AllocSize());
    ctrl_ = EmptyGroup();
    slots_ = nullptr;
    size_ = 0;
    capacity_ = 0;
    growth_left() = 0;
  }

  void resize(size_t new_capacity) {
    assert(IsValidCapacity(new_capacity));
    auto* old_ctrl = ctrl_;
    auto* old_slots = slots_;
    const size_t old_capacity = capacity_;
    capacity_ = new_capacity;
    initialize_slots();

    for (size_t i = 0; i != old_capacity; ++i) {
      if (IsFull(old_ctrl[i])) {
        size_t hash = PolicyTraits::apply(HashElement{hash_ref()},
                                          PolicyTraits::element(old_slots + i));
        size_t new_i = find_first_non_full(hash);
        set_ctrl(new_i, H2(hash));
        PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, old_slots + i);
      }
    }
    if (old_capacity) {
      SanitizerUnpoisonMemoryRegion(old_slots,
                                    sizeof(slot_type) * old_capacity);
      auto layout = MakeLayout(old_capacity);
      Deallocate<Layout::Alignment()>(&alloc_ref(), old_ctrl,
                                      layout.AllocSize());
    }
  }

  void drop_deletes_without_resize() ABSL_ATTRIBUTE_NOINLINE {
    assert(IsValidCapacity(capacity_));
    // Algorithm:
    // - mark all DELETED slots as EMPTY
    // - mark all FULL slots as DELETED
    // - for each slot marked as DELETED
    //     hash = Hash(element)
    //     target = find_first_non_full(hash)
    //     if target is in the same group
    //       mark slot as FULL
    //     else if target is EMPTY
    //       transfer element to target
    //       mark slot as EMPTY
    //       mark target as FULL
    //     else if target is DELETED
    //       swap current element with target element
    //       mark target as FULL
    //       repeat procedure for current slot with moved from element (target)
    ConvertDeletedToEmptyAndFullToDeleted(ctrl_, capacity_);
    typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type
        raw;
    slot_type* slot = reinterpret_cast<slot_type*>(&raw);
    for (size_t i = 0; i != capacity_; ++i) {
      if (!IsDeleted(ctrl_[i])) continue;
      size_t hash = PolicyTraits::apply(HashElement{hash_ref()},
                                        PolicyTraits::element(slots_ + i));
      size_t new_i = find_first_non_full(hash);

      // Verify if the old and new i fall within the same group wrt the hash.
      // If they do, we don't need to move the object as it falls already in the
      // best probe we can.
      const auto probe_index = [&](size_t pos) {
        return ((pos - probe(hash).offset()) & capacity_) / Group::kWidth;
      };

      // Element doesn't move.
      if (ABSL_PREDICT_TRUE(probe_index(new_i) == probe_index(i))) {
        set_ctrl(i, H2(hash));
        continue;
      }
      if (IsEmpty(ctrl_[new_i])) {
        // Transfer element to the empty spot.
        // set_ctrl poisons/unpoisons the slots so we have to call it at the
        // right time.
        set_ctrl(new_i, H2(hash));
        PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, slots_ + i);
        set_ctrl(i, kEmpty);
      } else {
        assert(IsDeleted(ctrl_[new_i]));
        set_ctrl(new_i, H2(hash));
        // Until we are done rehashing, DELETED marks previously FULL slots.
        // Swap i and new_i elements.
        PolicyTraits::transfer(&alloc_ref(), slot, slots_ + i);
        PolicyTraits::transfer(&alloc_ref(), slots_ + i, slots_ + new_i);
        PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, slot);
        --i;  // repeat
      }
    }
    growth_left() = static_cast<size_t>(capacity_ * kMaxLoadFactor) - size_;
  }

  void rehash_and_grow_if_necessary() {
    if (capacity_ == 0) {
      resize(Group::kWidth - 1);
    } else if (size() <= kMaxLoadFactor / 2 * capacity_) {
      // Squash DELETED without growing if there is enough capacity.
      drop_deletes_without_resize();
    } else {
      // Otherwise grow the container.
      resize(capacity_ * 2 + 1);
    }
  }

  bool has_element(const value_type& elem) const {
    size_t hash = PolicyTraits::apply(HashElement{hash_ref()}, elem);
    auto seq = probe(hash);
    while (true) {
      Group g{ctrl_ + seq.offset()};
      for (int i : g.Match(H2(hash))) {
        if (ABSL_PREDICT_TRUE(PolicyTraits::element(slots_ + seq.offset(i)) ==
                              elem))
          return true;
      }
      if (ABSL_PREDICT_TRUE(g.MatchEmpty())) return false;
      seq.next();
      assert(seq.index() < capacity_ && "full table!");
    }
    return false;
  }

  // Probes the raw_hash_set with the probe sequence for hash and returns the
  // pointer to the first empty or deleted slot.
  // NOTE: this function must work with tables having both kEmpty and kDelete
  // in one group. Such tables appears during drop_deletes_without_resize.
  //
  // This function is very useful when insertions happen and:
  // - the input is already a set
  // - there are enough slots
  // - the element with the hash is not in the table
  size_t find_first_non_full(size_t hash) {
    auto seq = probe(hash);
    while (true) {
      Group g{ctrl_ + seq.offset()};
      auto mask = g.MatchEmptyOrDeleted();
      if (mask) {
#if !defined(NDEBUG)
        // We want to force small tables to have random entries too, so
        // in debug build we will randomly insert in either the front or back of
        // the group.
        // TODO(kfm,sbenza): revisit after we do unconditional mixing
        if (ShouldInsertBackwards(hash, ctrl_))
          return seq.offset(mask.HighestBitSet());
        else
          return seq.offset(mask.LowestBitSet());
#else
        return seq.offset(mask.LowestBitSet());
#endif
      }
      assert(seq.index() < capacity_ && "full table!");
      seq.next();
    }
  }

  // TODO(alkis): Optimize this assuming *this and that don't overlap.
  raw_hash_set& move_assign(raw_hash_set&& that, std::true_type) {
    raw_hash_set tmp(std::move(that));
    swap(tmp);
    return *this;
  }
  raw_hash_set& move_assign(raw_hash_set&& that, std::false_type) {
    raw_hash_set tmp(std::move(that), alloc_ref());
    swap(tmp);
    return *this;
  }

 protected:
  template <class K>
  std::pair<size_t, bool> find_or_prepare_insert(const K& key) {
    auto hash = hash_ref()(key);
    auto seq = probe(hash);
    while (true) {
      Group g{ctrl_ + seq.offset()};
      for (int i : g.Match(H2(hash))) {
        if (ABSL_PREDICT_TRUE(PolicyTraits::apply(
                EqualElement<K>{key, eq_ref()},
                PolicyTraits::element(slots_ + seq.offset(i)))))
          return {seq.offset(i), false};
      }
      if (ABSL_PREDICT_TRUE(g.MatchEmpty())) break;
      seq.next();
    }
    return {prepare_insert(hash), true};
  }

  size_t prepare_insert(size_t hash) ABSL_ATTRIBUTE_NOINLINE {
    size_t target = find_first_non_full(hash);
    if (ABSL_PREDICT_FALSE(growth_left() == 0 && !IsDeleted(ctrl_[target]))) {
      rehash_and_grow_if_necessary();
      target = find_first_non_full(hash);
    }
    ++size_;
    growth_left() -= IsEmpty(ctrl_[target]);
    set_ctrl(target, H2(hash));
    return target;
  }

  // Constructs the value in the space pointed by the iterator. This only works
  // after an unsuccessful find_or_prepare_insert() and before any other
  // modifications happen in the raw_hash_set.
  //
  // PRECONDITION: i is an index returned from find_or_prepare_insert(k), where
  // k is the key decomposed from `forward<Args>(args)...`, and the bool
  // returned by find_or_prepare_insert(k) was true.
  // POSTCONDITION: *m.iterator_at(i) == value_type(forward<Args>(args)...).
  template <class... Args>
  void emplace_at(size_t i, Args&&... args) {
    PolicyTraits::construct(&alloc_ref(), slots_ + i,
                            std::forward<Args>(args)...);

    assert(PolicyTraits::apply(FindElement{*this}, *iterator_at(i)) ==
               iterator_at(i) &&
           "constructed value does not match the lookup key");
  }

  iterator iterator_at(size_t i) { return {ctrl_ + i, slots_ + i}; }
  const_iterator iterator_at(size_t i) const { return {ctrl_ + i, slots_ + i}; }

 private:
  friend struct RawHashSetTestOnlyAccess;

  probe_seq<Group::kWidth> probe(size_t hash) const {
    return probe_seq<Group::kWidth>(H1(hash, ctrl_), capacity_);
  }

  // Reset all ctrl bytes back to kEmpty, except the sentinel.
  void reset_ctrl() {
    std::memset(ctrl_, kEmpty, capacity_ + Group::kWidth);
    ctrl_[capacity_] = kSentinel;
    SanitizerPoisonMemoryRegion(slots_, sizeof(slot_type) * capacity_);
  }

  // Sets the control byte, and if `i < Group::kWidth`, set the cloned byte at
  // the end too.
  void set_ctrl(size_t i, ctrl_t h) {
    assert(i < capacity_);

    if (IsFull(h)) {
      SanitizerUnpoisonObject(slots_ + i);
    } else {
      SanitizerPoisonObject(slots_ + i);
    }

    ctrl_[i] = h;
    ctrl_[((i - Group::kWidth) & capacity_) + Group::kWidth] = h;
  }

  size_t& growth_left() { return settings_.template get<0>(); }

  hasher& hash_ref() { return settings_.template get<1>(); }
  const hasher& hash_ref() const { return settings_.template get<1>(); }
  key_equal& eq_ref() { return settings_.template get<2>(); }
  const key_equal& eq_ref() const { return settings_.template get<2>(); }
  allocator_type& alloc_ref() { return settings_.template get<3>(); }
  const allocator_type& alloc_ref() const {
    return settings_.template get<3>();
  }

  // On average each group has 2 empty slot (for the vectorized case).
  static constexpr int64_t kMaxLoadFactorNumerator = 14;
  static constexpr int64_t kMaxLoadFactorDenominator = 16;
  static constexpr float kMaxLoadFactor =
      1.0 * kMaxLoadFactorNumerator / kMaxLoadFactorDenominator;

  // TODO(alkis): Investigate removing some of these fields:
  // - ctrl/slots can be derived from each other
  // - size can be moved into the slot array
  ctrl_t* ctrl_ = EmptyGroup();    // [(capacity + 1) * ctrl_t]
  slot_type* slots_ = nullptr;     // [capacity * slot_type]
  size_t size_ = 0;                // number of full slots
  size_t capacity_ = 0;            // total number of slots
  absl::container_internal::CompressedTuple<size_t /* growth_left */, hasher,
                                            key_equal, allocator_type>
      settings_{0, hasher{}, key_equal{}, allocator_type{}};
};

namespace hashtable_debug_internal {
template <typename Set>
struct HashtableDebugAccess<Set, absl::void_t<typename Set::raw_hash_set>> {
  using Traits = typename Set::PolicyTraits;
  using Slot = typename Traits::slot_type;

  static size_t GetNumProbes(const Set& set,
                             const typename Set::key_type& key) {
    size_t num_probes = 0;
    size_t hash = set.hash_ref()(key);
    auto seq = set.probe(hash);
    while (true) {
      container_internal::Group g{set.ctrl_ + seq.offset()};
      for (int i : g.Match(container_internal::H2(hash))) {
        if (Traits::apply(
                typename Set::template EqualElement<typename Set::key_type>{
                    key, set.eq_ref()},
                Traits::element(set.slots_ + seq.offset(i))))
          return num_probes;
        ++num_probes;
      }
      if (g.MatchEmpty()) return num_probes;
      seq.next();
      ++num_probes;
    }
  }

  static size_t AllocatedByteSize(const Set& c) {
    size_t capacity = c.capacity_;
    if (capacity == 0) return 0;
    auto layout = Set::MakeLayout(capacity);
    size_t m = layout.AllocSize();

    size_t per_slot = Traits::space_used(static_cast<const Slot*>(nullptr));
    if (per_slot != ~size_t{}) {
      m += per_slot * c.size();
    } else {
      for (size_t i = 0; i != capacity; ++i) {
        if (container_internal::IsFull(c.ctrl_[i])) {
          m += Traits::space_used(c.slots_ + i);
        }
      }
    }
    return m;
  }

  static size_t LowerBoundAllocatedByteSize(size_t size) {
    size_t capacity = container_internal::NormalizeCapacity(
        std::ceil(size / Set::kMaxLoadFactor));
    if (capacity == 0) return 0;
    auto layout = Set::MakeLayout(capacity);
    size_t m = layout.AllocSize();
    size_t per_slot = Traits::space_used(static_cast<const Slot*>(nullptr));
    if (per_slot != ~size_t{}) {
      m += per_slot * size;
    }
    return m;
  }
};

}  // namespace hashtable_debug_internal
}  // namespace container_internal
}  // namespace absl

#endif  // ABSL_CONTAINER_INTERNAL_RAW_HASH_SET_H_
