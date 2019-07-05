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

#ifndef ABSL_BASE_INTERNAL_UNALIGNED_ACCESS_H_
#define ABSL_BASE_INTERNAL_UNALIGNED_ACCESS_H_

#include <string.h>
#include <cstdint>

#include "absl/base/attributes.h"

// unaligned APIs

// Portable handling of unaligned loads, stores, and copies.
// On some platforms, like ARM, the copy functions can be more efficient
// then a load and a store.
//
// It is possible to implement all of these these using constant-length memcpy
// calls, which is portable and will usually be inlined into simple loads and
// stores if the architecture supports it. However, such inlining usually
// happens in a pass that's quite late in compilation, which means the resulting
// loads and stores cannot participate in many other optimizations, leading to
// overall worse code.

// The unaligned API is C++ only.  The declarations use C++ features
// (namespaces, inline) which are absent or incompatible in C.
#if defined(__cplusplus)

#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) ||\
    defined(MEMORY_SANITIZER)
// Consider we have an unaligned load/store of 4 bytes from address 0x...05.
// AddressSanitizer will treat it as a 3-byte access to the range 05:07 and
// will miss a bug if 08 is the first unaddressable byte.
// ThreadSanitizer will also treat this as a 3-byte access to 05:07 and will
// miss a race between this access and some other accesses to 08.
// MemorySanitizer will correctly propagate the shadow on unaligned stores
// and correctly report bugs on unaligned loads, but it may not properly
// update and report the origin of the uninitialized memory.
// For all three tools, replacing an unaligned access with a tool-specific
// callback solves the problem.

// Make sure uint16_t/uint32_t/uint64_t are defined.
#include <stdint.h>

extern "C" {
uint16_t __sanitizer_unaligned_load16(const void *p);
uint32_t __sanitizer_unaligned_load32(const void *p);
uint64_t __sanitizer_unaligned_load64(const void *p);
void __sanitizer_unaligned_store16(void *p, uint16_t v);
void __sanitizer_unaligned_store32(void *p, uint32_t v);
void __sanitizer_unaligned_store64(void *p, uint64_t v);
}  // extern "C"

namespace absl {
namespace base_internal {

inline uint16_t UnalignedLoad16(const void *p) {
  return __sanitizer_unaligned_load16(p);
}

inline uint32_t UnalignedLoad32(const void *p) {
  return __sanitizer_unaligned_load32(p);
}

inline uint64_t UnalignedLoad64(const void *p) {
  return __sanitizer_unaligned_load64(p);
}

inline void UnalignedStore16(void *p, uint16_t v) {
  __sanitizer_unaligned_store16(p, v);
}

inline void UnalignedStore32(void *p, uint32_t v) {
  __sanitizer_unaligned_store32(p, v);
}

inline void UnalignedStore64(void *p, uint64_t v) {
  __sanitizer_unaligned_store64(p, v);
}

}  // namespace base_internal
}  // namespace absl

#define ABSL_INTERNAL_UNALIGNED_LOAD16(_p) \
  (absl::base_internal::UnalignedLoad16(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD32(_p) \
  (absl::base_internal::UnalignedLoad32(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD64(_p) \
  (absl::base_internal::UnalignedLoad64(_p))

#define ABSL_INTERNAL_UNALIGNED_STORE16(_p, _val) \
  (absl::base_internal::UnalignedStore16(_p, _val))
#define ABSL_INTERNAL_UNALIGNED_STORE32(_p, _val) \
  (absl::base_internal::UnalignedStore32(_p, _val))
#define ABSL_INTERNAL_UNALIGNED_STORE64(_p, _val) \
  (absl::base_internal::UnalignedStore64(_p, _val))

#elif defined(UNDEFINED_BEHAVIOR_SANITIZER)

namespace absl {
namespace base_internal {

inline uint16_t UnalignedLoad16(const void *p) {
  uint16_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint32_t UnalignedLoad32(const void *p) {
  uint32_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint64_t UnalignedLoad64(const void *p) {
  uint64_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UnalignedStore16(void *p, uint16_t v) { memcpy(p, &v, sizeof v); }

inline void UnalignedStore32(void *p, uint32_t v) { memcpy(p, &v, sizeof v); }

inline void UnalignedStore64(void *p, uint64_t v) { memcpy(p, &v, sizeof v); }

}  // namespace base_internal
}  // namespace absl

#define ABSL_INTERNAL_UNALIGNED_LOAD16(_p) \
  (absl::base_internal::UnalignedLoad16(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD32(_p) \
  (absl::base_internal::UnalignedLoad32(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD64(_p) \
  (absl::base_internal::UnalignedLoad64(_p))

#define ABSL_INTERNAL_UNALIGNED_STORE16(_p, _val) \
  (absl::base_internal::UnalignedStore16(_p, _val))
#define ABSL_INTERNAL_UNALIGNED_STORE32(_p, _val) \
  (absl::base_internal::UnalignedStore32(_p, _val))
#define ABSL_INTERNAL_UNALIGNED_STORE64(_p, _val) \
  (absl::base_internal::UnalignedStore64(_p, _val))

#elif defined(__x86_64__) || defined(_M_X64) || defined(__i386) || \
    defined(_M_IX86) || defined(__ppc__) || defined(__PPC__) ||    \
    defined(__ppc64__) || defined(__PPC64__)

// x86 and x86-64 can perform unaligned loads/stores directly;
// modern PowerPC hardware can also do unaligned integer loads and stores;
// but note: the FPU still sends unaligned loads and stores to a trap handler!

#define ABSL_INTERNAL_UNALIGNED_LOAD16(_p) \
  (*reinterpret_cast<const uint16_t *>(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD32(_p) \
  (*reinterpret_cast<const uint32_t *>(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD64(_p) \
  (*reinterpret_cast<const uint64_t *>(_p))

#define ABSL_INTERNAL_UNALIGNED_STORE16(_p, _val) \
  (*reinterpret_cast<uint16_t *>(_p) = (_val))
#define ABSL_INTERNAL_UNALIGNED_STORE32(_p, _val) \
  (*reinterpret_cast<uint32_t *>(_p) = (_val))
#define ABSL_INTERNAL_UNALIGNED_STORE64(_p, _val) \
  (*reinterpret_cast<uint64_t *>(_p) = (_val))

#elif defined(__arm__) && \
      !defined(__ARM_ARCH_5__) && \
      !defined(__ARM_ARCH_5T__) && \
      !defined(__ARM_ARCH_5TE__) && \
      !defined(__ARM_ARCH_5TEJ__) && \
      !defined(__ARM_ARCH_6__) && \
      !defined(__ARM_ARCH_6J__) && \
      !defined(__ARM_ARCH_6K__) && \
      !defined(__ARM_ARCH_6Z__) && \
      !defined(__ARM_ARCH_6ZK__) && \
      !defined(__ARM_ARCH_6T2__)


// ARMv7 and newer support native unaligned accesses, but only of 16-bit
// and 32-bit values (not 64-bit); older versions either raise a fatal signal,
// do an unaligned read and rotate the words around a bit, or do the reads very
// slowly (trip through kernel mode). There's no simple #define that says just
// "ARMv7 or higher", so we have to filter away all ARMv5 and ARMv6
// sub-architectures. Newer gcc (>= 4.6) set an __ARM_FEATURE_ALIGNED #define,
// so in time, maybe we can move on to that.
//
// This is a mess, but there's not much we can do about it.
//
// To further complicate matters, only LDR instructions (single reads) are
// allowed to be unaligned, not LDRD (two reads) or LDM (many reads). Unless we
// explicitly tell the compiler that these accesses can be unaligned, it can and
// will combine accesses. On armcc, the way to signal this is done by accessing
// through the type (uint32_t __packed *), but GCC has no such attribute
// (it ignores __attribute__((packed)) on individual variables). However,
// we can tell it that a _struct_ is unaligned, which has the same effect,
// so we do that.

namespace absl {
namespace base_internal {

struct Unaligned16Struct {
  uint16_t value;
  uint8_t dummy;  // To make the size non-power-of-two.
} ABSL_ATTRIBUTE_PACKED;

struct Unaligned32Struct {
  uint32_t value;
  uint8_t dummy;  // To make the size non-power-of-two.
} ABSL_ATTRIBUTE_PACKED;

}  // namespace base_internal
}  // namespace absl

#define ABSL_INTERNAL_UNALIGNED_LOAD16(_p)                                  \
  ((reinterpret_cast<const ::absl::base_internal::Unaligned16Struct *>(_p)) \
       ->value)
#define ABSL_INTERNAL_UNALIGNED_LOAD32(_p)                                  \
  ((reinterpret_cast<const ::absl::base_internal::Unaligned32Struct *>(_p)) \
       ->value)

#define ABSL_INTERNAL_UNALIGNED_STORE16(_p, _val)                      \
  ((reinterpret_cast< ::absl::base_internal::Unaligned16Struct *>(_p)) \
       ->value = (_val))
#define ABSL_INTERNAL_UNALIGNED_STORE32(_p, _val)                      \
  ((reinterpret_cast< ::absl::base_internal::Unaligned32Struct *>(_p)) \
       ->value = (_val))

namespace absl {
namespace base_internal {

inline uint64_t UnalignedLoad64(const void *p) {
  uint64_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UnalignedStore64(void *p, uint64_t v) { memcpy(p, &v, sizeof v); }

}  // namespace base_internal
}  // namespace absl

#define ABSL_INTERNAL_UNALIGNED_LOAD64(_p) \
  (absl::base_internal::UnalignedLoad64(_p))
#define ABSL_INTERNAL_UNALIGNED_STORE64(_p, _val) \
  (absl::base_internal::UnalignedStore64(_p, _val))

#else

// ABSL_INTERNAL_NEED_ALIGNED_LOADS is defined when the underlying platform
// doesn't support unaligned access.
#define ABSL_INTERNAL_NEED_ALIGNED_LOADS

// These functions are provided for architectures that don't support
// unaligned loads and stores.

namespace absl {
namespace base_internal {

inline uint16_t UnalignedLoad16(const void *p) {
  uint16_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint32_t UnalignedLoad32(const void *p) {
  uint32_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint64_t UnalignedLoad64(const void *p) {
  uint64_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UnalignedStore16(void *p, uint16_t v) { memcpy(p, &v, sizeof v); }

inline void UnalignedStore32(void *p, uint32_t v) { memcpy(p, &v, sizeof v); }

inline void UnalignedStore64(void *p, uint64_t v) { memcpy(p, &v, sizeof v); }

}  // namespace base_internal
}  // namespace absl

#define ABSL_INTERNAL_UNALIGNED_LOAD16(_p) \
  (absl::base_internal::UnalignedLoad16(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD32(_p) \
  (absl::base_internal::UnalignedLoad32(_p))
#define ABSL_INTERNAL_UNALIGNED_LOAD64(_p) \
  (absl::base_internal::UnalignedLoad64(_p))

#define ABSL_INTERNAL_UNALIGNED_STORE16(_p, _val) \
  (absl::base_internal::UnalignedStore16(_p, _val))
#define ABSL_INTERNAL_UNALIGNED_STORE32(_p, _val) \
  (absl::base_internal::UnalignedStore32(_p, _val))
#define ABSL_INTERNAL_UNALIGNED_STORE64(_p, _val) \
  (absl::base_internal::UnalignedStore64(_p, _val))

#endif

#endif  // defined(__cplusplus), end of unaligned API

#endif  // ABSL_BASE_INTERNAL_UNALIGNED_ACCESS_H_
