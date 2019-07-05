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
// UnscaledCycleClock
//    An UnscaledCycleClock yields the value and frequency of a cycle counter
//    that increments at a rate that is approximately constant.
//    This class is for internal / whitelisted use only, you should consider
//    using CycleClock instead.
//
// Notes:
// The cycle counter frequency is not necessarily the core clock frequency.
// That is, CycleCounter cycles are not necessarily "CPU cycles".
//
// An arbitrary offset may have been added to the counter at power on.
//
// On some platforms, the rate and offset of the counter may differ
// slightly when read from different CPUs of a multiprocessor.  Usually,
// we try to ensure that the operating system adjusts values periodically
// so that values agree approximately.   If you need stronger guarantees,
// consider using alternate interfaces.
//
// The CPU is not required to maintain the ordering of a cycle counter read
// with respect to surrounding instructions.

#ifndef ABSL_BASE_INTERNAL_UNSCALEDCYCLECLOCK_H_
#define ABSL_BASE_INTERNAL_UNSCALEDCYCLECLOCK_H_

#include <cstdint>

#if defined(__APPLE__)
#include <TargetConditionals.h>
#endif

#include "absl/base/port.h"

// The following platforms have an implementation of a hardware counter.
#if defined(__i386__) || defined(__x86_64__) || defined(__aarch64__) || \
  defined(__powerpc__) || defined(__ppc__) || \
  defined(_M_IX86) || defined(_M_X64)
#define ABSL_HAVE_UNSCALED_CYCLECLOCK_IMPLEMENTATION 1
#else
#define ABSL_HAVE_UNSCALED_CYCLECLOCK_IMPLEMENTATION 0
#endif

// The following platforms often disable access to the hardware
// counter (through a sandbox) even if the underlying hardware has a
// usable counter. The CycleTimer interface also requires a *scaled*
// CycleClock that runs at atleast 1 MHz. We've found some Android
// ARM64 devices where this is not the case, so we disable it by
// default on Android ARM64.
#if defined(__native_client__) || TARGET_OS_IPHONE || \
    (defined(__ANDROID__) && defined(__aarch64__))
#define ABSL_USE_UNSCALED_CYCLECLOCK_DEFAULT 0
#else
#define ABSL_USE_UNSCALED_CYCLECLOCK_DEFAULT 1
#endif

// UnscaledCycleClock is an optional internal feature.
// Use "#if ABSL_USE_UNSCALED_CYCLECLOCK" to test for its presence.
// Can be overridden at compile-time via -DABSL_USE_UNSCALED_CYCLECLOCK=0|1
#if !defined(ABSL_USE_UNSCALED_CYCLECLOCK)
#define ABSL_USE_UNSCALED_CYCLECLOCK               \
  (ABSL_HAVE_UNSCALED_CYCLECLOCK_IMPLEMENTATION && \
   ABSL_USE_UNSCALED_CYCLECLOCK_DEFAULT)
#endif

#if ABSL_USE_UNSCALED_CYCLECLOCK

// This macro can be used to test if UnscaledCycleClock::Frequency()
// is NominalCPUFrequency() on a particular platform.
#if  (defined(__i386__) || defined(__x86_64__) || \
      defined(_M_IX86) || defined(_M_X64))
#define ABSL_INTERNAL_UNSCALED_CYCLECLOCK_FREQUENCY_IS_CPU_FREQUENCY
#endif
namespace absl {
namespace time_internal {
class UnscaledCycleClockWrapperForGetCurrentTime;
}  // namespace time_internal

namespace base_internal {
class CycleClock;
class UnscaledCycleClockWrapperForInitializeFrequency;

class UnscaledCycleClock {
 private:
  UnscaledCycleClock() = delete;

  // Return the value of a cycle counter that counts at a rate that is
  // approximately constant.
  static int64_t Now();

  // Return the how much UnscaledCycleClock::Now() increases per second.
  // This is not necessarily the core CPU clock frequency.
  // It may be the nominal value report by the kernel, rather than a measured
  // value.
  static double Frequency();

  // Whitelisted friends.
  friend class base_internal::CycleClock;
  friend class time_internal::UnscaledCycleClockWrapperForGetCurrentTime;
  friend class base_internal::UnscaledCycleClockWrapperForInitializeFrequency;
};

}  // namespace base_internal
}  // namespace absl
#endif  // ABSL_USE_UNSCALED_CYCLECLOCK

#endif  // ABSL_BASE_INTERNAL_UNSCALEDCYCLECLOCK_H_
