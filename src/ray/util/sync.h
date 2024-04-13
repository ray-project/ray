// Copyright 2021 Google LLC
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

// A small library of support functionality with no uapi dependencies that can
// be shared between agent and clients.  This is picked up by both the agent and
// clients.

#pragma once

#include <linux/futex.h>
#include <stdint.h>
#include <syscall.h>

#include <atomic>

namespace ray {

// Futex functions. See `man 2 futex` for a description of what a Futex is and
// how to use one. This Futex class supports any type `T` whose size is equal to
// int's size.
//
// Example:
// enum class SomeType {
//   kTypeZero,
//   kTypeOne,
//   kTypeTwo,
//   ...
//   kTypeFive,
// };
//
// Class member: std::atomic<SomeType> val_ = SomeType::kTypeZero;
//
// ...
//
// Thread 1:
// Futex::Wait(&val_, /*val=*/SomeType::kTypeZero);
// (This code causes thread 1 to sleep on the futex.)
//
// ...
//
// Thread 2:
// val_.store(SomeType::kTypeFive, std::memory_order_release);
// Futex::Wake(&val_, /*count=*/1);
// (This code wakes up thread 1.)
class Futex {
 public:
  // Wakes up threads waiting on the futex. Up to `count` threads waiting on `f`
  // are woken up. Note that the type `T` must be the same size as an int, which
  // is the size that futex supports. We use a template mainly to support enum
  // class types.
  template <class T>
  static int Wake(std::atomic<T> *f, int count) {
    static_assert(sizeof(T) == sizeof(int));
    int rc = futex(reinterpret_cast<std::atomic<int> *>(f), FUTEX_WAKE, count, nullptr);
    return rc;
  }

  // Waits on the futex `f` while its value is `val`. Note that the type `T`
  // must be the same size as an int, which is the size that futex supports. We
  // use a template mainly to support enum class types.
  template <class T>
  static int Wait(std::atomic<T> *f, T val) {
    static_assert(sizeof(T) == sizeof(int));
    while (true) {
      int rc = futex(reinterpret_cast<std::atomic<int> *>(f),
                     FUTEX_WAIT,
                     static_cast<int>(val),
                     nullptr);
      if (rc == 0) {
        if (f->load(std::memory_order_acquire) != val) {
          return rc;
        }
        // This was a spurious wakeup, so sleep on the futex again.
      } else {
        if (errno == EAGAIN) {
          // Futex value mismatch.
          return 0;
        }
        RAY_CHECK_EQ(errno, EINTR);
      }
    }
  }

 private:
  // The futex system call. See `man 2 futex`.
  static int futex(std::atomic<int> *uaddr,
                   int futex_op,
                   int val,
                   const timespec *timeout) {
    return syscall(__NR_futex, uaddr, futex_op, val, timeout);
  }
};

}  // namespace ray
