// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <utility>

#include "absl/synchronization/mutex.h"

namespace ray {

// A wrapper class that protects a value with a mutex. One can get a const& with a reader
// lock and a mutable & with a writer lock.
//
// Limitation: we only protect a single value. For the classes where a mutex protects
// multiple values, or with methods that requires/excludes locks, we have to write an
// impl class to be protected.
template <typename T>
class MutexProtected {
 public:
  template <typename... Args>
  explicit MutexProtected(Args &&...args) : value_(std::forward(args)...) {}

  MutexProtected() = default;

  class ReadLocked {
   public:
    ReadLocked(absl::Mutex *mutex, const T &value) : lock_(mutex), value_(value) {}

    const T &Get() const { return value_; }

   private:
    absl::ReaderMutexLock lock_;
    const T &value_;
  };

  class WriteLocked {
   public:
    WriteLocked(absl::Mutex *mutex, T &value) : lock_(mutex), value_(value) {}

    T &Get() { return value_; }

   private:
    absl::WriterMutexLock lock_;
    T &value_;
  };

  ReadLocked LockForRead() const { return ReadLocked(&mutex_, value_); }

  WriteLocked LockForWrite() { return WriteLocked(&mutex_, value_); }

 private:
  T value_;
  mutable absl::Mutex mutex_;
};

}  // namespace ray
