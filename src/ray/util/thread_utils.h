// Copyright 2025 The Ray Authors.
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

#ifdef __APPLE__
#include <pthread.h>
#endif

#ifdef __linux__
#include <sys/syscall.h>
#endif

#ifdef _WIN32
#ifndef _WINDOWS_
#ifndef WIN32_LEAN_AND_MEAN  // Sorry for the inconvenience. Please include any related
                             // headers you need manually.
                             // (https://stackoverflow.com/a/8294669)
#define WIN32_LEAN_AND_MEAN  // Prevent inclusion of WinSock2.h
#endif
#include <Windows.h>  // Force inclusion of WinGDI here to resolve name conflict
#endif
#endif

#include <string>
#include <utility>

#include "ray/util/thread_checker.h"

// Returns the TID of the calling thread.
#ifdef __APPLE__
inline uint64_t GetTid() {
  uint64_t tid;
  RAY_CHECK_EQ(pthread_threadid_np(NULL, &tid), 0);
  return tid;
}
#elif defined(_WIN32)
inline DWORD GetTid() { return GetCurrentThreadId(); }
#else
inline pid_t GetTid() { return syscall(__NR_gettid); }
#endif

inline std::string GetThreadName() {
#if defined(__linux__) || defined(__APPLE__)
  char name[128];
  auto rc = pthread_getname_np(pthread_self(), name, sizeof(name));
  if (rc != 0) {
    return "ERROR";
  } else {
    return name;
  }
#else
  return "UNKNOWN";
#endif
}

// Set [thread_name] to current thread; if it fails, error will be logged.
// NOTICE: It only works for macos and linux.
inline void SetThreadName(const std::string &thread_name) {
  int ret = 0;
#if defined(__APPLE__)
  ret = pthread_setname_np(thread_name.c_str());
#elif defined(__linux__)
  ret = pthread_setname_np(pthread_self(), thread_name.substr(0, 15).c_str());
#endif
  if (ret < 0) {
    RAY_LOG(ERROR) << "Fails to set thread name to " << thread_name << " since "
                   << strerror(errno);
  }
}

namespace ray {
template <typename T>
class ThreadPrivate {
 public:
  template <typename... Ts>
  explicit ThreadPrivate(Ts &&...ts) : t_(std::forward<Ts>(ts)...) {}

  T &operator*() {
    RAY_CHECK(thread_checker_.IsOnSameThread());
    return t_;
  }

  T *operator->() {
    RAY_CHECK(thread_checker_.IsOnSameThread());
    return &t_;
  }

  const T &operator*() const {
    RAY_CHECK(thread_checker_.IsOnSameThread());
    return t_;
  }

  const T *operator->() const {
    RAY_CHECK(thread_checker_.IsOnSameThread());
    return &t_;
  }

 private:
  T t_;
  mutable ThreadChecker thread_checker_;
};
}  // namespace ray
