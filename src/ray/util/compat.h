// Copyright 2017 The Ray Authors.
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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "ray/common/status.h"

#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#else
using pid_t = int;
#endif

// Workaround for multithreading on XCode 9, see
// https://issues.apache.org/jira/browse/ARROW-1622 and
// https://github.com/tensorflow/tensorflow/issues/13220#issuecomment-331579775
// This should be a short-term fix until the problem is fixed upstream.
#ifdef __APPLE__
#ifndef _MACH_PORT_T
#define _MACH_PORT_T
#include <sys/_types.h> /* __darwin_mach_port_t */
typedef __darwin_mach_port_t mach_port_t;
#include <pthread.h>

#include <utility>
mach_port_t pthread_mach_thread_np(pthread_t);
#endif /* _MACH_PORT_T */
#endif /* __APPLE__ */

#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

#ifdef _WIN32
#include <io.h>
#ifndef _WINDOWS_
#ifndef WIN32_LEAN_AND_MEAN  // Sorry for the inconvenience. Please include any related
                             // headers you need manually.
                             // (https://stackoverflow.com/a/8294669)
#define WIN32_LEAN_AND_MEAN  // Prevent inclusion of WinSock2.h
#endif                       // #ifndef WIN32_LEAN_AND_MEAN
#include <Windows.h>         // Force inclusion of WinGDI here to resolve name conflict
#endif                       // #ifndef _WINDOWS_
#define MEMFD_TYPE_NON_UNIQUE HANDLE
#define INVALID_FD NULL
// https://docs.microsoft.com/en-us/windows/win32/winauto/32-bit-and-64-bit-interoperability
#define FD2INT(x) (static_cast<int>(reinterpret_cast<std::uintptr_t>(x)))
#define INT2FD(x) (reinterpret_cast<HANDLE>(static_cast<std::uintptr_t>(x)))
#else
#define MEMFD_TYPE_NON_UNIQUE int
#define INVALID_FD -1
#define FD2INT(x) (x)
#define INT2FD(x) (x)
#endif  // #ifndef _WIN32

// Pair of (fd, unique_id). We need to separately track a unique id here
// since fd values can get re-used by the operating system.
#define MEMFD_TYPE std::pair<MEMFD_TYPE_NON_UNIQUE, int64_t>
#define INVALID_UNIQUE_FD_ID 0

namespace ray {
#if defined(__APPLE__) || defined(__linux__)
inline int GetStdoutFd() { return STDOUT_FILENO; }
inline int GetStderrFd() { return STDERR_FILENO; }
inline int Dup(int fd) { return dup(fd); }
inline int Dup2(int oldfd, int newfd) { return dup2(oldfd, newfd); }
#elif defined(_WIN32)
inline int GetStdoutFd() { return _fileno(stdout); }
inline int GetStderrFd() { return _fileno(stderr); }
inline int Dup(int fd) { return _dup(fd); }
inline int Dup2(int oldfd, int newfd) { return _dup2(oldfd, newfd); }
#endif

// Write the whole content into file descriptor, if any error happens, or actual written
// content is less than expected, IO error status will be returned.
Status CompleteWrite(int fd, const char *data, size_t len);
// Flush the given file descriptor, if EIO happens, error message is logged and process
// exits directly. Reference to fsyncgate: https://wiki.postgresql.org/wiki/Fsync_Errors
Status Flush(int fd);
// Close the given file descriptor, if any error happens, IO error status will be
// returned.
Status Close(int fd);
}  // namespace ray
