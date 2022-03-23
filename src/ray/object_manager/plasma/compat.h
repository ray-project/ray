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

#ifdef _WIN32
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
