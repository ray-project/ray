// Copyright 2026 The Ray Authors.
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

#ifndef _WIN32
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace ray::gcs::internal {

enum class RedisTcpKeepaliveResult { kEnabled, kDegraded, kFailed };

// interval and probes have already been validated, and interval is positive.
// set_option(level, option, name, value) applies one socket option and reports
// whether it succeeded. Keeping the syscall at the call site lets tests reject
// individual options on real sockets without a process-wide syscall hook.
template <typename SetOption>
RedisTcpKeepaliveResult SetRedisTcpKeepalive(int interval,
                                             int probes,
                                             const SetOption &set_option) {
  if (!set_option(SOL_SOCKET, SO_KEEPALIVE, "SO_KEEPALIVE", 1)) {
    return RedisTcpKeepaliveResult::kFailed;
  }

  auto result = RedisTcpKeepaliveResult::kEnabled;
  static_cast<void>(interval);
  static_cast<void>(probes);
#if (defined(__linux__) && defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && \
     defined(TCP_KEEPCNT)) ||                                                 \
    (defined(__APPLE__) && defined(__MACH__))
  const struct {
    int option;
    const char *name;
    int value;
  } options[] = {
#if defined(__linux__)
    {TCP_KEEPIDLE, "TCP_KEEPIDLE", interval},
    {TCP_KEEPINTVL, "TCP_KEEPINTVL", interval},
    {TCP_KEEPCNT, "TCP_KEEPCNT", probes},
#else
    {TCP_KEEPALIVE, "TCP_KEEPALIVE", interval},
#endif
  };
  for (const auto &[option, name, value] : options) {
    if (!set_option(IPPROTO_TCP, option, name, value)) {
      // Keep the successfully applied options and attempt the remaining ones.
      result = RedisTcpKeepaliveResult::kDegraded;
    }
  }
#endif
  return result;
}

}  // namespace ray::gcs::internal
#endif  // !_WIN32
