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

#include <cstdint>

namespace ray {
namespace gcs {

/**
  @class ClientUtils

  Utility class for GCS client operations.
 */
class ClientUtils {
 public:
  /**
    Get the default GCS timeout in milliseconds.

    Default GCS Client timeout in milliseconds, as defined in
    RAY_gcs_server_request_timeout_seconds.

    @return The timeout value in milliseconds.
   */
  static int64_t GetGcsTimeoutMs();

 private:
  ClientUtils() = delete;
  ~ClientUtils() = delete;
  ClientUtils(const ClientUtils &) = delete;
  ClientUtils &operator=(const ClientUtils &) = delete;
};

}  // namespace gcs
}  // namespace ray
