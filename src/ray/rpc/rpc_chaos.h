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

#include <cstdint>
#include <string>

namespace ray {
namespace rpc {
namespace testing {

enum class RpcFailure : uint8_t {
  None,
  // Failure before server receives the request
  Request,
  // Failure after server sends the response
  Response,
};

/*
 * Get the random rpc failure to be injected
 * for the given rpc method.
 */
RpcFailure GetRpcFailure(const std::string &name);

/*
 * Initialize the rpc chaos framework (i.e. RpcFailureManager).
 * Should be called once before any rpc calls.
 */
void Init();

}  // namespace testing
}  // namespace rpc
}  // namespace ray
