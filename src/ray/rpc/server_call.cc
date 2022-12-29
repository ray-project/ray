// Copyright 2022 The Ray Authors.
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

#include "ray/rpc/server_call.h"

#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {
namespace {

std::unique_ptr<boost::asio::thread_pool> &_GetServerCallExecutor() {
  static auto thread_pool = std::make_unique<boost::asio::thread_pool>(
      ::RayConfig::instance().num_server_call_thread());
  return thread_pool;
}

}  // namespace

boost::asio::thread_pool &GetServerCallExecutor() { return *_GetServerCallExecutor(); }

void DrainAndResetServerCallExecutor() {
  GetServerCallExecutor().join();
  _GetServerCallExecutor() = std::make_unique<boost::asio::thread_pool>(
      ::RayConfig::instance().num_server_call_thread());
}

}  // namespace rpc
}  // namespace ray
