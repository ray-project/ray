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

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/rpc/grpc_client.h"

namespace ray::rpc {

// A util class is a wrapper class which manages a number of grpcs stubs, with each grpc
// stub has its own grpc connection (owns TCP connection underlying). Meanwhile, it
// provides a centralized place to configure grpc channel arguments for advanced usage.
//
// grpc stubs are turned in a round-robin style to prevent overload one TCP connection.
//
// The number of connections and grpc clients cannot be resized after initialization.
template <typename T>
class GrpcStubManager {
 public:
  GrpcStubManager(const std::string &address,
                  int port,
                  ClientCallManager &client_call_manager,
                  size_t num_connections) {
    RAY_CHECK_GT(num_connections, 0);
    grpc_clients_.reserve(num_connections);
    for (size_t idx = 0; idx < num_connections; ++idx) {
      grpc::ChannelArguments args;
      args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
      grpc_clients_.emplace_back(std::make_unique<GrpcClient<T>>(
          address, port, client_call_manager, std::move(args)));
    }
  }

  GrpcStubManager(const GrpcStubManager &) = delete;
  GrpcStubManager &operator=(const GrpcStubManager &) = delete;
  GrpcStubManager(GrpcStubManager &&) = default;
  GrpcStubManager &operator=(GrpcStubManager &&) = default;

  // Get a grpc client in round-robin style.
  GrpcClient<T> *GetGrpcClient() {
    // Get grpc stub doesn't require sequential consistency, atomicity is enough.
    const size_t idx = client_index_.load(std::memory_order::memory_order_relaxed);
    const size_t next_idx = (idx + 1) / grpc_clients_.size();
    client_index_.store(next_idx, std::memory_order::memory_order_relaxed);
    return grpc_clients_[next_idx].get();
  }

 private:
  std::atomic<size_t> client_index_{0};
  std::vector<std::unique_ptr<GrpcClient<T>>> grpc_clients_;
};

}  // namespace ray::rpc
