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

#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"
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
                  ClientCallManager &client_call_manager) {
    const int conn_num = ::RayConfig::instance().object_manager_client_connection_num();
    grpc_clients_.reserve(conn_num);
    for (int idx = 0; idx < conn_num; ++idx) {
      grpc_clients_.emplace_back(
          std::make_unique<GrpcClient<T>>(address, port, client_call_manager));
    }
  }

  GrpcStubManager(const GrpcStubManager &) = delete;
  GrpcStubManager &operator=(const GrpcStubManager &) = delete;
  GrpcStubManager(GrpcStubManager &&) = default;
  GrpcStubManager &operator=(GrpcStubManager &&) = default;

  ~GrpcStubManager() = default;

  // Get a grpc client in round-robin style.
  GrpcClient<T> *GetGrpcClient() {
    absl::MutexLock lock(&client_index_mutex_);
    client_index_ = (client_index_ + 1) % grpc_clients_.size();
    return grpc_clients_[client_index_].get();
  }

 private:
  absl::Mutex client_index_mutex_;
  size_t client_index_ ABSL_GUARDED_BY(client_index_mutex_) = 0;
  std::vector<std::unique_ptr<GrpcClient<T>>> grpc_clients_;
};

}  // namespace ray::rpc
