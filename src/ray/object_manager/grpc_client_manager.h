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

#include <memory>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"
#include "ray/rpc/grpc_client.h"

namespace ray::rpc {

// Managers multiple gRPC clients. It's reponsible for initializing
// gRPC clients with arguments, distributing requests between clients,
// and destroying the clients.
class ClientCallManager;

template <typename ServiceType>
class GrpcClientManager {
 public:
  GrpcClientManager() = default;
  GrpcClientManager(const GrpcClientManager &) = delete;
  GrpcClientManager &operator=(const GrpcClientManager &) = delete;
  GrpcClientManager(GrpcClientManager &&) = delete;
  GrpcClientManager &operator=(GrpcClientManager &&) = delete;

  virtual ~GrpcClientManager() = default;
  virtual GrpcClient<ServiceType> *GetGrpcClient() = 0;
};

template <typename ServiceType>
class GrpcClientManagerImpl final : public GrpcClientManager<ServiceType> {
 public:
  GrpcClientManagerImpl(const std::string &address,
                        int port,
                        ClientCallManager &client_call_manager) {
    const int conn_num = ::RayConfig::instance().object_manager_client_connection_num();
    grpc_clients_.reserve(conn_num);
    for (int idx = 0; idx < conn_num; ++idx) {
      grpc_clients_.emplace_back(
          std::make_unique<GrpcClient<ServiceType>>(address, port, client_call_manager));
    }
  }

  // Keeps track of gRPC clients and returns the next client
  // based on round-robin.
  GrpcClient<ServiceType> *GetGrpcClient() override {
    absl::MutexLock lock(&client_index_mutex_);
    client_index_ = (client_index_ + 1) % grpc_clients_.size();
    return grpc_clients_[client_index_].get();
  }

 private:
  absl::Mutex client_index_mutex_;
  size_t client_index_ ABSL_GUARDED_BY(client_index_mutex_) = 0;
  std::vector<std::unique_ptr<GrpcClient<ServiceType>>> grpc_clients_;
};

}  // namespace ray::rpc
