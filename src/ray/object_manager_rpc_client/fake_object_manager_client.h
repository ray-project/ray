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

#include <grpcpp/grpcpp.h>

#include <functional>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "ray/object_manager_rpc_client/object_manager_client_interface.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

class FakeObjectManagerClient : public ObjectManagerClientInterface {
 public:
  FakeObjectManagerClient(const std::string &address,
                          const int port,
                          ClientCallManager &client_call_manager)
      : address_(address), port_(port) {}

  void Push(const PushRequest &request,
            const ClientCallback<PushReply> &callback) override {
    num_push_requests++;
    push_callbacks.push_back(callback);
  }

  void Pull(const PullRequest &request,
            const ClientCallback<PullReply> &callback) override {
    num_pull_requests++;
    pull_callbacks.push_back(callback);
  }

  void FreeObjects(const FreeObjectsRequest &request,
                   const ClientCallback<FreeObjectsReply> &callback) override {
    num_free_objects_requests++;
    free_objects_callbacks.push_back(callback);
  }

  bool ReplyPush(const Status &status = Status::OK()) {
    if (push_callbacks.empty()) {
      return false;
    }
    PushReply reply;
    auto callback = push_callbacks.front();
    push_callbacks.pop_front();
    callback(status, std::move(reply));
    return true;
  }

  bool ReplyPull(const Status &status = Status::OK()) {
    if (pull_callbacks.empty()) {
      return false;
    }
    PullReply reply;
    auto callback = pull_callbacks.front();
    pull_callbacks.pop_front();
    callback(status, std::move(reply));
    return true;
  }

  bool ReplyFreeObjects(const Status &status = Status::OK()) {
    if (free_objects_callbacks.empty()) {
      return false;
    }
    FreeObjectsReply reply;
    auto callback = free_objects_callbacks.front();
    free_objects_callbacks.pop_front();
    callback(status, std::move(reply));
    return true;
  }

  const std::string &GetAddress() const { return address_; }

  int GetPort() const { return port_; }

  uint32_t num_push_requests = 0;
  uint32_t num_pull_requests = 0;
  uint32_t num_free_objects_requests = 0;

  std::list<ClientCallback<PushReply>> push_callbacks;
  std::list<ClientCallback<PullReply>> pull_callbacks;
  std::list<ClientCallback<FreeObjectsReply>> free_objects_callbacks;

  std::string address_;
  int port_;
};

}  // namespace rpc
}  // namespace ray
