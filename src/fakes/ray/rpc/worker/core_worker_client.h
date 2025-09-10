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

#include "absl/synchronization/mutex.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

class FakeCoreWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void PushNormalTask(std::unique_ptr<rpc::PushTaskRequest> request,
                      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    absl::MutexLock lock(&mutex_);
    callbacks_.push_back(callback);
  }

  bool ReplyPushTask(Status status = Status::OK(), bool exit = false) {
    rpc::ClientCallback<rpc::PushTaskReply> callback = nullptr;
    {
      absl::MutexLock lock(&mutex_);
      if (callbacks_.size() == 0) {
        return false;
      }
      callback = callbacks_.front();
      callbacks_.pop_front();
    }
    // call the callback without the lock to avoid deadlock.
    auto reply = rpc::PushTaskReply();
    if (exit) {
      reply.set_worker_exiting(true);
    }
    callback(status, std::move(reply));
    return true;
  }

  size_t GetNumCallbacks() {
    absl::MutexLock lock(&mutex_);
    return callbacks_.size();
  }

  std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

}  // namespace ray
