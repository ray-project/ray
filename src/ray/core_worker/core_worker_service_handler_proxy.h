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

#include <functional>
#include <memory>

#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace core {

#define RAY_CORE_WORKER_RPC_PROXY(METHOD)                                    \
  void Handle##METHOD(rpc::METHOD##Request request,                          \
                      rpc::METHOD##Reply *reply,                             \
                      rpc::SendReplyCallback send_reply_callback) override { \
    get_core_worker_()->Handle##METHOD(request, reply, send_reply_callback); \
  }

class CoreWorkerServiceHandlerProxy : public rpc::CoreWorkerServiceHandler {
 public:
  CoreWorkerServiceHandlerProxy(
      std::function<const std::shared_ptr<CoreWorker> &()> get_core_worker)
      : get_core_worker_(get_core_worker) {}

  RAY_CORE_WORKER_RPC_PROXY(PushTask)
  RAY_CORE_WORKER_RPC_PROXY(ActorCallArgWaitComplete)
  RAY_CORE_WORKER_RPC_PROXY(RayletNotifyGCSRestart)
  RAY_CORE_WORKER_RPC_PROXY(GetObjectStatus)
  RAY_CORE_WORKER_RPC_PROXY(WaitForActorRefDeleted)
  RAY_CORE_WORKER_RPC_PROXY(PubsubLongPolling)
  RAY_CORE_WORKER_RPC_PROXY(PubsubCommandBatch)
  RAY_CORE_WORKER_RPC_PROXY(UpdateObjectLocationBatch)
  RAY_CORE_WORKER_RPC_PROXY(GetObjectLocationsOwner)
  RAY_CORE_WORKER_RPC_PROXY(ReportGeneratorItemReturns)
  RAY_CORE_WORKER_RPC_PROXY(KillActor)
  RAY_CORE_WORKER_RPC_PROXY(CancelTask)
  RAY_CORE_WORKER_RPC_PROXY(RemoteCancelTask)
  RAY_CORE_WORKER_RPC_PROXY(RegisterMutableObjectReader)
  RAY_CORE_WORKER_RPC_PROXY(GetCoreWorkerStats)
  RAY_CORE_WORKER_RPC_PROXY(LocalGC)
  RAY_CORE_WORKER_RPC_PROXY(DeleteObjects)
  RAY_CORE_WORKER_RPC_PROXY(SpillObjects)
  RAY_CORE_WORKER_RPC_PROXY(RestoreSpilledObjects)
  RAY_CORE_WORKER_RPC_PROXY(DeleteSpilledObjects)
  RAY_CORE_WORKER_RPC_PROXY(PlasmaObjectReady)
  RAY_CORE_WORKER_RPC_PROXY(Exit)
  RAY_CORE_WORKER_RPC_PROXY(AssignObjectOwner)
  RAY_CORE_WORKER_RPC_PROXY(NumPendingTasks)
  RAY_CORE_WORKER_RPC_PROXY(FreeActorObject)

  /// Wait until the worker is initialized.
  void WaitUntilInitialized() override {
    absl::MutexLock lock(&initialize_mutex_);
    while (!initialized_) {
      intialize_cv_.WaitWithTimeout(&initialize_mutex_, absl::Seconds(1));
    }
  }

  void SetInitialized() {
    absl::MutexLock lock(&initialize_mutex_);
    initialized_ = true;
    intialize_cv_.SignalAll();
  }

 private:
  std::function<const std::shared_ptr<CoreWorker> &()> get_core_worker_;

  /// States that used for initialization.
  absl::Mutex initialize_mutex_;
  absl::CondVar intialize_cv_;
  bool initialized_ ABSL_GUARDED_BY(initialize_mutex_) = false;
};

}  // namespace core
}  // namespace ray
