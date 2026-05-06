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
#include <utility>

#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace core {

// Lock is unnecessary as SetCoreWorker is called only once and RPCs
// are blocked until it is called.
#define RAY_CORE_WORKER_RPC_PROXY(METHOD)                                    \
  void Handle##METHOD(rpc::METHOD##Request request,                          \
                      rpc::METHOD##Reply *reply,                             \
                      rpc::SendReplyCallback send_reply_callback) override { \
    core_worker_->Handle##METHOD(                                            \
        std::move(request), reply, std::move(send_reply_callback));          \
  }

/// Base for service handler proxies that forward RPCs to CoreWorker. Provides
/// the blocking WaitUntilInitialized / SetCoreWorker handshake so the gRPC server
/// can start before CoreWorker is fully constructed.
template <typename ServiceHandler>
class CoreWorkerServiceHandlerProxyBase : public ServiceHandler {
 public:
  void WaitUntilInitialized() override {
    std::unique_lock<std::mutex> lock(core_worker_mutex_);
    core_worker_cv_.wait(lock, [this]() { return this->core_worker_ != nullptr; });
  }

  void SetCoreWorker(CoreWorker *core_worker) {
    {
      std::scoped_lock<std::mutex> lock(core_worker_mutex_);
      core_worker_ = core_worker;
    }
    core_worker_cv_.notify_all();
  }

 protected:
  CoreWorker *core_worker_ = nullptr;

 private:
  std::mutex core_worker_mutex_;
  std::condition_variable core_worker_cv_;
};

// This proxy pattern was introduced in https://github.com/ray-project/ray/pull/54759
// so the gRPC server can start before CoreWorker is done initializing. NOT recommended
// for future use — it was chosen because other options were significantly more complex.
class CoreWorkerServiceHandlerProxy
    : public CoreWorkerServiceHandlerProxyBase<rpc::CoreWorkerServiceHandler> {
 public:
  RAY_CORE_WORKER_RPC_PROXY(PushTask)
  RAY_CORE_WORKER_RPC_PROXY(ActorCallArgWaitComplete)
  RAY_CORE_WORKER_RPC_PROXY(RayletNotifyGCSRestart)
  RAY_CORE_WORKER_RPC_PROXY(GetObjectStatus)
  RAY_CORE_WORKER_RPC_PROXY(WaitForActorRefDeleted)
  RAY_CORE_WORKER_RPC_PROXY(GetObjectLocationsOwner)
  RAY_CORE_WORKER_RPC_PROXY(ReportGeneratorItemReturns)
  RAY_CORE_WORKER_RPC_PROXY(KillActor)
  RAY_CORE_WORKER_RPC_PROXY(CancelTask)
  RAY_CORE_WORKER_RPC_PROXY(RequestOwnerToCancelTask)
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
  RAY_CORE_WORKER_RPC_PROXY(UpdateObjectLocationBatch)
};

class CoreWorkerPubsubServiceHandlerProxy
    : public CoreWorkerServiceHandlerProxyBase<rpc::CoreWorkerPubsubServiceHandler> {
 public:
  RAY_CORE_WORKER_RPC_PROXY(PubsubLongPolling)
  RAY_CORE_WORKER_RPC_PROXY(PubsubCommandBatch)
};

}  // namespace core
}  // namespace ray
