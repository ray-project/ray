// Copyright 2017 The Ray Authors.
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

#include "ray/gcs_rpc_client/accessors/worker_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// \class WorkerInfoAccessor
/// Implementation of WorkerInfoAccessorInterface.
class WorkerInfoAccessor : public WorkerInfoAccessorInterface {
 public:
  WorkerInfoAccessor() = default;
  explicit WorkerInfoAccessor(GcsClientContext *context);
  virtual ~WorkerInfoAccessor() = default;

  /// Subscribe to all unexpected failure of workers from GCS asynchronously.
  /// Note that this does not include workers that failed due to node failure
  /// and only fileds in WorkerDeltaData would be published.
  ///
  /// \param subscribe Callback that will be called each time when a worker failed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToWorkerFailures(
      const ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const StatusCallback &done) override;

  /// Report worker failure to GCS.
  virtual void AsyncReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
      const StatusCallback &callback) override;

  /// Get worker information by worker ID.
  virtual void AsyncGet(
      const WorkerID &worker_id,
      const OptionalItemCallback<rpc::WorkerTableData> &callback) override;

  /// Get information for all workers.
  virtual void AsyncGetAll(
      const MultiItemCallback<rpc::WorkerTableData> &callback) override;

  /// Add worker information.
  virtual void AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                        const StatusCallback &callback) override;

  /// Resubscribe to worker failures.
  virtual void AsyncResubscribe() override;

  /// Update the worker debugger port in GCS asynchronously.
  ///
  /// \param worker_id The ID of worker to update in the GCS.
  /// \param debugger_port The debugger port of worker to update in the GCS.
  /// \param callback Callback that will be called after update finishes.
  virtual void AsyncUpdateDebuggerPort(const WorkerID &worker_id,
                                       uint32_t debugger_port,
                                       const StatusCallback &callback) override;

  /// Update the number of worker's paused threads in GCS asynchronously.
  ///
  /// \param worker_id The ID of worker to update in the GCS.
  /// \param num_paused_threads_delta The number of paused threads to update in the GCS.
  /// \param callback Callback that will be called after update finishes.
  virtual void AsyncUpdateWorkerNumPausedThreads(const WorkerID &worker_id,
                                                 int num_paused_threads_delta,
                                                 const StatusCallback &callback) override;

 private:
  // GCS client context for accessing RPC and subscriber.
  GcsClientContext *context_ = nullptr;

  // Save the subscribe operation
  using SubscribeOperation = std::function<Status(const StatusCallback &done)>;
  SubscribeOperation subscribe_operation_;
};

}  // namespace gcs
}  // namespace ray
