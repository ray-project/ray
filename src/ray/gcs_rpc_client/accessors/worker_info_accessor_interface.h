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

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// \class WorkerInfoAccessorInterface
/// Interface for WorkerInfo operations.
class WorkerInfoAccessorInterface {
 public:
  virtual ~WorkerInfoAccessorInterface() = default;

  /// Subscribe to all unexpected failure of workers from GCS asynchronously.
  /// Note that this does not include workers that failed due to node failure
  /// and only fileds in WorkerDeltaData would be published.
  ///
  /// \param subscribe Callback that will be called each time when a worker failed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToWorkerFailures(
      const ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const StatusCallback &done) = 0;

  /// Report worker failure to GCS.
  virtual void AsyncReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Get worker information by worker ID.
  virtual void AsyncGet(const WorkerID &worker_id,
                        const OptionalItemCallback<rpc::WorkerTableData> &callback) = 0;

  /// Get information for all workers.
  virtual void AsyncGetAll(const MultiItemCallback<rpc::WorkerTableData> &callback) = 0;

  /// Add worker information.
  virtual void AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                        const StatusCallback &callback) = 0;

  /// Resubscribe to worker failures.
  virtual void AsyncResubscribe() = 0;

  // Additional methods from original accessor.h
  virtual void AsyncUpdateDebuggerPort(const WorkerID &worker_id,
                                       uint32_t debugger_port,
                                       const StatusCallback &callback) = 0;

  virtual void AsyncUpdateWorkerNumPausedThreads(const WorkerID &worker_id,
                                                 int num_paused_threads_delta,
                                                 const StatusCallback &callback) = 0;
};

}  // namespace gcs
}  // namespace ray
