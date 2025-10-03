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

#include "ray/gcs_rpc_client/accessors/task_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"

namespace ray {
namespace gcs {

/// \class TaskInfoAccessor
/// Implementation of TaskInfoAccessorInterface.
class TaskInfoAccessor : public TaskInfoAccessorInterface {
 public:
  TaskInfoAccessor() = default;
  explicit TaskInfoAccessor(GcsClientContext *context);
  virtual ~TaskInfoAccessor() = default;

  /// Add task event data to GCS asynchronously.
  ///
  /// \param data_ptr The task states event data that will be added to GCS.
  /// \param callback Callback that will be called when add is complete.
  virtual void AsyncAddTaskEventData(std::unique_ptr<rpc::TaskEventData> data_ptr,
                                     StatusCallback callback) override;

  /// Get all info/events of all tasks stored in GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetTaskEvents(
      const MultiItemCallback<rpc::TaskEvents> &callback) override;

 private:
  // GCS client implementation.
  GcsClientContext *context_ = nullptr;
};

}  // namespace gcs
}  // namespace ray
