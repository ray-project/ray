// Copyright 2023 The Ray Authors.
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
#include "ray/core_worker/common.h"

namespace ray {
namespace core {

/// A class to pause a task execution while a generator is backpressured.
class GeneratorBackpressureWaiter {
 public:
  /// Create a generator backpressure waiter. One instance should be created
  /// for each streaming generator task.
  ///
  /// \param[in] generator_backpressure_num_objects The number of objects to
  /// generate before requiring a signal from the consumer that ObjectRefs have
  /// been consumed (yield'ed) to continue generation. This ensures that we do
  /// not generate too many objects in the object store during execution if the
  /// producer is faster than the consumer. Set to -1 to disable.
  /// \param[in] check_signals A callback to check Python signals while we are
  /// blocked in C++ code. If check_signals returns non-ok status, it finishes
  /// blocking and returns the non-ok status to the caller.
  GeneratorBackpressureWaiter(int64_t generator_backpressure_num_objects,
                              std::function<Status()> check_signals);

  /// Block and wait until enough objects are consumed from the consumer, so
  /// that we are under the backpressure threshold. Returns OK status if
  /// backpressure is not set or if backpressure is relieved.
  ///
  /// \return Status OK if backpressure is released and the executor may
  /// generate another item. Else, returns the status returned by check_signals
  /// callback.
  Status WaitUntilObjectConsumed();

  /// Block and wait until all objects that have been generated so far have
  /// been reported to the consumer.
  ///
  /// At the end of a streaming generator task, the executor should call this
  /// before sending the PushTaskReply RPC reply to the caller. Otherwise, we
  /// may fail after completing a task but before we have reported a return.
  /// Then, the consumer will mark the task as completed but will never receive
  /// the dynamic return's value.
  ///
  /// \return Status OK if all generator items have been reported. Else,
  /// returns the status returned by check_signals callback.
  Status WaitAllObjectsReported();

  void UpdateTotalObjectConsumed();

  /// Increment the number of objects generated. The executor should call this
  /// before sending an object report to the caller.
  void IncrementObjectGenerated();

  /// Handle a completed object report. The executor should call this after
  /// receiving an ack from the caller for an object report.
  ///
  /// Updates the total number of objects that have been consumed by the
  /// caller.  The caller "consumes" an object by calling next() on the
  /// ObjectRef generator.
  ///
  /// \param[in] total_objects_consumed The number of objects consumed by the
  /// caller will be set to this value, if it is greater than the previous
  /// value. Unblocks execution if the updated number of objects consumed puts
  /// us under the backpressure threshold. Setting this to the total objects
  /// generated will always unblock execution.
  void HandleObjectReported(int64_t total_objects_consumed);

  /// Get the total number of objects consumed by the caller so far.
  int64_t TotalObjectConsumed() const;

  /// Get the total number of objects generated so far.
  int64_t TotalObjectGenerated() const;

 private:
  mutable absl::Mutex mutex_;
  // Used to signal when backpressure is released and
  // execution may continue. Only used if
  // backpressure_threshold_ > 0.
  absl::CondVar backpressure_cond_var_;
  // Used to signal when all objects that have been generated so far have been
  // reported to the caller. At the end of a streaming generator task, we
  // should wait on this cond var before sending the PushTaskReply RPC reply to
  // the caller. Otherwise, we may fail after completing a task but before we
  // have reported a return.
  absl::CondVar all_objects_reported_cond_var_;
  // If total_objects_generated_ - total_objects_consumed_ < this
  // the task will stop.
  const int64_t backpressure_threshold_;
  const std::function<Status()> check_signals_;
  // Total number of objects generated from a generator.
  int64_t total_objects_generated_ = 0;
  // Total number of objects whose reports to the caller are in flight.
  int64_t num_object_reports_in_flight_ = 0;
  // Total number of objects consumed from a generator.
  int64_t total_objects_consumed_ = 0;
};

}  // namespace core
}  // namespace ray
