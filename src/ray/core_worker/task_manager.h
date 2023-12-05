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

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace core {

class TaskFinisherInterface {
 public:
  virtual void CompletePendingTask(const TaskID &task_id,
                                   const rpc::PushTaskReply &reply,
                                   const rpc::Address &actor_addr,
                                   bool is_application_error) = 0;

  virtual bool RetryTaskIfPossible(const TaskID &task_id,
                                   const rpc::RayErrorInfo &error_info) = 0;

  virtual void FailPendingTask(const TaskID &task_id,
                               rpc::ErrorType error_type,
                               const Status *status = nullptr,
                               const rpc::RayErrorInfo *ray_error_info = nullptr) = 0;

  virtual bool FailOrRetryPendingTask(const TaskID &task_id,
                                      rpc::ErrorType error_type,
                                      const Status *status,
                                      const rpc::RayErrorInfo *ray_error_info = nullptr,
                                      bool mark_task_object_failed = true,
                                      bool fail_immediately = false) = 0;

  virtual void MarkTaskWaitingForExecution(const TaskID &task_id,
                                           const NodeID &node_id,
                                           const WorkerID &worker_id) = 0;

  virtual void OnTaskDependenciesInlined(
      const std::vector<ObjectID> &inlined_dependency_ids,
      const std::vector<ObjectID> &contained_ids) = 0;

  virtual void MarkDependenciesResolved(const TaskID &task_id) = 0;

  virtual bool MarkTaskCanceled(const TaskID &task_id) = 0;

  virtual absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const = 0;

  virtual ~TaskFinisherInterface() {}
};

class TaskResubmissionInterface {
 public:
  virtual bool ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) = 0;

  virtual ~TaskResubmissionInterface() {}
};

using TaskStatusCounter = CounterMap<std::tuple<std::string, rpc::TaskStatus, bool>>;
using PutInLocalPlasmaCallback =
    std::function<void(const RayObject &object, const ObjectID &object_id)>;
using RetryTaskCallback =
    std::function<void(TaskSpecification &spec, bool object_recovery, uint32_t delay_ms)>;
using ReconstructObjectCallback = std::function<void(const ObjectID &object_id)>;
using PushErrorCallback = std::function<Status(const JobID &job_id,
                                               const std::string &type,
                                               const std::string &error_message,
                                               double timestamp)>;
using ExecutionSignalCallback = std::function<void(Status, int64_t)>;

/// When the streaming generator tasks are submitted,
/// the intermediate return objects are streamed
/// back to the task manager.
/// This class manages the references of intermediately
/// streamed object references.
///
/// The API is not thread-safe.
class ObjectRefStream {
 public:
  ObjectRefStream(const ObjectID &generator_id)
      : generator_id_(generator_id),
        generator_task_id_(generator_id.TaskId()),
        total_num_object_written_(0),
        total_num_object_consumed_(0) {}

  /// Asynchronously read object reference of the next index.
  ///
  /// \param[out] object_id_out The next object ID from the stream.
  /// Nil ID is returned if the next index hasn't been written.
  /// \return KeyError if it reaches to EoF. Ok otherwise.
  Status TryReadNextItem(ObjectID *object_id_out);

  /// Return True if there's no more object to read. False otherwise.
  bool IsFinished() const;

  std::pair<ObjectID, bool> PeekNextItem();

  /// Return True if the item_index is already consumed.
  bool IsObjectConsumed(int64_t item_index);

  /// Insert the object id to the stream of an index item_index.
  ///
  /// If the item_index has been already read (by TryReadNextItem),
  /// the write request will be ignored. If the item_index has been
  /// already written, it will be no-op. It doesn't override.
  ///
  /// \param[in] object_id The object id that will be read at index item_index.
  /// \param[in] item_index The index where the object id will be written.
  /// If -1 is given, it means an index is not known yet. In this case,
  /// the ref will be temporarily written until it is written with an index.
  /// \return True if the ref is written to a stream. False otherwise.
  bool InsertToStream(const ObjectID &object_id, int64_t item_index);

  /// Sometimes, index of the object ID is not known.
  ///
  /// In this case, we should temporarily write the object ref to the
  /// stream until it is written with an index.
  ///
  /// In the following scenario, the API will be no-op because
  /// it means the object ID was already written with an index.
  /// - If the object ID is already consumed.
  /// - If the object ID is already written with an index.
  ///
  /// \param[in] object_id The temporarily written object id.
  /// \return True if object ID is temporarily written. False otherwise.
  bool TemporarilyInsertToStreamIfNeeded(const ObjectID &object_id);

  /// Mark that after a given item_index, the stream cannot be written
  /// anymore.
  ///
  /// \param[in] The last item index that means the end of stream.
  void MarkEndOfStream(int64_t item_index, ObjectID *object_id_in_last_index);

  /// Get all the ObjectIDs that are not read yet via TryReadNextItem.
  ///
  /// \return A list of object IDs that are not read yet.
  absl::flat_hash_set<ObjectID> GetItemsUnconsumed() const;

  /// \return Index of the last consumed item, -1 if nothing is consumed yet.
  int64_t LastConsumedIndex() const { return next_index_ - 1; }

  /// Total number of object that's written to the stream
  int64_t TotalNumObjectWritten() const { return total_num_object_written_; }
  int64_t TotalNumObjectConsumed() const { return total_num_object_consumed_; }

 private:
  ObjectID GetObjectRefAtIndex(int64_t generator_index) const;

  const ObjectID generator_id_;
  const TaskID generator_task_id_;

  /// Refs that are temporarily owned. It means a ref is
  /// written to a stream, but index is not known yet.
  absl::flat_hash_set<ObjectID> temporarily_owned_refs_;
  // A set of refs that's already written to a stream -> size of the object.
  absl::flat_hash_set<ObjectID> refs_written_to_stream_;
  /// The last index of the stream.
  /// item_index < last will contain object references.
  /// If -1, that means the stream hasn't reached to EoF.
  int64_t end_of_stream_index_ = -1;
  /// The next index of the stream.
  /// If next_index_ == end_of_stream_index_, that means it is the end of the stream.
  int64_t next_index_ = 0;
  /// The maximum index that we have seen from the executor. We need to track
  /// this in case the first execution fails mid-generator, and the second task
  /// ends with fewer returns. Then, we mark one past this index as the end of
  /// the stream.
  int64_t max_index_seen_ = -1;
  /// The total number of the objects that are written to stream.
  int64_t total_num_object_written_;
  /// The total number of the objects that are consumed from stream.
  int64_t total_num_object_consumed_;
};

class TaskManager : public TaskFinisherInterface, public TaskResubmissionInterface {
 public:
  TaskManager(std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
              std::shared_ptr<ReferenceCounter> reference_counter,
              PutInLocalPlasmaCallback put_in_local_plasma_callback,
              RetryTaskCallback retry_task_callback,
              PushErrorCallback push_error_callback,
              int64_t max_lineage_bytes,
              worker::TaskEventBuffer &task_event_buffer)
      : in_memory_store_(in_memory_store),
        reference_counter_(reference_counter),
        put_in_local_plasma_callback_(put_in_local_plasma_callback),
        retry_task_callback_(retry_task_callback),
        push_error_callback_(push_error_callback),
        max_lineage_bytes_(max_lineage_bytes),
        task_event_buffer_(task_event_buffer) {
    task_counter_.SetOnChangeCallback(
        [this](const std::tuple<std::string, rpc::TaskStatus, bool> key)
            ABSL_EXCLUSIVE_LOCKS_REQUIRED(&mu_) {
              ray::stats::STATS_tasks.Record(
                  task_counter_.Get(key),
                  {{"State", rpc::TaskStatus_Name(std::get<1>(key))},
                   {"Name", std::get<0>(key)},
                   {"IsRetry", std::get<2>(key) ? "1" : "0"},
                   {"Source", "owner"}});
            });
    reference_counter_->SetReleaseLineageCallback(
        [this](const ObjectID &object_id, std::vector<ObjectID> *ids_to_release) {
          return RemoveLineageReference(object_id, ids_to_release);
          ShutdownIfNeeded();
        });
  }

  /// Add a task that is pending execution.
  ///
  /// The local ref count for all return refs (excluding actor creation tasks)
  /// will be initialized to 1 so that the ref is considered in scope before
  /// returning to the language frontend. The caller is responsible for
  /// decrementing the ref count once the frontend ref has gone out of scope.
  ///
  /// \param[in] caller_address The rpc address of the calling task.
  /// \param[in] spec The spec of the pending task.
  /// \param[in] max_retries Number of times this task may be retried
  /// on failure.
  /// \return ObjectRefs returned by this task.
  std::vector<rpc::ObjectReference> AddPendingTask(const rpc::Address &caller_address,
                                                   const TaskSpecification &spec,
                                                   const std::string &call_site,
                                                   int max_retries = 0);

  /// Resubmit a task that has completed execution before. This is used to
  /// reconstruct objects stored in Plasma that were lost.
  ///
  /// \param[in] task_id The ID of the task to resubmit.
  /// \param[out] task_deps The object dependencies of the resubmitted task,
  /// i.e. all arguments that were not inlined in the task spec. The caller is
  /// responsible for making sure that these dependencies become available, so
  /// that the resubmitted task can run. This is only populated if the task was
  /// not already pending and was successfully resubmitted.
  /// \return OK if the task was successfully resubmitted or was
  /// already pending, Invalid if the task spec is no longer present.
  bool ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) override;

  /// Wait for all pending tasks to finish, and then shutdown.
  ///
  /// \param shutdown The shutdown callback to call.
  void DrainAndShutdown(std::function<void()> shutdown);

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \param[in] worker_addr Address of the worker that executed the task.
  /// \param[in] is_application_error Whether this is an Exception return.
  /// \return Void.
  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &reply,
                           const rpc::Address &worker_addr,
                           bool is_application_error) override;

  /**
   * The below APIs support streaming generator.
   *
   * API NOTES
   * ---------
   * - The stream must be created when a task is submitted first time. The stream
   * must be deleted by the language frontend when the stream
   * is not used anymore. The DelObjectRefStream APIs guarantee to clean
   * up object references associated with the stream.
   * - The generator return values are reported via HandleReportGeneratorItemReturns.
   * The report ordering is not guaranteed. HandleReportGeneratorItemReturns
   * must handle the out of ordering report correctly.
   * - Streaming generator return reference IDs are deterministic.
   * Ray preserves first `max_num_generator_returns` indexes for a streaming
   * generator returns.
   * - MarkEndOfStream must be called when a task finishes or fails.
   * Once this API is called, the stream will contain the sentinel object
   * that raises END_OF_STREAMING_GENERATOR error at the end of the stream.
   * The language frontend can catch this error and take proper actions.
   * - The generator's first return value contains an exception
   * if the task fails by a system error. Otherwise, it contains nothing.
   *
   * Backpressure Impl
   * -----------------
   * Streaming generator optionally supports backpressure when
   * `generator_backpressure_num_objects` is included in a task spec.
   *
   * Executor Side:
   * - When a new object is yielded, executor sends a gRPC request that
   *   contains an object size and records total_object_generated.
   * - If a total_object_generated - total_object_consumed > threshold,
   *   it blocks a thread and pauses execution. The consumer communicates
   *   `object_consumed` (via gRPC reply) when objects are consumed from it,
   *   and the execution resumes.
   * - If a gRPC request fails, the executor assumes all the objects are
   *   consumed and resume execution. (alternatively, we can fail execution).
   *
   * Client Side:
   * - If object_generated - object_consumed < threshold, it sends a reply that
   *   contains `object_consumed` to an executor immediately.
   * - If object_generated - object_consumed > threshold, it doesn't reply
   *   until objects are consumed via TryReadObjectRefStream.
   * - If objects are not going to be consumed (e.g., generator is deleted
   *   or objects are already consumed), it replies immediately.
   *
   * Reference implementation of streaming generator using the following APIs
   * is available from `_raylet.ObjectRefGenerator`.
   */

  /// Handle the generator task return so that it will be accessible
  /// via TryReadObjectRefStream.
  ///
  /// Generator tasks can report task returns before task is finished.
  /// It is the opposite of regular tasks which can only batch
  /// report the task returns after the task finishes.
  ///
  /// \param[in] request The request that contains reported objects.
  /// \param[in] execution_signal_callback Note: this callback is NOT GUARANTEED
  /// to run in the same thread as the caller.
  /// The callback that receives arguments "status" and
  /// "total_num_object_consumed". status: OK if the object will be consumed/already
  /// consumed. NotFound if the stream is already deleted or the object is from the
  /// previous attempt. total_num_object_consumed: total objects consumed from the
  /// generator. The executor can receive the value to decide to resume execution or keep
  /// being backpressured. If status is not OK, this must be -1.
  ///
  /// \return True if a task return is registered. False otherwise.
  bool HandleReportGeneratorItemReturns(
      const rpc::ReportGeneratorItemReturnsRequest &request,
      ExecutionSignalCallback execution_signal_callback) ABSL_LOCKS_EXCLUDED(mu_);

  /// Temporarily register a given generator return reference.
  ///
  /// For a generator return, the references are not known until
  /// it is reported from an executor (via HandleReportGeneratorItemReturns).
  /// However, there are times when generator return references need to be
  /// owned before the return values are reported.
  ///
  /// For example, when an object is created or spilled from the object store,
  /// pinning or OBOD update requests could be sent from raylets,
  /// and it is possible those requests come before generator returns
  /// are reported. In this case, we should own a reference temporarily,
  /// otherwise, these requests will be ignored.
  ///
  /// In the following scenario, references don't need to be owned. In this case,
  /// the API will be no-op.
  /// - The stream has been already deleted.
  /// - The reference is already read/consumed from a stream.
  ///   In this case, we already owned or GC'ed the refernece.
  /// - The reference is already owned via HandleReportGeneratorItemReturns.
  ///
  /// \param object_id The object ID to temporarily owns.
  /// \param generator_id The return ref ID of a generator task.
  /// \return True if we temporarily owned the reference. False otherwise.
  bool TemporarilyOwnGeneratorReturnRefIfNeeded(const ObjectID &object_id,
                                                const ObjectID &generator_id)
      ABSL_LOCKS_EXCLUDED(mu_);

  /// Delete the object ref stream.
  ///
  /// Once the stream is deleted, it will clean up all unconsumed
  /// object references, and all the future intermediate report
  /// will be ignored.
  ///
  /// This method is idempotent. It is because the language
  /// frontend often calls this method upon destructor, but
  /// not every langauge guarantees the destructor is called
  /// only once.
  ///
  /// \param[in] generator_id The object ref id of the streaming
  /// generator task.
  void DelObjectRefStream(const ObjectID &generator_id) ABSL_LOCKS_EXCLUDED(mu_);

  /// Return true if the object ref stream exists.
  ///
  /// \param[in] generator_id The object ref id of the streaming
  /// generator task.
  bool ObjectRefStreamExists(const ObjectID &generator_id) ABSL_LOCKS_EXCLUDED(mu_);

  /// Read object reference of the next index from the
  /// object stream of a generator_id.
  ///
  /// This API consumes the next index, meaning it is not idempotent.
  /// If you don't want to consume the next index, use PeekObjectRefStream.
  /// This API always return immediately.
  ///
  /// The caller should ensure the ObjectRefStream is already
  /// created, by calling AddPendingTask.
  /// If it is called after the stream hasn't been created or deleted
  /// it will panic.
  ///
  /// \param[out] object_id_out The next object ID from the stream.
  /// Nil ID is returned if the next index hasn't been written.
  /// \return ObjectRefEndOfStream if it reaches to EoF. Ok otherwise.
  Status TryReadObjectRefStream(const ObjectID &generator_id, ObjectID *object_id_out)
      ABSL_LOCKS_EXCLUDED(mu_);

  /// Return True if there's no more object to read. False otherwise.
  bool IsFinished(const ObjectID &generator_id) const ABSL_LOCKS_EXCLUDED(mu_);

  /// Read the next index of a ObjectRefStream of generator_id without
  /// consuming an index.
  ///
  /// This API must be idempotent.
  ///
  /// \param[in] generator_id The object ref id of the streaming
  /// generator task.
  /// \return A object reference of the next index and if the object is already ready
  /// (meaning if the object's value if retrievable).
  /// It should not be nil.
  std::pair<ObjectID, bool> PeekObjectRefStream(const ObjectID &generator_id)
      ABSL_LOCKS_EXCLUDED(mu_);

  /// Returns true if task can be retried.
  ///
  /// \param[in] task_id ID of the task to be retried.
  /// \return true if task is scheduled to be retried.
  bool RetryTaskIfPossible(const TaskID &task_id,
                           const rpc::RayErrorInfo &error_info) override;

  /// A pending task failed. This will either retry the task or mark the task
  /// as failed if there are no retries left.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \param[in] status Optional status message.
  /// \param[in] ray_error_info The error information of a given error type.
  /// Nullptr means that there's no error information.
  /// TODO(sang): Remove nullptr case. Every error message should have metadata.
  /// \param[in] mark_task_object_failed whether or not it marks the task
  /// return object as failed. If this is set to false, then the caller is
  /// responsible for later failing or completing the task.
  /// \param[in] fail_immediately whether to fail the task and ignore
  /// the retries that are available.
  /// \return Whether the task will be retried or not.
  bool FailOrRetryPendingTask(const TaskID &task_id,
                              rpc::ErrorType error_type,
                              const Status *status = nullptr,
                              const rpc::RayErrorInfo *ray_error_info = nullptr,
                              bool mark_task_object_failed = true,
                              bool fail_immediately = false) override;

  /// A pending task failed. This will mark the task as failed.
  /// This doesn't always mark the return object as failed
  /// depending on mark_task_object_failed.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \param[in] status Optional status message.
  /// \param[in] ray_error_info The error information of a given error type.
  /// \param[in] mark_task_object_failed whether or not it marks the task
  /// return object as failed.
  void FailPendingTask(const TaskID &task_id,
                       rpc::ErrorType error_type,
                       const Status *status = nullptr,
                       const rpc::RayErrorInfo *ray_error_info = nullptr) override;

  /// Treat a pending task's returned Ray object as failed. The lock should not be held
  /// when calling this method because it may trigger callbacks in this or other classes.
  ///
  /// \param[in] spec The TaskSpec that contains return object.
  /// \param[in] error_type The error type the returned Ray object will store.
  /// \param[in] ray_error_info The error information of a given error type.
  void MarkTaskReturnObjectsFailed(
      const TaskSpecification &spec,
      rpc::ErrorType error_type,
      const rpc::RayErrorInfo *ray_error_info,
      const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) ABSL_LOCKS_EXCLUDED(mu_);

  /// A task's dependencies were inlined in the task spec. This will decrement
  /// the ref count for the dependency IDs. If the dependencies contained other
  /// ObjectIDs, then the ref count for these object IDs will be incremented.
  ///
  /// \param[in] inlined_dependency_ids The args that were originally passed by
  /// reference into the task, but have now been inlined.
  /// \param[in] contained_ids Any ObjectIDs that were newly inlined in the
  /// task spec, because a serialized copy of the ID was contained in one of
  /// the inlined dependencies.
  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override;

  /// Set number of retries to zero for a task that is being canceled.
  ///
  /// \param[in] task_id to cancel.
  /// \return Whether the task was pending and was marked for cancellation.
  bool MarkTaskCanceled(const TaskID &task_id) override;

  /// Return the spec for a pending task.
  absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override;

  /// Return specs for pending children tasks of the given parent task.
  std::vector<TaskID> GetPendingChildrenTasks(const TaskID &parent_task_id) const;

  /// Return whether this task can be submitted for execution.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task can be submitted for execution.
  bool IsTaskSubmissible(const TaskID &task_id) const;

  /// Return whether the task is pending.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is pending.
  bool IsTaskPending(const TaskID &task_id) const;

  /// Return whether the task is scheduled adn waiting for execution.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is waiting for execution.
  bool IsTaskWaitingForExecution(const TaskID &task_id) const;

  /// Return the number of submissible tasks. This includes both tasks that are
  /// pending execution and tasks that have finished but that may be
  /// re-executed to recover from a failure.
  size_t NumSubmissibleTasks() const;

  /// Return the number of pending tasks.
  size_t NumPendingTasks() const;

  int64_t TotalLineageFootprintBytes() const {
    absl::MutexLock lock(&mu_);
    return total_lineage_footprint_bytes_;
  }

  /// Record that the given task's dependencies have been created and the task
  /// can now be scheduled for execution.
  ///
  /// \param[in] task_id The task that is now scheduled.
  void MarkDependenciesResolved(const TaskID &task_id) override;

  /// Record that the given task is scheduled and wait for execution.
  ///
  /// \param[in] task_id The task that is will be running.
  /// \param[in] node_id The node id that this task wil be running.
  /// \param[in] worker_id The worker id that this task wil be running.
  void MarkTaskWaitingForExecution(const TaskID &task_id,
                                   const NodeID &node_id,
                                   const WorkerID &worker_id) override;

  /// Add debug information about the current task status for the ObjectRefs
  /// included in the given stats.
  ///
  /// \param[out] stats Will be populated with objects' current task status, if
  /// any.
  void AddTaskStatusInfo(rpc::CoreWorkerStats *stats) const;

  /// Fill every task information of the current worker to GetCoreWorkerStatsReply.
  void FillTaskInfo(rpc::GetCoreWorkerStatsReply *reply, const int64_t limit) const;

  /// Returns the generator ID that contains the dynamically allocated
  /// ObjectRefs, if the task is dynamic. Else, returns Nil.
  ObjectID TaskGeneratorId(const TaskID &task_id) const;

  /// Record OCL metrics.
  void RecordMetrics();

  /// Update task status change for the task attempt in TaskEventBuffer.
  ///
  /// \param attempt_number Attempt number for the task attempt.
  /// \param spec corresponding TaskSpecification of the task
  /// \param status the changed status.
  /// \param state_update optional task state updates.
  void RecordTaskStatusEvent(
      int32_t attempt_number,
      const TaskSpecification &spec,
      rpc::TaskStatus status,
      bool include_task_info = false,
      absl::optional<const worker::TaskStatusEvent::TaskStateUpdate> state_update =
          absl::nullopt);

 private:
  struct TaskEntry {
    TaskEntry(const TaskSpecification &spec_arg,
              int num_retries_left_arg,
              size_t num_returns,
              TaskStatusCounter &counter,
              int64_t num_oom_retries_left)
        : spec(spec_arg),
          num_retries_left(num_retries_left_arg),
          counter(counter),
          num_oom_retries_left(num_oom_retries_left) {
      for (size_t i = 0; i < num_returns; i++) {
        reconstructable_return_ids.insert(spec.ReturnId(i));
      }
      auto new_status =
          std::make_tuple(spec.GetName(), rpc::TaskStatus::PENDING_ARGS_AVAIL, false);
      counter.Increment(new_status);
      status = new_status;
    }

    void SetStatus(rpc::TaskStatus new_status) {
      auto new_tuple = std::make_tuple(spec.GetName(), new_status, is_retry_);
      counter.Swap(status, new_tuple);
      status = new_tuple;
    }

    void MarkRetryOnFailed() {
      // Record a separate counter increment for retries. This means that if a task
      // is retried N times, we show it as N separate task counts.
      // Note that the increment is for the "previous" task attempt. From now on, this
      // task entry will report metrics or the "current" task attempt.
      counter.Increment({spec.GetName(), rpc::TaskStatus::FAILED, is_retry_});
      is_retry_ = true;
    }

    void MarkRetryOnResubmit() {
      // Record a separate counter increment for resubmits. This means that if a task
      // is resubmitted N times, we show it as N separate task counts.
      // Note that the increment is for the "previous" task attempt. From now on, this
      // task entry will report metrics or the "current" task attempt.
      counter.Increment({spec.GetName(), rpc::TaskStatus::FINISHED, is_retry_});
      is_retry_ = true;
    }

    rpc::TaskStatus GetStatus() const { return std::get<1>(status); }

    // Get the NodeID where the task is executed.
    NodeID GetNodeId() const { return node_id_; }
    // Set the NodeID where the task is executed.
    void SetNodeId(const NodeID &node_id) { node_id_ = node_id; }

    bool IsPending() const { return GetStatus() != rpc::TaskStatus::FINISHED; }

    bool IsWaitingForExecution() const {
      return GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER;
    }

    /// The task spec. This is pinned as long as the following are true:
    /// - The task is still pending execution. This means that the task may
    /// fail and so it may be retried in the future.
    /// - The task finished execution, but it has num_retries_left > 0 and
    /// reconstructable_return_ids is not empty. This means that the task may
    /// be retried in the future to recreate its return objects.
    /// TODO(swang): The TaskSpec protobuf must be copied into the
    /// PushTaskRequest protobuf when sent to a worker so that we can retry it if
    /// the worker fails. We could avoid this by either not caching the full
    /// TaskSpec for tasks that cannot be retried (e.g., actor tasks), or by
    /// storing a shared_ptr to a PushTaskRequest protobuf for all tasks.
    const TaskSpecification spec;
    // Number of times this task may be resubmitted. If this reaches 0, then
    // the task entry may be erased.
    int32_t num_retries_left;
    // Reference to the task stats tracker.
    TaskStatusCounter &counter;
    // Number of times this task may be resubmitted if the task failed
    // due to out of memory failure.
    int32_t num_oom_retries_left;
    // Objects returned by this task that are reconstructable. This is set

    // objects may be reconstructed by resubmitting the task. Once the task
    // finishes its first execution, then the objects that the task returned by
    // value are removed from this set because they can be inlined in any
    // dependent tasks. Objects that the task returned through plasma are
    // reconstructable, so they are only removed from this set once:
    // 1) The language frontend no longer has a reference to the object ID.
    // 2) There are no tasks that depend on the object. This includes both
    //    pending tasks and tasks that finished execution but that may be
    //    retried in the future.
    absl::flat_hash_set<ObjectID> reconstructable_return_ids;
    // The size of this (serialized) task spec in bytes, if the task spec is
    // not pending, i.e. it is being pinned because it's in another object's
    // lineage. We cache this because the task spec protobuf can mutate
    // out-of-band.
    int64_t lineage_footprint_bytes = 0;
    // Number of times this task successfully completed execution so far.
    int num_successful_executions = 0;

   private:
    // The task's current execution and metric status (name, status, is_retry).
    std::tuple<std::string, rpc::TaskStatus, bool> status;
    // The node id where task is executed.
    NodeID node_id_;
    // Whether this is a task retry due to task failure.
    bool is_retry_ = false;
  };

  /// Update nested ref count info and store the in-memory value for a task's
  /// return object. Returns true if the task's return object was returned
  /// directly by value.
  bool HandleTaskReturn(const ObjectID &object_id,
                        const rpc::ReturnObject &return_object,
                        const NodeID &worker_raylet_id,
                        bool store_in_plasma) ABSL_LOCKS_EXCLUDED(mu_);

  /// Remove a lineage reference to this object ID. This should be called
  /// whenever a task that depended on this object ID can no longer be retried.
  ///
  /// \param[in] object_id The object ID whose lineage to delete.
  /// \param[out] ids_to_release If a task was deleted, then these are the
  /// task's arguments whose lineage should also be released.
  /// \param[out] The amount of lineage in bytes that was removed.
  int64_t RemoveLineageReference(const ObjectID &object_id,
                                 std::vector<ObjectID> *ids_to_release)
      ABSL_LOCKS_EXCLUDED(mu_);

  /// Helper function to call RemoveSubmittedTaskReferences on the remaining
  /// dependencies of the given task spec after the task has finished or
  /// failed. The remaining dependencies are plasma objects and any ObjectIDs
  /// that were inlined in the task spec.
  void RemoveFinishedTaskReferences(
      TaskSpecification &spec,
      bool release_lineage,
      const rpc::Address &worker_addr,
      const ReferenceCounter::ReferenceTableProto &borrowed_refs);

  /// Get the objects that were stored in plasma upon the first successful
  /// execution of this task. If the task is re-executed, these objects should
  /// get stored in plasma again, even if they are small and were returned
  /// directly in the worker's reply. This ensures that any reference holders
  /// that are already scheduled at the raylet can retrieve these objects
  /// through plasma.
  ///
  /// \param[in] task_id The task ID.
  /// \param[out] first_execution Whether the task has been successfully
  /// executed before. If this is false, then the objects to store in plasma
  /// will be empty.
  /// \param [out] Return objects that should be stored in plasma. If the
  /// task has been already terminated, it returns an empty set.
  absl::flat_hash_set<ObjectID> GetTaskReturnObjectsToStoreInPlasma(
      const TaskID &task_id, bool *first_execution = nullptr) const
      ABSL_LOCKS_EXCLUDED(mu_);

  /// Shutdown if all tasks are finished and shutdown is scheduled.
  void ShutdownIfNeeded() ABSL_LOCKS_EXCLUDED(mu_);

  /// Set the TaskStatus
  ///
  /// Sets the task status on the TaskEntry, and record the task status change events in
  /// the TaskEventBuffer if enabled.
  ///
  /// \param task_entry corresponding TaskEntry of a task to record the event.
  /// \param status new status.
  /// \param error_info Optional error info for task execution.
  void SetTaskStatus(
      TaskEntry &task_entry,
      rpc::TaskStatus status,
      const absl::optional<const rpc::RayErrorInfo> &error_info = absl::nullopt);

  /// Update the task entry for the task attempt to reflect retry on resubmit.
  ///
  /// This will set the task status, update the attempt number for the task, and increment
  /// the retry counter.
  ///
  /// \param task_entry Task entry for the corresponding task attempt
  void MarkTaskRetryOnResubmit(TaskEntry &task_entry);

  /// Update the task entry for the task attempt to reflect retry on failure.
  ///
  /// This will set the task status, update the attempt number for the task, and increment
  /// the retry counter.
  ///
  /// \param task_entry Task entry for the corresponding task attempt
  void MarkTaskRetryOnFailed(TaskEntry &task_entry, const rpc::RayErrorInfo &error_info);

  /// Mark the stream is ended.
  /// The end of the stream always contains a "sentinel object" passed
  /// via end_of_stream_obj.
  ///
  /// \param generator_id The object ref id of the streaming
  /// generator task.
  /// \param end_of_stream_index The index of the end of the stream.
  /// If -1 is specified, it will mark the current last index as end of stream.
  /// this should be used when a task fails (which means we know the task won't
  /// report any more generator return values).
  void MarkEndOfStream(const ObjectID &generator_id, int64_t end_of_stream_index)
      ABSL_LOCKS_EXCLUDED(objet_ref_stream_ops_mu_) ABSL_LOCKS_EXCLUDED(mu_);

  /// See TemporarilyOwnGeneratorReturnRefIfNeeded for a docstring.
  bool TemporarilyOwnGeneratorReturnRefIfNeededInternal(const ObjectID &object_id,
                                                        const ObjectID &generator_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(objet_ref_stream_ops_mu_) ABSL_LOCKS_EXCLUDED(mu_);

  /// Used to store task results.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Used for reference counting objects.
  /// The task manager is responsible for managing all references related to
  /// submitted tasks (dependencies and return objects).
  std::shared_ptr<ReferenceCounter> reference_counter_;

  /// Mapping from a streaming generator task id -> object ref stream.
  absl::flat_hash_map<ObjectID, ObjectRefStream> object_ref_streams_
      ABSL_GUARDED_BY(objet_ref_stream_ops_mu_);

  /// The consumer side of object ref stream should signal the executor
  /// to resume execution via signal callbacks (i.e., RPC reply).
  /// This data structure maintains the mapping of ObjectRefStreamID -> signal_callbacks
  absl::flat_hash_map<ObjectID, std::vector<ExecutionSignalCallback>>
      ref_stream_execution_signal_callbacks_ ABSL_GUARDED_BY(objet_ref_stream_ops_mu_);

  /// Callback to store objects in plasma. This is used for objects that were
  /// originally stored in plasma. During reconstruction, we ensure that these
  /// objects get stored in plasma again so that any reference holders can
  /// retrieve them.
  const PutInLocalPlasmaCallback put_in_local_plasma_callback_;

  /// Called when a task should be retried.
  const RetryTaskCallback retry_task_callback_;

  // Called to push an error to the relevant driver.
  const PushErrorCallback push_error_callback_;

  const int64_t max_lineage_bytes_;

  // The number of task failures we have logged total.
  int64_t num_failure_logs_ ABSL_GUARDED_BY(mu_) = 0;

  // The last time we logged a task failure.
  int64_t last_log_time_ms_ ABSL_GUARDED_BY(mu_) = 0;

  /// Protects below fields.
  mutable absl::Mutex mu_;

  /// The lock to protect concurrency problems when
  /// using object ref stream APIs
  mutable absl::Mutex objet_ref_stream_ops_mu_;

  /// Tracks per-task-state counters for metric purposes.
  TaskStatusCounter task_counter_ ABSL_GUARDED_BY(mu_);

  /// This map contains one entry per task that may be submitted for
  /// execution. This includes both tasks that are currently pending execution
  /// and tasks that finished execution but that may be retried again in the
  /// future.
  absl::flat_hash_map<TaskID, TaskEntry> submissible_tasks_ ABSL_GUARDED_BY(mu_);

  /// Number of tasks that are pending. This is a count of all tasks in
  /// submissible_tasks_ that have been submitted and are currently pending
  /// execution.
  size_t num_pending_tasks_ = 0;

  int64_t total_lineage_footprint_bytes_ ABSL_GUARDED_BY(mu_) = 0;

  /// Optional shutdown hook to call when pending tasks all finish.
  std::function<void()> shutdown_hook_ ABSL_GUARDED_BY(mu_) = nullptr;

  /// A task state events buffer initialized managed by the CoreWorker.
  /// task_event_buffer_.Enabled() will return false if disabled (due to config or set-up
  /// error).
  worker::TaskEventBuffer &task_event_buffer_;

  friend class TaskManagerTest;
};

}  // namespace core
}  // namespace ray
