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

#include "ray/core_worker/task_manager.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "ray/common/buffer.h"
#include "ray/common/protobuf_utils.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/util/exponential_backoff.h"
#include "ray/util/time.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

// Start throttling task failure logs once we hit this threshold.
constexpr int64_t kTaskFailureThrottlingThreshold = 50;

// Throttle task failure logs to once this interval.
constexpr int64_t kTaskFailureLoggingFrequencyMillis = 5000;

namespace {

rpc::ErrorType MapPlasmaPutStatusToErrorType(const Status &status) {
  // Only the following should be returned from plasma put paths today.
  RAY_DCHECK(status.IsObjectStoreFull() || status.IsTransientObjectStoreFull() ||
             status.IsOutOfDisk() || status.IsIOError())
      << "Unexpected status from plasma put: " << status;

  if (status.IsObjectStoreFull() || status.IsTransientObjectStoreFull()) {
    // TODO(codope): add a dedicated OBJECT_STORE_FULL error type and map to it.
    // https://github.com/ray-project/ray/pull/56070
    return rpc::ErrorType::OUT_OF_MEMORY;
  }
  if (status.IsOutOfDisk()) {
    return rpc::ErrorType::OUT_OF_DISK_ERROR;
  }
  if (status.IsIOError()) {
    // Local IPC failure to plasma/raylet; attribute to local control-plane failure.
    return rpc::ErrorType::LOCAL_RAYLET_DIED;
  }
  return rpc::ErrorType::WORKER_DIED;
}

}  // namespace

absl::flat_hash_set<ObjectID> ObjectRefStream::GetItemsUnconsumed() const {
  absl::flat_hash_set<ObjectID> result;
  for (int64_t index = 0; index <= max_index_seen_; index++) {
    const auto &object_id = GetObjectRefAtIndex(index);
    if (refs_written_to_stream_.find(object_id) == refs_written_to_stream_.end()) {
      continue;
    }

    if (index >= next_index_) {
      result.emplace(object_id);
    }
  }

  if (end_of_stream_index_ != -1) {
    // End of stream index is never consumed by a caller
    // so we should add it here.
    const auto &object_id = GetObjectRefAtIndex(end_of_stream_index_);
    result.emplace(object_id);
  }

  // Temporarily owned refs are not consumed.
  for (const auto &object_id : temporarily_owned_refs_) {
    result.emplace(object_id);
  }
  return result;
}

std::vector<ObjectID> ObjectRefStream::PopUnconsumedItems() {
  // Get all unconsumed refs.
  std::vector<ObjectID> unconsumed_ids;
  for (int64_t index = 0; index <= max_index_seen_; index++) {
    const auto &object_id = GetObjectRefAtIndex(index);
    auto it = refs_written_to_stream_.find(object_id);
    if (it == refs_written_to_stream_.end()) {
      continue;
    }

    if (index >= next_index_) {
      unconsumed_ids.push_back(object_id);
      refs_written_to_stream_.erase(it);
    }
  }

  if (end_of_stream_index_ != -1) {
    // End of stream index is never consumed by a caller
    // so we should add it here.
    const auto &object_id = GetObjectRefAtIndex(end_of_stream_index_);
    unconsumed_ids.push_back(object_id);
  }

  // Temporarily owned refs are not consumed.
  for (const auto &object_id : temporarily_owned_refs_) {
    unconsumed_ids.push_back(object_id);
  }
  temporarily_owned_refs_.clear();

  return unconsumed_ids;
}

bool ObjectRefStream::IsObjectConsumed(int64_t item_index) const {
  return item_index < next_index_;
}

Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
  *object_id_out = GetObjectRefAtIndex(next_index_);
  if (IsFinished()) {
    // next_index_ cannot be bigger than end_of_stream_index_.
    RAY_CHECK(next_index_ == end_of_stream_index_);
    RAY_LOG(DEBUG) << "ObjectRefStream of an id " << generator_id_
                   << " has no more objects.";
    return Status::ObjectRefEndOfStream("");
  }

  auto it = refs_written_to_stream_.find(*object_id_out);
  if (it != refs_written_to_stream_.end()) {
    total_num_object_consumed_ += 1;
    next_index_ += 1;
    RAY_LOG_EVERY_MS(DEBUG, 10000) << "Get the next object id " << *object_id_out
                                   << " generator id: " << generator_id_;
  } else {
    // If the current index hasn't been written, return nothing.
    // The caller is supposed to retry.
    RAY_LOG_EVERY_MS(DEBUG, 10000)
        << "Object not available. Current index: " << next_index_
        << " end_of_stream_index_: " << end_of_stream_index_
        << " generator id: " << generator_id_;
    *object_id_out = ObjectID::Nil();
  }
  return Status::OK();
}

bool ObjectRefStream::IsFinished() const {
  bool is_eof_set = end_of_stream_index_ != -1;
  return is_eof_set && next_index_ >= end_of_stream_index_;
}

std::pair<ObjectID, bool> ObjectRefStream::PeekNextItem() {
  const auto &object_id = GetObjectRefAtIndex(next_index_);
  if (refs_written_to_stream_.find(object_id) == refs_written_to_stream_.end()) {
    return {object_id, false};
  } else {
    return {object_id, true};
  }
}

bool ObjectRefStream::TemporarilyInsertToStreamIfNeeded(const ObjectID &object_id) {
  // Write to a stream if the object ID is not consumed yet.
  if (refs_written_to_stream_.find(object_id) == refs_written_to_stream_.end()) {
    temporarily_owned_refs_.insert(object_id);
    return true;
  }

  return false;
}

bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
  RAY_CHECK_EQ(object_id, GetObjectRefAtIndex(item_index));
  if (end_of_stream_index_ != -1 && item_index >= end_of_stream_index_) {
    RAY_CHECK(next_index_ <= end_of_stream_index_);
    // Ignore the index after the end of the stream index.
    // It can happen if the stream is marked as ended
    // and a new item is written. E.g., Report RPC sent ->
    // worker crashes -> worker crash detected (task failed)
    // -> report RPC received.
    return false;
  }

  if (item_index < next_index_) {
    // Index is already used. Don't write it to the stream.
    return false;
  }

  if (temporarily_owned_refs_.find(object_id) != temporarily_owned_refs_.end()) {
    temporarily_owned_refs_.erase(object_id);
  }

  auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
  if (!inserted) {
    return false;
  }

  max_index_seen_ = std::max(max_index_seen_, item_index);
  total_num_object_written_ += 1;
  return true;
}

void ObjectRefStream::MarkEndOfStream(int64_t item_index,
                                      ObjectID *object_id_in_last_index) {
  if (end_of_stream_index_ != -1) {
    return;
  }
  // ObjectRefStream should guarantee that next_index_ will always have an
  // object value, to avoid hanging the caller the next time it tries to read
  // the stream.
  //
  // NOTE: If the task returns a nondeterministic number of values, the second
  // try may return fewer values than the first try. If the first try fails
  // mid-execution, then on a successful second try, when we mark the end of
  // the stream here, any extra unconsumed returns from the first try will be
  // dropped.
  end_of_stream_index_ = std::max(next_index_, item_index);

  auto end_of_stream_id = GetObjectRefAtIndex(end_of_stream_index_);
  *object_id_in_last_index = end_of_stream_id;
}

ObjectID ObjectRefStream::GetObjectRefAtIndex(int64_t generator_index) const {
  RAY_CHECK_LT(generator_index, RayConfig::instance().max_num_generator_returns());
  // Index 1 is reserved for the first task return from a generator task itself.
  return ObjectID::FromIndex(generator_task_id_, 2 + generator_index);
}

std::vector<rpc::ObjectReference> TaskManager::AddPendingTask(
    const rpc::Address &caller_address,
    const TaskSpecification &spec,
    const std::string &call_site,
    int max_retries) {
  int32_t max_oom_retries =
      (max_retries != 0) ? RayConfig::instance().task_oom_retries() : 0;
  RAY_LOG(DEBUG) << "Adding pending task " << spec.TaskId() << " with " << max_retries
                 << " retries, " << max_oom_retries << " oom retries";

  // Add references for the dependencies to the task.
  std::vector<ObjectID> task_deps;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps.push_back(spec.ArgObjectId(i));
      RAY_LOG(DEBUG) << "Adding arg ID " << spec.ArgObjectId(i);
    } else {
      const auto &inlined_refs = spec.ArgInlinedRefs(i);
      for (const auto &inlined_ref : inlined_refs) {
        const auto inlined_id = ObjectID::FromBinary(inlined_ref.object_id());
        task_deps.push_back(inlined_id);
        RAY_LOG(DEBUG) << "Adding inlined ID " << inlined_id;
      }
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
    task_deps.push_back(actor_creation_return_id);
  }

  // Add new owned objects for the return values of the task.
  size_t num_returns = spec.NumReturns();
  std::vector<rpc::ObjectReference> returned_refs;
  returned_refs.reserve(num_returns);
  std::vector<ObjectID> return_ids;
  return_ids.reserve(num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    auto return_id = spec.ReturnId(i);
    if (!spec.IsActorCreationTask()) {
      bool is_reconstructable = max_retries != 0;
      // We pass an empty vector for inner IDs because we do not know the return
      // value of the task yet. If the task returns an ID(s), the worker will
      // publish the WaitForRefRemoved message that we are now a borrower for
      // the inner IDs. Note that this message can be received *before* the
      // PushTaskReply.
      // NOTE(swang): We increment the local ref count to ensure that the
      // object is considered in scope before we return the ObjectRef to the
      // language frontend. Note that the language bindings should set
      // skip_adding_local_ref=True to avoid double referencing the object.
      reference_counter_.AddOwnedObject(return_id,
                                        /*contained_ids=*/{},
                                        caller_address,
                                        call_site,
                                        -1,
                                        is_reconstructable,
                                        /*add_local_ref=*/true,
                                        /*pinned_at_node_id=*/std::optional<NodeID>(),
                                        /*tensor_transport=*/spec.TensorTransport());
    }

    return_ids.push_back(return_id);
    rpc::ObjectReference ref;
    auto return_object_id = spec.ReturnId(i);
    ref.set_object_id(return_object_id.Binary());
    ref.mutable_owner_address()->CopyFrom(caller_address);
    ref.set_call_site(call_site);
    ref.set_tensor_transport(spec.TensorTransport());

    // Register the callback to free the GPU object when it is out of scope.
    auto tensor_transport = reference_counter_.GetTensorTransport(return_object_id);
    if (tensor_transport.value_or(rpc::TensorTransport::OBJECT_STORE) !=
        rpc::TensorTransport::OBJECT_STORE) {
      reference_counter_.AddObjectOutOfScopeOrFreedCallback(return_object_id,
                                                            free_actor_object_callback_);
    }

    returned_refs.push_back(std::move(ref));
  }

  reference_counter_.UpdateSubmittedTaskReferences(return_ids, task_deps);

  // If it is a generator task, create an object ref stream.
  // The language frontend is responsible for calling DeleteObjectRefStream.
  if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);
    RAY_LOG(DEBUG) << "Create an object ref stream of an id " << generator_id;
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    auto inserted =
        object_ref_streams_.emplace(generator_id, ObjectRefStream(generator_id));
    ref_stream_execution_signal_callbacks_.emplace(
        generator_id, std::vector<ExecutionSignalCallback>());
    RAY_CHECK(inserted.second);
  }

  {
    absl::MutexLock lock(&mu_);
    auto inserted = submissible_tasks_.try_emplace(
        spec.TaskId(), spec, max_retries, num_returns, task_counter_, max_oom_retries);
    RAY_CHECK(inserted.second);
    num_pending_tasks_++;
  }

  RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
      spec.TaskId(),
      spec.JobId(),
      spec.AttemptNumber(),
      spec,
      rpc::TaskStatus::PENDING_ARGS_AVAIL,
      /* include_task_info */ true));

  return returned_refs;
}

std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {
  RAY_CHECK(task_deps->empty());
  TaskSpecification spec;
  bool should_queue_generator_resubmit = false;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      // This can happen when the task has already been
      // retried up to its max attempts.
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    }
    auto &task_entry = it->second;
    if (task_entry.is_canceled_) {
      return rpc::ErrorType::TASK_CANCELLED;
    }

    if (task_entry.spec_.IsStreamingGenerator() &&
        task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
      if (task_entry.num_retries_left_ == 0) {
        // If the last attempt is in progress.
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
      }
      // If the task is a running streaming generator, the object may have been created,
      // deleted, and then needed again for recovery. When the task is finished / failed,
      // ResubmitTask will be called again.
      should_queue_generator_resubmit = true;
    } else if (task_entry.GetStatus() != rpc::TaskStatus::FINISHED &&
               task_entry.GetStatus() != rpc::TaskStatus::FAILED) {
      // Assuming the task retry is already submitted / running.
      return std::nullopt;
    } else {
      // Going to resubmit the task now.
      SetupTaskEntryForResubmit(task_entry);
    }

    spec = task_entry.spec_;
  }

  if (should_queue_generator_resubmit) {
    // Needs to be called outside of the lock to avoid deadlock.
    return queue_generator_resubmit_(spec)
               ? std::nullopt
               : std::make_optional(rpc::ErrorType::TASK_CANCELLED);
  }

  UpdateReferencesForResubmit(spec, task_deps);

  // TODO(can-anyscale): There is a race condition here where a task can still be
  // retried after its retry count has reached zero. Additional information in github
  // issue #54260.
  RAY_LOG(INFO) << "Resubmitting task that produced lost plasma object, attempt #"
                << spec.AttemptNumber() << ": " << spec.DebugString();
  async_retry_task_callback_(spec, /*delay_ms=*/0);

  return std::nullopt;
}

void TaskManager::SetupTaskEntryForResubmit(TaskEntry &task_entry) {
  task_entry.MarkRetry();
  // NOTE(rickyx): We only increment the AttemptNumber on the task spec when
  // `async_retry_task_callback_` is invoked. In order to record the correct status change
  // for the new task attempt, we pass the attempt number explicitly.
  SetTaskStatus(task_entry,
                rpc::TaskStatus::PENDING_ARGS_AVAIL,
                /* state_update */ std::nullopt,
                /* include_task_info */ true,
                task_entry.spec_.AttemptNumber() + 1);
  num_pending_tasks_++;

  // The task is pending again, so it's no longer counted as lineage. If
  // the task finishes and we still need the spec, we'll add the task back
  // to the footprint sum.
  total_lineage_footprint_bytes_ -= task_entry.lineage_footprint_bytes_;
  task_entry.lineage_footprint_bytes_ = 0;

  if (task_entry.num_retries_left_ > 0) {
    task_entry.num_retries_left_--;
  } else {
    RAY_CHECK(task_entry.num_retries_left_ == -1);
  }
}

void TaskManager::UpdateReferencesForResubmit(const TaskSpecification &spec,
                                              std::vector<ObjectID> *task_deps) {
  task_deps->reserve(spec.NumArgs());
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps->emplace_back(spec.ArgObjectId(i));
    } else {
      const auto &inlined_refs = spec.ArgInlinedRefs(i);
      for (const auto &inlined_ref : inlined_refs) {
        task_deps->emplace_back(ObjectID::FromBinary(inlined_ref.object_id()));
      }
    }
  }

  reference_counter_.UpdateResubmittedTaskReferences(*task_deps);

  for (const auto &task_dep : *task_deps) {
    bool was_freed = reference_counter_.TryMarkFreedObjectInUseAgain(task_dep);
    if (was_freed) {
      RAY_LOG(DEBUG) << "Dependency " << task_dep << " of task " << spec.TaskId()
                     << " was freed";
      // We do not keep around copies for objects that were freed, but now that
      // they're needed for recovery, we need to generate and pin a new copy.
      // Delete the old in-memory marker that indicated that the object was
      // freed. Now workers that attempt to get the object will be able to get
      // the reconstructed value.
      in_memory_store_.Delete({task_dep});
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
    reference_counter_.UpdateResubmittedTaskReferences({actor_creation_return_id});
  }
}

void TaskManager::MarkGeneratorFailedAndResubmit(const TaskID &task_id) {
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end());
    auto &task_entry = it->second;

    rpc::RayErrorInfo error_info;
    error_info.set_error_type(
        rpc::ErrorType::GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION);
    SetTaskStatus(task_entry,
                  rpc::TaskStatus::FAILED,
                  worker::TaskStatusEvent::TaskStateUpdate(error_info));

    SetupTaskEntryForResubmit(task_entry);
    spec = task_entry.spec_;
  }

  // Note: Don't need to call UpdateReferencesForResubmit because CompletePendingTask or
  // FailPendingTask are not called when this is. Therefore, RemoveFinishedTaskReferences
  // never happened for this task.
  async_retry_task_callback_(spec, /*delay_ms*/ 0);
}

void TaskManager::DrainAndShutdown(std::function<void()> shutdown) {
  bool has_pending_tasks = false;
  {
    absl::MutexLock lock(&mu_);
    if (num_pending_tasks_ > 0) {
      has_pending_tasks = true;
      RAY_LOG(WARNING)
          << "This worker is still managing " << submissible_tasks_.size()
          << " in flight tasks, waiting for them to finish before shutting down.";
      shutdown_hook_ = std::move(shutdown);
    }
  }

  // Do not hold the lock when calling callbacks.
  if (!has_pending_tasks) {
    shutdown();
  }
}

bool TaskManager::IsTaskSubmissible(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  return submissible_tasks_.contains(task_id);
}

bool TaskManager::IsTaskPending(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  const auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return false;
  }
  return it->second.IsPending();
}

bool TaskManager::IsTaskWaitingForExecution(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  const auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return false;
  }
  return it->second.IsWaitingForExecution();
}

size_t TaskManager::NumSubmissibleTasks() const {
  absl::MutexLock lock(&mu_);
  return submissible_tasks_.size();
}

size_t TaskManager::NumPendingTasks() const {
  absl::MutexLock lock(&mu_);
  return num_pending_tasks_;
}

StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                             const rpc::ReturnObject &return_object,
                                             const NodeID &worker_node_id,
                                             bool store_in_plasma) {
  bool direct_return = false;
  reference_counter_.UpdateObjectSize(object_id, return_object.size());
  RAY_LOG(DEBUG) << "Task return object " << object_id << " has size "
                 << return_object.size();
  const auto nested_refs =
      VectorFromProtobuf<rpc::ObjectReference>(return_object.nested_inlined_refs());

  if (return_object.in_plasma()) {
    // NOTE(swang): We need to add the location of the object before marking
    // it as local in the in-memory store so that the data locality policy
    // will choose the right raylet for any queued dependent tasks.
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    // Mark it as in plasma with a dummy object.
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
  } else {
    // NOTE(swang): If a direct object was promoted to plasma, then we do not
    // record the node ID that it was pinned at, which means that we will not
    // be able to reconstruct it if the plasma object copy is lost. However,
    // this is okay because the pinned copy is on the local node, so we will
    // fate-share with the object if the local node fails.
    std::shared_ptr<LocalMemoryBuffer> data_buffer;
    if (!return_object.data().empty()) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(
              reinterpret_cast<const uint8_t *>(return_object.data().data())),
          return_object.data().size());
    }
    std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
    if (!return_object.metadata().empty()) {
      metadata_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(
              reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
          return_object.metadata().size());
    }

    auto tensor_transport = reference_counter_.GetTensorTransport(object_id);
    RayObject object(data_buffer,
                     metadata_buffer,
                     nested_refs,
                     /*copy_data=*/false,
                     tensor_transport.value_or(rpc::TensorTransport::OBJECT_STORE));
    if (store_in_plasma) {
      Status s = put_in_local_plasma_callback_(object, object_id);
      if (!s.ok()) {
        return s;
      }
    } else {
      in_memory_store_.Put(object, object_id, reference_counter_.HasReference(object_id));
      direct_return = true;
    }
  }

  rpc::Address owner_address;
  if (reference_counter_.GetOwner(object_id, &owner_address) && !nested_refs.empty()) {
    std::vector<ObjectID> nested_ids;
    nested_ids.reserve(nested_refs.size());
    for (const auto &nested_ref : nested_refs) {
      nested_ids.emplace_back(ObjectRefToId(nested_ref));
    }
    reference_counter_.AddNestedObjectIds(object_id, nested_ids, owner_address);
  }
  return direct_return;
}

bool TaskManager::TryDelObjectRefStream(const ObjectID &generator_id) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  bool can_gc_lineage = TryDelObjectRefStreamInternal(generator_id);
  if (!can_gc_lineage) {
    RAY_LOG(DEBUG) << "Generator " << generator_id
                   << " still has lineage in scope, try again later";
    return false;
  }

  RAY_LOG(DEBUG) << "Deleting object ref stream of an id " << generator_id;
  object_ref_streams_.erase(generator_id);
  return true;
}

Status TaskManager::TryReadObjectRefStream(const ObjectID &generator_id,
                                           ObjectID *object_id_out) {
  auto backpressure_threshold = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(generator_id.TaskId());
    if (it != submissible_tasks_.end()) {
      backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
    }
  }

  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  RAY_CHECK(object_id_out != nullptr);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end())
      << "TryReadObjectRefStream API can be used only when the stream has been "
         "created "
         "and not removed.";
  auto status = stream_it->second.TryReadNextItem(object_id_out);

  /// If you could read the next item, signal the executor to resume
  /// if necessary.
  if (status.ok()) {
    auto total_generated = stream_it->second.TotalNumObjectWritten();
    auto total_consumed = stream_it->second.TotalNumObjectConsumed();
    auto total_unconsumed = total_generated - total_consumed;
    if (backpressure_threshold != -1 && total_unconsumed < backpressure_threshold) {
      auto it = ref_stream_execution_signal_callbacks_.find(generator_id);
      if (it != ref_stream_execution_signal_callbacks_.end()) {
        for (const auto &execution_signal : it->second) {
          RAY_LOG(DEBUG) << "The task for a stream " << generator_id
                         << " should resume. total_generated: " << total_generated
                         << ". total_consumed: " << total_consumed
                         << ". threshold: " << backpressure_threshold;
          execution_signal(Status::OK(), total_consumed);
        }
        it->second.clear();
      }
    }
  }

  return status;
}

bool TaskManager::StreamingGeneratorIsFinished(const ObjectID &generator_id) const {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end())
      << "IsFinished API can be used only when the stream has been "
         "created "
         "and not removed.";
  return stream_it->second.IsFinished();
}

bool TaskManager::TryDelObjectRefStreamInternal(const ObjectID &generator_id) {
  // Call execution signal callbacks to ensure that the executor does not block
  // after the generator goes out of scope at the caller.
  auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
  if (signal_it != ref_stream_execution_signal_callbacks_.end()) {
    RAY_LOG(DEBUG) << "Deleting execution signal callbacks for generator "
                   << generator_id;
    for (const auto &execution_signal : signal_it->second) {
      execution_signal(Status::NotFound("Stream is deleted."), -1);
    }
    // We may still receive more generator return reports in the future, if the
    // generator task is still running or is retried. They will get the
    // callback immediately because we deleted this entry.
    ref_stream_execution_signal_callbacks_.erase(signal_it);
  }

  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    return true;
  }

  // Remove any unconsumed refs from the stream metadata in-memory store.
  auto unconsumed_ids = stream_it->second.PopUnconsumedItems();
  std::vector<ObjectID> deleted;
  reference_counter_.TryReleaseLocalRefs(unconsumed_ids, &deleted);
  in_memory_store_.Delete(deleted);

  int64_t num_objects_generated = stream_it->second.EofIndex();
  if (num_objects_generated == -1) {
    RAY_LOG(DEBUG) << "Skip streaming generator deletion, EOF not written yet";
    // Generator task has not finished yet. Wait for EoF to be marked before
    // deleting.
    return false;
  }

  bool can_gc_lineage = reference_counter_.CheckGeneratorRefsLineageOutOfScope(
      generator_id, num_objects_generated);
  return can_gc_lineage;
}

std::pair<ObjectID, bool> TaskManager::PeekObjectRefStream(const ObjectID &generator_id) {
  ObjectID next_object_id;
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end())
      << "PeekObjectRefStream API can be used only when the stream has been "
         "created and not removed.";
  const auto &result = stream_it->second.PeekNextItem();

  // Temporarily own the ref since the corresponding reference is probably
  // not reported yet.
  TemporarilyOwnGeneratorReturnRefIfNeededInternal(result.first /*=object_id*/,
                                                   generator_id);
  return result;
}

bool TaskManager::ObjectRefStreamExists(const ObjectID &generator_id) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto it = object_ref_streams_.find(generator_id);
  return it != object_ref_streams_.end();
}

void TaskManager::MarkEndOfStream(const ObjectID &generator_id,
                                  int64_t end_of_stream_index) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  ObjectID last_object_id;

  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    // Stream has been already deleted. Do not handle it.
    return;
  }

  stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);
  if (!last_object_id.IsNil()) {
    RAY_LOG(DEBUG) << "Write EoF to the object ref stream. Index: "
                   << stream_it->second.EofIndex()
                   << ". Last object id: " << last_object_id;

    reference_counter_.OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);
    RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
    // Put a dummy object at the end of the stream. We don't need to check if
    // the object should be stored in plasma because the end of the stream is a
    // fake ObjectRef that should never be read by the application.
    in_memory_store_.Put(
        error, last_object_id, reference_counter_.HasReference(last_object_id));
  }
}

bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request,
    const ExecutionSignalCallback &execution_signal_callback) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  const auto &task_id = generator_id.TaskId();
  int64_t item_index = request.item_index();
  int64_t attempt_number = request.attempt_number();
  // Every generated object has the same task id.
  RAY_LOG(DEBUG) << "Received an intermediate result of index " << item_index
                 << " generator_id: " << generator_id;
  auto backpressure_threshold = -1;

  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
      backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
      if (it->second.spec_.AttemptNumber() > attempt_number) {
        // Generator task reports can arrive at any time. If the first attempt
        // fails, we may receive a report from the first executor after the
        // second attempt has started. In this case, we should ignore the first
        // attempt.
        execution_signal_callback(
            Status::NotFound("Stale object reports from the previous attempt."), -1);
        return false;
      }
    }
  }

  // NOTE: If it is the first execution (e.g., CompletePendingTask has never been called),
  // it is always empty.
  const auto store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id);

  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    // Stream has been already deleted. Do not handle it.
    execution_signal_callback(Status::NotFound("Stream is already deleted"), -1);
    return false;
  }
  size_t num_objects_written = 0;

  if (request.has_returned_object()) {
    const rpc::ReturnObject &returned_object = request.returned_object();
    const auto object_id = ObjectID::FromBinary(returned_object.object_id());

    RAY_LOG(DEBUG) << "Write an object " << object_id
                   << " to the object ref stream of id " << generator_id;
    auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);

    // If the ref was written to a stream, we should also
    // own the dynamically generated task return.
    // NOTE: If we call this method while holding a lock, it can deadlock.
    if (index_not_used_yet) {
      reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
      num_objects_written += 1;
    }
    // When an object is reported, the object is ready to be fetched.
    reference_counter_.UpdateObjectPendingCreation(object_id, false);
    StatusOr<bool> put_res =
        HandleTaskReturn(object_id,
                         returned_object,
                         NodeID::FromBinary(request.worker_addr().node_id()),
                         /*store_in_plasma=*/store_in_plasma_ids.contains(object_id));
    if (!put_res.ok()) {
      RAY_LOG(WARNING).WithField(object_id)
          << "Failed to handle streaming dynamic return: " << put_res.status();
    }
  }

  // Handle backpressure if needed.
  auto total_generated = stream_it->second.TotalNumObjectWritten();
  auto total_consumed = stream_it->second.TotalNumObjectConsumed();

  if (stream_it->second.IsObjectConsumed(item_index)) {
    execution_signal_callback(Status::OK(), total_consumed);
    return false;
  }

  // Otherwise, follow the regular backpressure logic.
  // NOTE, here we check `item_index - last_consumed_index >= backpressure_threshold`,
  // instead of the number of unconsumed items, because we may receive the
  // `HandleReportGeneratorItemReturns` requests out of order.
  if (backpressure_threshold != -1 &&
      (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    RAY_LOG(DEBUG) << "Stream " << generator_id
                   << " is backpressured. total_generated: " << total_generated
                   << ". total_consumed: " << total_consumed
                   << ". threshold: " << backpressure_threshold;
    auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
    if (signal_it == ref_stream_execution_signal_callbacks_.end()) {
      execution_signal_callback(Status::NotFound("Stream is deleted."), -1);
    } else {
      signal_it->second.push_back(execution_signal_callback);
    }
  } else {
    // No need to backpressure.
    execution_signal_callback(Status::OK(), total_consumed);
  }
  return num_objects_written != 0;
}

bool TaskManager::TemporarilyOwnGeneratorReturnRefIfNeeded(const ObjectID &object_id,
                                                           const ObjectID &generator_id) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  return TemporarilyOwnGeneratorReturnRefIfNeededInternal(object_id, generator_id);
}

bool TaskManager::TemporarilyOwnGeneratorReturnRefIfNeededInternal(
    const ObjectID &object_id, const ObjectID &generator_id) {
  bool inserted_to_stream = false;
  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    return false;
  }

  auto &stream = stream_it->second;
  inserted_to_stream = stream.TemporarilyInsertToStreamIfNeeded(object_id);

  // We shouldn't hold a lock when calling reference counter API.
  if (inserted_to_stream) {
    RAY_LOG(DEBUG) << "Added streaming ref " << object_id;
    reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
    return true;
  }

  return false;
}

void TaskManager::CompletePendingTask(const TaskID &task_id,
                                      const rpc::PushTaskReply &reply,
                                      const rpc::Address &worker_addr,
                                      bool is_application_error) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;

  bool first_execution = false;
  const auto store_in_plasma_ids =
      GetTaskReturnObjectsToStoreInPlasma(task_id, &first_execution);
  std::vector<ObjectID> dynamic_return_ids;
  std::vector<ObjectID> dynamic_returns_in_plasma;
  std::vector<ObjectID> direct_return_ids;
  if (reply.dynamic_return_objects_size() > 0) {
    RAY_CHECK(reply.return_objects_size() == 1)
        << "Dynamic generators only supported for num_returns=1";
    const auto generator_id = ObjectID::FromBinary(reply.return_objects(0).object_id());
    for (const auto &return_object : reply.dynamic_return_objects()) {
      const auto object_id = ObjectID::FromBinary(return_object.object_id());
      if (first_execution) {
        reference_counter_.AddDynamicReturn(object_id, generator_id);
        dynamic_return_ids.push_back(object_id);
      }
      StatusOr<bool> direct_or =
          HandleTaskReturn(object_id,
                           return_object,
                           NodeID::FromBinary(worker_addr.node_id()),
                           store_in_plasma_ids.contains(object_id));
      if (!direct_or.ok()) {
        RAY_LOG(WARNING).WithField(object_id)
            << "Failed to handle dynamic task return: " << direct_or.status();
        Status st = direct_or.status();
        rpc::ErrorType err_type = MapPlasmaPutStatusToErrorType(st);
        rpc::RayErrorInfo err_info;
        err_info.set_error_message(st.ToString());
        FailOrRetryPendingTask(task_id,
                               err_type,
                               &st,
                               /*ray_error_info=*/&err_info,
                               /*mark_task_object_failed=*/true,
                               /*fail_immediately=*/true);
        return;
      } else if (!direct_or.value() && first_execution) {
        dynamic_returns_in_plasma.push_back(object_id);
      }
    }
  }

  for (const auto &return_object : reply.return_objects()) {
    const auto object_id = ObjectID::FromBinary(return_object.object_id());
    StatusOr<bool> direct_or = HandleTaskReturn(object_id,
                                                return_object,
                                                NodeID::FromBinary(worker_addr.node_id()),
                                                store_in_plasma_ids.contains(object_id));
    if (!direct_or.ok()) {
      RAY_LOG(WARNING).WithField(object_id)
          << "Failed to handle task return: " << direct_or.status();
      // If storing return in plasma failed, treat as system failure for this attempt.
      // Do not proceed with normal completion. Mark task failed immediately.
      Status st = direct_or.status();
      rpc::ErrorType err_type = MapPlasmaPutStatusToErrorType(st);
      rpc::RayErrorInfo err_info;
      err_info.set_error_message(st.ToString());
      FailOrRetryPendingTask(task_id,
                             err_type,
                             &st,
                             /*ray_error_info=*/&err_info,
                             /*mark_task_object_failed=*/true,
                             /*fail_immediately=*/true);
      return;
    } else if (direct_or.value()) {
      direct_return_ids.push_back(object_id);
    }
  }

  TaskSpecification spec;
  bool release_lineage = true;
  int64_t min_lineage_bytes_to_evict = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.spec_;

    // Record any dynamically returned objects. We need to store these with the
    // task spec so that the worker will recreate them if the task gets
    // re-executed.
    // TODO(sang): Remove this logic once streaming generator is the default.
    if (first_execution) {
      for (const auto &dynamic_return_id : dynamic_return_ids) {
        RAY_LOG(DEBUG) << "Task " << task_id << " produced dynamic return object "
                       << dynamic_return_id;
        spec.AddDynamicReturnId(dynamic_return_id);
      }
      for (const auto &dynamic_return_id : dynamic_returns_in_plasma) {
        it->second.reconstructable_return_ids_.insert(dynamic_return_id);
      }

      if (spec.IsStreamingGenerator()) {
        // Upon the first complete execution, set the number of streaming
        // generator returns.
        auto num_streaming_generator_returns =
            reply.streaming_generator_return_ids_size();
        if (num_streaming_generator_returns > 0) {
          spec.SetNumStreamingGeneratorReturns(num_streaming_generator_returns);
          RAY_LOG(DEBUG) << "Completed streaming generator task " << spec.TaskId()
                         << " has " << spec.NumStreamingGeneratorReturns()
                         << " return objects.";
          for (const auto &return_id_info : reply.streaming_generator_return_ids()) {
            if (return_id_info.is_plasma_object()) {
              // TODO(swang): It is possible that the dynamically returned refs
              // have already been consumed by the caller and deleted. This can
              // cause a memory leak of the task metadata, because we will
              // never receive a callback from the ReferenceCounter to erase
              // the task.
              it->second.reconstructable_return_ids_.insert(
                  ObjectID::FromBinary(return_id_info.object_id()));
            }
          }
        }
      }
    }

    // Release the lineage for any non-plasma return objects.
    // TODO(sang): Remove this logic once streaming generator is the default.
    for (const auto &direct_return_id : direct_return_ids) {
      RAY_LOG(DEBUG) << "Task " << it->first << " returned direct object "
                     << direct_return_id << ", now has "
                     << it->second.reconstructable_return_ids_.size()
                     << " plasma returns in scope";
      it->second.reconstructable_return_ids_.erase(direct_return_id);
    }
    RAY_LOG(DEBUG) << "Task " << it->first << " now has "
                   << it->second.reconstructable_return_ids_.size()
                   << " plasma returns in scope";
    it->second.num_successful_executions_++;

    if (is_application_error) {
      SetTaskStatus(
          it->second,
          rpc::TaskStatus::FAILED,
          worker::TaskStatusEvent::TaskStateUpdate(gcs::GetRayErrorInfo(
              rpc::ErrorType::TASK_EXECUTION_EXCEPTION, reply.task_execution_error())));
    } else {
      SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
    }
    num_pending_tasks_--;

    // A finished task can only be re-executed if it has some number of
    // retries left and returned at least one object that is still in use and
    // stored in plasma.
    bool task_retryable = it->second.num_retries_left_ != 0 &&
                          !it->second.reconstructable_return_ids_.empty();
    if (task_retryable) {
      // Pin the task spec if it may be retried again.
      release_lineage = false;
      it->second.lineage_footprint_bytes_ = it->second.spec_.GetMessage().ByteSizeLong();
      total_lineage_footprint_bytes_ += it->second.lineage_footprint_bytes_;
      if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {
        RAY_LOG(INFO) << "Total lineage size is " << total_lineage_footprint_bytes_ / 1e6
                      << "MB, which exceeds the limit of " << max_lineage_bytes_ / 1e6
                      << "MB";
        min_lineage_bytes_to_evict =
            total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
      }
    } else {
      submissible_tasks_.erase(it);
    }
  }

  // If it is a streaming generator, mark the end of stream since the task is finished.
  // We handle this logic here because the lock shouldn't be held while calling
  // HandleTaskReturn.
  if (spec.IsStreamingGenerator()) {
    const auto generator_id = ObjectID::FromBinary(reply.return_objects(0).object_id());
    if (first_execution) {
      ObjectID last_ref_in_stream;
      MarkEndOfStream(generator_id, reply.streaming_generator_return_ids_size());
    } else {
      // The end of the stream should already have been marked on the first
      // successful execution.
      if (is_application_error) {
        // It means the task was re-executed but failed with an application
        // error. In this case, we should fail the rest of known streaming
        // generator returns with the same error.
        RAY_LOG(DEBUG) << "Streaming generator task " << spec.TaskId()
                       << " failed with application error, failing "
                       << spec.NumStreamingGeneratorReturns() << " return objects.";
        RAY_CHECK_EQ(reply.return_objects_size(), 1);
        for (size_t i = 0; i < spec.NumStreamingGeneratorReturns(); i++) {
          const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
          RAY_CHECK_EQ(reply.return_objects_size(), 1);
          const auto &return_object = reply.return_objects(0);
          StatusOr<bool> res =
              HandleTaskReturn(generator_return_id,
                               return_object,
                               NodeID::FromBinary(worker_addr.node_id()),
                               store_in_plasma_ids.contains(generator_return_id));
          if (!res.ok()) {
            RAY_LOG(WARNING).WithField(generator_return_id)
                << "Failed to handle generator return during app error propagation: "
                << res.status();
            Status st = res.status();
            rpc::ErrorType err_type = MapPlasmaPutStatusToErrorType(st);
            rpc::RayErrorInfo err_info;
            err_info.set_error_message(st.ToString());
            FailOrRetryPendingTask(spec.TaskId(),
                                   err_type,
                                   &st,
                                   /*ray_error_info=*/&err_info,
                                   /*mark_task_object_failed=*/true,
                                   /*fail_immediately=*/true);
            return;
          }
        }
      }
    }
  }

  RemoveFinishedTaskReferences(spec, release_lineage, worker_addr, reply.borrowed_refs());
  if (min_lineage_bytes_to_evict > 0) {
    // Evict at least half of the current lineage.
    auto bytes_evicted = reference_counter_.EvictLineage(min_lineage_bytes_to_evict);
    RAY_LOG(INFO) << "Evicted " << bytes_evicted / 1e6 << "MB of task lineage.";
  }

  ShutdownIfNeeded();
}

bool TaskManager::RetryTaskIfPossible(const TaskID &task_id,
                                      const rpc::RayErrorInfo &error_info) {
  TaskSpecification spec;
  bool will_retry = false;
  int32_t num_retries_left = 0;
  int32_t num_oom_retries_left = 0;
  bool task_failed_due_to_oom = error_info.error_type() == rpc::ErrorType::OUT_OF_MEMORY;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to retry task that was not pending " << task_id;
    auto &task_entry = it->second;
    RAY_CHECK(task_entry.IsPending())
        << "Tried to retry task that was not pending " << task_id;
    spec = task_entry.spec_;
    num_retries_left = task_entry.num_retries_left_;
    num_oom_retries_left = task_entry.num_oom_retries_left_;
    if (task_failed_due_to_oom) {
      if (num_oom_retries_left > 0) {
        will_retry = true;
        task_entry.num_oom_retries_left_--;
      } else if (num_oom_retries_left == -1) {
        will_retry = true;
      } else {
        RAY_CHECK(num_oom_retries_left == 0);
      }
    } else {
      auto is_preempted = false;
      if (error_info.error_type() == rpc::ErrorType::NODE_DIED) {
        const auto node_info =
            gcs_client_->Nodes().GetNodeAddressAndLiveness(task_entry.GetNodeId(),
                                                           /*filter_dead_nodes=*/false);
        is_preempted = node_info && node_info->has_death_info() &&
                       node_info->death_info().reason() ==
                           rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED;
      }
      if (num_retries_left > 0 || (is_preempted && task_entry.spec_.IsRetriable())) {
        will_retry = true;
        if (is_preempted) {
          RAY_LOG(INFO) << "Task " << task_id << " failed due to node preemption on node "
                        << task_entry.GetNodeId() << ", not counting against retries";
        } else {
          task_entry.num_retries_left_--;
        }
      } else if (num_retries_left == -1) {
        will_retry = true;
      } else {
        RAY_CHECK(num_retries_left == 0);
      }
    }
    // Keep `num_retries_left` and `num_oom_retries_left` up to date
    num_retries_left = task_entry.num_retries_left_;
    num_oom_retries_left = task_entry.num_oom_retries_left_;

    if (will_retry) {
      // Record the old attempt status as FAILED.
      SetTaskStatus(task_entry,
                    rpc::TaskStatus::FAILED,
                    worker::TaskStatusEvent::TaskStateUpdate(error_info));
      task_entry.MarkRetry();
      // Push the error to the driver if the task will still retry.
      bool enable_output_error_log_if_still_retry =
          RayConfig::instance().enable_output_error_log_if_still_retry();
      if (enable_output_error_log_if_still_retry) {
        std::string num_retries_left_str;
        if (task_failed_due_to_oom) {
          num_retries_left_str = num_oom_retries_left == -1
                                     ? "infinite"
                                     : std::to_string(num_oom_retries_left);
        } else {
          num_retries_left_str =
              num_retries_left == -1 ? "infinite" : std::to_string(num_retries_left);
        }
        auto error_message = "Task " + spec.FunctionDescriptor()->CallString() +
                             " failed. There are " + num_retries_left_str +
                             " retries remaining, so the task will be retried. Error: " +
                             error_info.error_message();
        Status push_error_status =
            push_error_callback_(task_entry.spec_.JobId(),
                                 rpc::ErrorType_Name(error_info.error_type()),
                                 error_message,
                                 current_time_ms());
        if (!push_error_status.ok()) {
          RAY_LOG(ERROR) << "Failed to push error to driver for task " << spec.TaskId();
        }
      }
      // Mark the new status and also include task spec info for the new attempt.
      SetTaskStatus(task_entry,
                    rpc::TaskStatus::PENDING_ARGS_AVAIL,
                    /* state_update */ std::nullopt,
                    /* include_task_info */ true,
                    task_entry.spec_.AttemptNumber() + 1);
    }
  }

  // We should not hold the lock during these calls because they may trigger
  // callbacks in this or other classes.
  std::string num_retries_left_str =
      num_retries_left == -1 ? "infinite" : std::to_string(num_retries_left);
  RAY_LOG(INFO) << "task " << spec.TaskId() << " retries left: " << num_retries_left_str
                << ", oom retries left: " << num_oom_retries_left
                << ", task failed due to oom: " << task_failed_due_to_oom;
  if (will_retry) {
    RAY_LOG(INFO) << "Attempting to resubmit task " << spec.TaskId()
                  << " for attempt number: " << spec.AttemptNumber();
    int32_t delay_ms = task_failed_due_to_oom
                           ? ExponentialBackoff::GetBackoffMs(
                                 spec.AttemptNumber(),
                                 RayConfig::instance().task_oom_retry_delay_base_ms())
                           : RayConfig::instance().task_retry_delay_ms();
    async_retry_task_callback_(spec, delay_ms);
    return true;
  } else {
    RAY_LOG(INFO) << "No retries left for task " << spec.TaskId()
                  << ", not going to resubmit.";
    return false;
  }
}

void TaskManager::FailPendingTask(const TaskID &task_id,
                                  rpc::ErrorType error_type,
                                  const Status *status,
                                  const rpc::RayErrorInfo *ray_error_info) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(DEBUG) << "Task " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type) << ", ray_error_info: "
                 << ((ray_error_info == nullptr) ? "nullptr"
                                                 : ray_error_info->DebugString());

  TaskSpecification spec;
  // Check whether the error should be stored in plasma or not.
  bool first_execution = false;
  const auto store_in_plasma_ids =
      GetTaskReturnObjectsToStoreInPlasma(task_id, &first_execution);
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      // Failing a pending task can happen through the normal task lifecycle or task
      // cancellation. Since task cancellation runs concurrently with the normal task
      // lifecycle, we do expect this state. It is safe to assume the task
      // has been failed correctly by either the normal task lifecycle or task
      // cancellation, and we can skip failing it again.
      RAY_LOG(INFO).WithField("task_id", task_id)
          << "Task is no longer in the submissible tasks map. It has either completed or "
             "been cancelled. Skip failing";
      return;
    }
    RAY_CHECK(it->second.IsPending())
        << "Tried to fail task that was not pending " << task_id;
    spec = it->second.spec_;
    if (it->second.is_canceled_ && error_type != rpc::ErrorType::TASK_CANCELLED) {
      // If the task is marked as cancelled before reaching FailPendingTask (which is
      // essentially the final state of the task lifecycle), that failure reason takes
      // precedence.
      error_type = rpc::ErrorType::TASK_CANCELLED;
      ray_error_info = nullptr;
    }

    if (status != nullptr && status->IsIntentionalSystemExit()) {
      // We don't mark intentional system exit as failures, such as tasks that
      // exit by exit_actor(), exit by ray.shutdown(), etc. These tasks are expected
      // to exit and not be marked as failure.
      SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
    } else {
      const auto error_info =
          (ray_error_info == nullptr
               ? gcs::GetRayErrorInfo(error_type,
                                      (status != nullptr ? status->ToString() : ""))
               : *ray_error_info);
      SetTaskStatus(it->second,
                    rpc::TaskStatus::FAILED,
                    worker::TaskStatusEvent::TaskStateUpdate(error_info));
    }
    submissible_tasks_.erase(it);
    num_pending_tasks_--;

    // Throttled logging of task failure errors.
    auto debug_str = spec.DebugString();
    if (!absl::StrContains(debug_str, "__ray_terminate__") &&
        (num_failure_logs_ < kTaskFailureThrottlingThreshold ||
         (current_time_ms() - last_log_time_ms_) > kTaskFailureLoggingFrequencyMillis)) {
      if (num_failure_logs_++ == kTaskFailureThrottlingThreshold) {
        RAY_LOG(WARNING) << "Too many failure logs, throttling to once every "
                         << kTaskFailureLoggingFrequencyMillis << " millis.";
      }
      last_log_time_ms_ = current_time_ms();
      if (status != nullptr) {
        RAY_LOG(INFO) << "Task failed: " << *status << ": " << spec.DebugString();
      } else {
        RAY_LOG(INFO) << "Task failed: " << spec.DebugString();
      }
      RAY_LOG(DEBUG) << "Runtime env for task " << spec.TaskId() << " is "
                     << spec.RuntimeEnvDebugString();
    }
  }

  // The worker failed to execute the task, so it cannot be borrowing any
  // objects.
  RemoveFinishedTaskReferences(spec,
                               /*release_lineage=*/true,
                               rpc::Address(),
                               ReferenceCounterInterface::ReferenceTableProto());

  MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info, store_in_plasma_ids);

  ShutdownIfNeeded();
}

bool TaskManager::FailOrRetryPendingTask(const TaskID &task_id,
                                         rpc::ErrorType error_type,
                                         const Status *status,
                                         const rpc::RayErrorInfo *ray_error_info,
                                         bool mark_task_object_failed,
                                         bool fail_immediately) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(WARNING) << "Task attempt " << task_id << " failed with error "
                   << rpc::ErrorType_Name(error_type) << " Fail immediately? "
                   << fail_immediately << ", status "
                   << (status == nullptr ? "null" : status->ToString()) << ", error info "
                   << (ray_error_info == nullptr ? "null"
                                                 : ray_error_info->DebugString());

  bool will_retry = false;
  if (!fail_immediately) {
    will_retry = RetryTaskIfPossible(
        task_id,
        ray_error_info == nullptr ? gcs::GetRayErrorInfo(error_type) : *ray_error_info);
  }

  if (!will_retry && mark_task_object_failed) {
    FailPendingTask(task_id, error_type, status, ray_error_info);
  }

  ShutdownIfNeeded();

  return will_retry;
}

void TaskManager::ShutdownIfNeeded() {
  std::function<void()> shutdown_hook = nullptr;
  {
    absl::MutexLock lock(&mu_);
    if (shutdown_hook_ && num_pending_tasks_ == 0) {
      RAY_LOG(WARNING) << "All in flight tasks finished, worker will shut down after "
                          "draining references.";
      std::swap(shutdown_hook_, shutdown_hook);
    }
  }
  // Do not hold the lock when calling callbacks.
  if (shutdown_hook != nullptr) {
    shutdown_hook();
  }
}

void TaskManager::OnTaskDependenciesInlined(
    const std::vector<ObjectID> &inlined_dependency_ids,
    const std::vector<ObjectID> &contained_ids) {
  std::vector<ObjectID> deleted;
  reference_counter_.UpdateSubmittedTaskReferences(
      /*return_ids=*/{},
      /*argument_ids_to_add=*/contained_ids,
      /*argument_ids_to_remove=*/inlined_dependency_ids,
      &deleted);
  in_memory_store_.Delete(deleted);
}

void TaskManager::RemoveFinishedTaskReferences(
    TaskSpecification &spec,
    bool release_lineage,
    const rpc::Address &borrower_addr,
    const ReferenceCounterInterface::ReferenceTableProto &borrowed_refs) {
  std::vector<ObjectID> plasma_dependencies = ExtractPlasmaDependencies(spec);

  std::vector<ObjectID> return_ids;
  size_t num_returns = spec.NumReturns();
  return_ids.reserve(num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    return_ids.push_back(spec.ReturnId(i));
  }
  // TODO(sang): Remove it once the streaming generator is turned on
  // by default.
  if (spec.ReturnsDynamic()) {
    for (const auto &dynamic_return_id : spec.DynamicReturnIds()) {
      return_ids.push_back(dynamic_return_id);
    }
  }
  if (spec.IsStreamingGenerator()) {
    for (size_t i = 0; i < spec.NumStreamingGeneratorReturns(); i++) {
      return_ids.push_back(spec.StreamingGeneratorReturnId(i));
    }
  }

  std::vector<ObjectID> deleted;
  reference_counter_.UpdateFinishedTaskReferences(return_ids,
                                                  plasma_dependencies,
                                                  release_lineage,
                                                  borrower_addr,
                                                  borrowed_refs,
                                                  &deleted);
  in_memory_store_.Delete(deleted);
}

int64_t TaskManager::RemoveLineageReference(const ObjectID &object_id,
                                            std::vector<ObjectID> *released_objects) {
  absl::MutexLock lock(&mu_);
  const int64_t total_lineage_footprint_bytes_prev(total_lineage_footprint_bytes_);

  const TaskID &task_id = object_id.TaskId();
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    RAY_LOG(DEBUG) << "No lineage for object " << object_id;
    return 0;
  }

  RAY_LOG(DEBUG) << "Plasma object " << object_id << " out of scope";
  for (const auto &plasma_id : it->second.reconstructable_return_ids_) {
    RAY_LOG(DEBUG) << "Task " << task_id << " has " << plasma_id << " in scope";
  }
  it->second.reconstructable_return_ids_.erase(object_id);
  RAY_LOG(DEBUG) << "Task " << task_id << " now has "
                 << it->second.reconstructable_return_ids_.size()
                 << " plasma returns in scope";

  if (it->second.reconstructable_return_ids_.empty() && !it->second.IsPending()) {
    // If the task can no longer be retried, decrement the lineage ref count
    // for each of the task's args.
    for (size_t i = 0; i < it->second.spec_.NumArgs(); i++) {
      if (it->second.spec_.ArgByRef(i)) {
        released_objects->push_back(it->second.spec_.ArgObjectId(i));
      } else {
        const auto &inlined_refs = it->second.spec_.ArgInlinedRefs(i);
        for (const auto &inlined_ref : inlined_refs) {
          released_objects->push_back(ObjectID::FromBinary(inlined_ref.object_id()));
        }
      }
    }

    if (it->second.spec_.IsActorTask()) {
      // We need to decrement the actor lineage ref count here
      // since it's incremented during TaskManager::AddPendingTask.
      const auto actor_creation_return_id = it->second.spec_.ActorCreationDummyObjectId();
      released_objects->push_back(actor_creation_return_id);
    }

    total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes_;
    // The task has finished and none of the return IDs are in scope anymore,
    // so it is safe to remove the task spec.
    submissible_tasks_.erase(it);
  }

  return total_lineage_footprint_bytes_ - total_lineage_footprint_bytes_prev;
}

void TaskManager::MarkTaskNoRetryInternal(const TaskID &task_id, bool canceled) {
  ObjectID generator_id = TaskGeneratorId(task_id);
  if (!generator_id.IsNil()) {
    // Pass -1 because the task has been canceled, so we should just end the
    // stream at the caller's current index. This is needed because we may
    // receive generator reports out of order. If the task reports a later
    // index then exits because it was canceled, we will hang waiting for the
    // intermediate indices.
    MarkEndOfStream(generator_id, /*end_of_stream_index=*/-1);
  }

  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it != submissible_tasks_.end()) {
    it->second.num_retries_left_ = 0;
    it->second.num_oom_retries_left_ = 0;
    if (canceled) {
      it->second.is_canceled_ = true;
    }
  }
}

void TaskManager::MarkTaskCanceled(const TaskID &task_id) {
  MarkTaskNoRetryInternal(task_id, /*canceled=*/true);
}

void TaskManager::MarkTaskNoRetry(const TaskID &task_id) {
  MarkTaskNoRetryInternal(task_id, /*canceled=*/false);
}

bool TaskManager::IsTaskCanceled(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return false;
  }
  return it->second.is_canceled_;
}

absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  bool first_execution = false;
  absl::flat_hash_set<ObjectID> store_in_plasma_ids = {};
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    // When a generator task is used, it is possible
    // this API is used after the task has been removed
    // from submissible_tasks_. Do nothing in this case.
    return {};
  }
  first_execution = it->second.num_successful_executions_ == 0;
  if (!first_execution) {
    store_in_plasma_ids = it->second.reconstructable_return_ids_;
  }
  if (first_execution_out != nullptr) {
    *first_execution_out = first_execution;
  }
  return store_in_plasma_ids;
}

void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec,
    rpc::ErrorType error_type,
    const rpc::RayErrorInfo *ray_error_info,
    const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) {
  const TaskID task_id = spec.TaskId();
  RayObject error(error_type, ray_error_info);
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, /*index=*/i + 1);
    if (store_in_plasma_ids.contains(object_id)) {
      Status s = put_in_local_plasma_callback_(error, object_id);
      if (!s.ok()) {
        RAY_LOG(WARNING).WithField(object_id)
            << "Failed to put error object in plasma: " << s;
        in_memory_store_.Put(
            error, object_id, reference_counter_.HasReference(object_id));
      }
    } else {
      in_memory_store_.Put(error, object_id, reference_counter_.HasReference(object_id));
    }
  }
  if (spec.ReturnsDynamic()) {
    for (const auto &dynamic_return_id : spec.DynamicReturnIds()) {
      if (store_in_plasma_ids.contains(dynamic_return_id)) {
        Status s = put_in_local_plasma_callback_(error, dynamic_return_id);
        if (!s.ok()) {
          RAY_LOG(WARNING).WithField(dynamic_return_id)
              << "Failed to put error object in plasma: " << s;
          in_memory_store_.Put(error,
                               dynamic_return_id,
                               reference_counter_.HasReference(dynamic_return_id));
        }
      } else {
        in_memory_store_.Put(
            error, dynamic_return_id, reference_counter_.HasReference(dynamic_return_id));
      }
    }
  }

  if (spec.IsStreamingGenerator()) {
    // If a streaming generator task failed, mark the end of the stream if it
    // hasn't been ended already. The stream will be ended one index past the
    // maximum index seen so far.
    const auto generator_id = spec.ReturnId(0);
    MarkEndOfStream(generator_id, /*item_index*/ -1);

    // If it was a streaming generator, try failing all the return object refs.
    // In a normal time, it is no-op because the object ref values are already
    // written, and Ray doesn't allow to overwrite values for the object ref.
    // It is only useful when lineage reconstruction retry is failed. In this
    // case, all these objects are lost from the plasma store, so we
    // can overwrite them. See the test test_dynamic_generator_reconstruction_fails
    // for more details.
    auto num_streaming_generator_returns = spec.NumStreamingGeneratorReturns();
    for (size_t i = 0; i < num_streaming_generator_returns; i++) {
      const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
      if (store_in_plasma_ids.contains(generator_return_id)) {
        Status s = put_in_local_plasma_callback_(error, generator_return_id);
        if (!s.ok()) {
          RAY_LOG(WARNING).WithField(generator_return_id)
              << "Failed to put error object in plasma: " << s;
          in_memory_store_.Put(error,
                               generator_return_id,
                               reference_counter_.HasReference(generator_return_id));
        }
      } else {
        in_memory_store_.Put(error,
                             generator_return_id,
                             reference_counter_.HasReference(generator_return_id));
      }
    }
  }
}

std::optional<TaskSpecification> TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return std::optional<TaskSpecification>();
  }
  return it->second.spec_;
}

std::vector<TaskID> TaskManager::GetPendingChildrenTasks(
    const TaskID &parent_task_id) const {
  std::vector<TaskID> ret_vec;
  absl::MutexLock lock(&mu_);
  for (const auto &it : submissible_tasks_) {
    if (it.second.IsPending() && (it.second.spec_.ParentTaskId() == parent_task_id)) {
      ret_vec.push_back(it.first);
    }
  }
  return ret_vec;
}

void TaskManager::AddTaskStatusInfo(rpc::CoreWorkerStats *stats) const {
  absl::MutexLock lock(&mu_);
  for (int64_t i = 0; i < stats->object_refs_size(); i++) {
    auto ref = stats->mutable_object_refs(i);
    const auto obj_id = ObjectID::FromBinary(ref->object_id());
    const auto task_id = obj_id.TaskId();
    const auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      continue;
    }
    ref->set_task_status(it->second.GetStatus());
    ref->set_attempt_number(it->second.spec_.AttemptNumber());
  }
}

void TaskManager::MarkDependenciesResolved(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }

  RAY_CHECK(it->second.GetStatus() == rpc::TaskStatus::PENDING_ARGS_AVAIL)
      << ", task ID = " << it->first
      << ", status = " << rpc::TaskStatus_Name(it->second.GetStatus());
  SetTaskStatus(it->second, rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
}

void TaskManager::MarkTaskWaitingForExecution(const TaskID &task_id,
                                              const NodeID &node_id,
                                              const WorkerID &worker_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }
  RAY_CHECK(it->second.GetStatus() == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT)
      << ", task ID = " << it->first
      << ", status = " << rpc::TaskStatus_Name(it->second.GetStatus());
  it->second.SetNodeId(node_id);
  SetTaskStatus(it->second,
                rpc::TaskStatus::SUBMITTED_TO_WORKER,
                worker::TaskStatusEvent::TaskStateUpdate(node_id, worker_id));
}

void TaskManager::SetTaskStatus(
    TaskEntry &task_entry,
    rpc::TaskStatus status,
    std::optional<worker::TaskStatusEvent::TaskStateUpdate> state_update,
    bool include_task_info,
    std::optional<int32_t> attempt_number) {
  RAY_LOG(DEBUG).WithField(task_entry.spec_.TaskId())
      << "Setting task status from " << rpc::TaskStatus_Name(task_entry.GetStatus())
      << " to " << rpc::TaskStatus_Name(status);
  task_entry.SetStatus(status);

  const int32_t attempt_number_to_record =
      attempt_number.value_or(task_entry.spec_.AttemptNumber());
  const auto state_update_to_record =
      state_update.value_or(worker::TaskStatusEvent::TaskStateUpdate());
  RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(task_entry.spec_.TaskId(),
                                                              task_entry.spec_.JobId(),
                                                              attempt_number_to_record,
                                                              task_entry.spec_,
                                                              status,
                                                              include_task_info,
                                                              state_update_to_record));
}

std::unordered_map<rpc::LineageReconstructionTask, uint64_t>
TaskManager::GetOngoingLineageReconstructionTasks(
    const ActorManager &actor_manager) const {
  absl::MutexLock lock(&mu_);
  std::unordered_map<rpc::LineageReconstructionTask, uint64_t> result;
  for (const auto &task_it : submissible_tasks_) {
    const auto &task_entry = task_it.second;
    if (!task_entry.IsPending()) {
      continue;
    }

    if (task_entry.num_successful_executions_ == 0) {
      // Not lineage reconstruction task
      continue;
    }

    rpc::LineageReconstructionTask task;
    task.set_name(task_entry.spec_.GetName());
    task.set_status(task_entry.GetStatus());
    if (task_entry.spec_.IsNormalTask()) {
      task.mutable_labels()->insert(task_entry.spec_.GetMessage().labels().begin(),
                                    task_entry.spec_.GetMessage().labels().end());
    } else if (task_entry.spec_.IsActorTask()) {
      auto actor_handle = actor_manager.GetActorHandle(task_entry.spec_.ActorId());
      RAY_CHECK(actor_handle) << "Actor task must be submitted via actor handle";
      const auto &labels = actor_handle->GetLabels();
      task.mutable_labels()->insert(labels.begin(), labels.end());
    }

    if (result.find(task) != result.end()) {
      result[task] += 1;
    } else {
      result.emplace(std::move(task), 1);
    }
  }

  return result;
}

void TaskManager::FillTaskInfo(rpc::GetCoreWorkerStatsReply *reply,
                               const int64_t limit) const {
  absl::MutexLock lock(&mu_);
  auto total = submissible_tasks_.size();
  auto count = 0;
  for (const auto &task_it : submissible_tasks_) {
    if (limit != -1 && count >= limit) {
      break;
    }
    count += 1;

    const auto &task_entry = task_it.second;
    auto entry = reply->add_owned_task_info_entries();
    const auto &task_spec = task_entry.spec_;
    const auto &task_state = task_entry.GetStatus();
    const auto &node_id = task_entry.GetNodeId();
    rpc::TaskType type;
    if (task_spec.IsNormalTask()) {
      type = rpc::TaskType::NORMAL_TASK;
    } else if (task_spec.IsActorCreationTask()) {
      type = rpc::TaskType::ACTOR_CREATION_TASK;
      entry->set_actor_id(task_spec.ActorCreationId().Binary());
    } else {
      RAY_CHECK(task_spec.IsActorTask());
      type = rpc::TaskType::ACTOR_TASK;
      entry->set_actor_id(task_spec.ActorId().Binary());
    }
    entry->set_type(type);
    entry->set_name(task_spec.GetName());
    entry->set_language(task_spec.GetLanguage());
    entry->set_func_or_class_name(task_spec.FunctionDescriptor()->CallString());
    entry->set_scheduling_state(task_state);
    entry->set_job_id(task_spec.JobId().Binary());
    if (!node_id.IsNil()) {
      entry->set_node_id(node_id.Binary());
    }
    entry->set_task_id(task_spec.TaskIdBinary());
    entry->set_parent_task_id(task_spec.ParentTaskIdBinary());
    const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
    entry->mutable_required_resources()->insert(resources_map.begin(),
                                                resources_map.end());
    entry->mutable_runtime_env_info()->CopyFrom(task_spec.RuntimeEnvInfo());
  }
  reply->set_tasks_total(total);
}

void TaskManager::RecordMetrics() {
  absl::MutexLock lock(&mu_);
  total_lineage_bytes_gauge_.Record(total_lineage_footprint_bytes_);
  task_counter_.FlushOnChangeCallbacks();
}

ObjectID TaskManager::TaskGeneratorId(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return ObjectID::Nil();
  }
  if (!it->second.spec_.ReturnsDynamic()) {
    return ObjectID::Nil();
  }
  return it->second.spec_.ReturnId(0);
}

std::vector<ObjectID> ExtractPlasmaDependencies(const TaskSpecification &spec) {
  std::vector<ObjectID> plasma_dependencies;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      plasma_dependencies.push_back(spec.ArgObjectId(i));
    } else if (spec.ArgTensorTransport(i) != rpc::TensorTransport::OBJECT_STORE) {
      // GPU objects are inlined but the actual data lives on the remote actor.
      // Therefore, we apply the reference counting protocol used for plasma objects
      // instead of decrementing the ref count upon inlining.
      plasma_dependencies.push_back(spec.ArgObjectId(i));
    } else {
      const auto &inlined_refs = spec.ArgInlinedRefs(i);
      for (const auto &inlined_ref : inlined_refs) {
        plasma_dependencies.push_back(ObjectID::FromBinary(inlined_ref.object_id()));
      }
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
    plasma_dependencies.push_back(actor_creation_return_id);
  }
  return plasma_dependencies;
}

}  // namespace core
}  // namespace ray
