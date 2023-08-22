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

#include "ray/common/buffer.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/exponential_backoff.h"
#include "ray/util/util.h"

namespace ray {
namespace core {

// Start throttling task failure logs once we hit this threshold.
const int64_t kTaskFailureThrottlingThreshold = 50;

// Throttle task failure logs to once this interval.
const int64_t kTaskFailureLoggingFrequencyMillis = 5000;

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

Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
  *object_id_out = GetObjectRefAtIndex(next_index_);
  bool is_eof_set = end_of_stream_index_ != -1;
  if (is_eof_set && next_index_ >= end_of_stream_index_) {
    // next_index_ cannot be bigger than end_of_stream_index_.
    RAY_CHECK(next_index_ == end_of_stream_index_);
    RAY_LOG(DEBUG) << "ObjectRefStream of an id " << generator_id_
                   << " has no more objects.";
    return Status::ObjectRefEndOfStream("");
  }

  if (refs_written_to_stream_.find(*object_id_out) != refs_written_to_stream_.end()) {
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

ObjectID ObjectRefStream::PeekNextItem() { return GetObjectRefAtIndex(next_index_); }

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
  refs_written_to_stream_.insert(object_id);
  max_index_seen_ = std::max(max_index_seen_, item_index);
  return true;
}

void ObjectRefStream::MarkEndOfStream(int64_t item_index,
                                      ObjectID *object_id_in_last_index) {
  if (end_of_stream_index_ != -1) {
    return;
  }
  // ObjectRefStream should guarantee the max_index_seen_
  // will always have an object reference to avoid hang.
  // That said, if there was already an index that's bigger than a given
  // end of stream index, we should mark that as the end of stream.
  // It can happen when a task is retried and return less values
  // (e.g., the second retry is failed by an exception or worker failure).
  end_of_stream_index_ = std::max(max_index_seen_ + 1, item_index);

  auto end_of_stream_id = GetObjectRefAtIndex(end_of_stream_index_);
  *object_id_in_last_index = end_of_stream_id;
  return;
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
      task_deps.push_back(spec.ArgId(i));
      RAY_LOG(DEBUG) << "Adding arg ID " << spec.ArgId(i);
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
  std::vector<ObjectID> return_ids;
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
      reference_counter_->AddOwnedObject(return_id,
                                         /*inner_ids=*/{},
                                         caller_address,
                                         call_site,
                                         -1,
                                         is_reconstructable,
                                         /*add_local_ref=*/true);
    }

    return_ids.push_back(return_id);
    rpc::ObjectReference ref;
    ref.set_object_id(spec.ReturnId(i).Binary());
    ref.mutable_owner_address()->CopyFrom(caller_address);
    ref.set_call_site(call_site);
    returned_refs.push_back(std::move(ref));
  }

  reference_counter_->UpdateSubmittedTaskReferences(return_ids, task_deps);

  // If it is a generator task, create an object ref stream.
  // The language frontend is responsible for calling DeleteObjectRefStream.
  if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);
    RAY_LOG(DEBUG) << "Create an object ref stream of an id " << generator_id;
    absl::MutexLock lock(&objet_ref_stream_ops_mu_);
    auto inserted =
        object_ref_streams_.emplace(generator_id, ObjectRefStream(generator_id));
    RAY_CHECK(inserted.second);
  }

  {
    absl::MutexLock lock(&mu_);
    auto inserted = submissible_tasks_.try_emplace(
        spec.TaskId(), spec, max_retries, num_returns, task_counter_, max_oom_retries);
    RAY_CHECK(inserted.second);
    num_pending_tasks_++;
  }

  RecordTaskStatusEvent(spec.AttemptNumber(),
                        spec,
                        rpc::TaskStatus::PENDING_ARGS_AVAIL,
                        /* include_task_info */ true);

  return returned_refs;
}

bool TaskManager::ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) {
  TaskSpecification spec;
  bool resubmit = false;
  std::vector<ObjectID> return_ids;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      // This can happen when the task has already been
      // retried up to its max attempts.
      return false;
    }

    if (!it->second.IsPending()) {
      resubmit = true;
      MarkTaskRetryOnResubmit(it->second);
      num_pending_tasks_++;

      // The task is pending again, so it's no longer counted as lineage. If
      // the task finishes and we still need the spec, we'll add the task back
      // to the footprint sum.
      total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes;
      it->second.lineage_footprint_bytes = 0;

      if (it->second.num_retries_left > 0) {
        it->second.num_retries_left--;
      } else {
        RAY_CHECK(it->second.num_retries_left == -1);
      }
      spec = it->second.spec;

      for (const auto &return_id : it->second.reconstructable_return_ids) {
        return_ids.push_back(return_id);
      }
    }
  }

  if (resubmit) {
    for (size_t i = 0; i < spec.NumArgs(); i++) {
      if (spec.ArgByRef(i)) {
        task_deps->push_back(spec.ArgId(i));
      } else {
        const auto &inlined_refs = spec.ArgInlinedRefs(i);
        for (const auto &inlined_ref : inlined_refs) {
          task_deps->push_back(ObjectID::FromBinary(inlined_ref.object_id()));
        }
      }
    }

    reference_counter_->UpdateResubmittedTaskReferences(return_ids, *task_deps);

    for (const auto &task_dep : *task_deps) {
      bool was_freed = reference_counter_->TryMarkFreedObjectInUseAgain(task_dep);
      if (was_freed) {
        RAY_LOG(DEBUG) << "Dependency " << task_dep << " of task " << task_id
                       << " was freed";
        // We do not keep around copies for objects that were freed, but now that
        // they're needed for recovery, we need to generate and pin a new copy.
        // Delete the old in-memory marker that indicated that the object was
        // freed. Now workers that attempt to get the object will be able to get
        // the reconstructed value.
        in_memory_store_->Delete({task_dep});
      }
    }
    if (spec.IsActorTask()) {
      const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
      reference_counter_->UpdateResubmittedTaskReferences(return_ids,
                                                          {actor_creation_return_id});
    }

    RAY_LOG(INFO) << "Resubmitting task that produced lost plasma object, attempt #"
                  << spec.AttemptNumber() << ": " << spec.DebugString();
    retry_task_callback_(spec, /*object_recovery*/ true, /*delay_ms*/ 0);
  }

  return true;
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
      shutdown_hook_ = shutdown;
    }
  }

  // Do not hold the lock when calling callbacks.
  if (!has_pending_tasks) {
    shutdown();
  }
}

bool TaskManager::IsTaskSubmissible(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  return submissible_tasks_.count(task_id) > 0;
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

bool TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                   const rpc::ReturnObject &return_object,
                                   const NodeID &worker_raylet_id,
                                   bool store_in_plasma) {
  bool direct_return = false;
  reference_counter_->UpdateObjectSize(object_id, return_object.size());
  RAY_LOG(DEBUG) << "Task return object " << object_id << " has size "
                 << return_object.size();
  const auto nested_refs =
      VectorFromProtobuf<rpc::ObjectReference>(return_object.nested_inlined_refs());

  if (return_object.in_plasma()) {
    // NOTE(swang): We need to add the location of the object before marking
    // it as local in the in-memory store so that the data locality policy
    // will choose the right raylet for any queued dependent tasks.
    reference_counter_->UpdateObjectPinnedAtRaylet(object_id, worker_raylet_id);
    // Mark it as in plasma with a dummy object.
    RAY_CHECK(
        in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  } else {
    // NOTE(swang): If a direct object was promoted to plasma, then we do not
    // record the node ID that it was pinned at, which means that we will not
    // be able to reconstruct it if the plasma object copy is lost. However,
    // this is okay because the pinned copy is on the local node, so we will
    // fate-share with the object if the local node fails.
    std::shared_ptr<LocalMemoryBuffer> data_buffer;
    if (return_object.data().size() > 0) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(
              reinterpret_cast<const uint8_t *>(return_object.data().data())),
          return_object.data().size());
    }
    std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
    if (return_object.metadata().size() > 0) {
      metadata_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(
              reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
          return_object.metadata().size());
    }

    RayObject object(data_buffer, metadata_buffer, nested_refs);
    if (store_in_plasma) {
      put_in_local_plasma_callback_(object, object_id);
    } else {
      direct_return = in_memory_store_->Put(object, object_id);
    }
  }

  rpc::Address owner_address;
  if (reference_counter_->GetOwner(object_id, &owner_address) && !nested_refs.empty()) {
    std::vector<ObjectID> nested_ids;
    for (const auto &nested_ref : nested_refs) {
      nested_ids.emplace_back(ObjectRefToId(nested_ref));
    }
    reference_counter_->AddNestedObjectIds(object_id, nested_ids, owner_address);
  }
  return direct_return;
}

void TaskManager::DelObjectRefStream(const ObjectID &generator_id) {
  absl::MutexLock lock(&objet_ref_stream_ops_mu_);

  RAY_LOG(DEBUG) << "Deleting an object ref stream of an id " << generator_id;
  absl::flat_hash_set<ObjectID> object_ids_unconsumed;

  auto it = object_ref_streams_.find(generator_id);
  if (it == object_ref_streams_.end()) {
    return;
  }

  const auto &stream = it->second;
  object_ids_unconsumed = stream.GetItemsUnconsumed();
  object_ref_streams_.erase(generator_id);

  // When calling RemoveLocalReference, we shouldn't hold a lock.
  for (const auto &object_id : object_ids_unconsumed) {
    std::vector<ObjectID> deleted;
    RAY_LOG(DEBUG) << "Removing unconsume streaming ref " << object_id;
    reference_counter_->RemoveLocalReference(object_id, &deleted);
    // TODO(sang): This is required because the reference counter
    // cannot remove objects from the in memory store.
    // Instead of doing this manually here, we should modify
    // reference_count.h to automatically remove objects
    // when the ref goes to 0.
    in_memory_store_->Delete(deleted);
  }
}

Status TaskManager::TryReadObjectRefStream(const ObjectID &generator_id,
                                           ObjectID *object_id_out) {
  absl::MutexLock lock(&objet_ref_stream_ops_mu_);
  RAY_CHECK(object_id_out != nullptr);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end())
      << "TryReadObjectRefStream API can be used only when the stream has been "
         "created "
         "and not removed.";
  return stream_it->second.TryReadNextItem(object_id_out);
}

ObjectID TaskManager::PeekObjectRefStream(const ObjectID &generator_id) {
  ObjectID next_object_id;
  absl::MutexLock lock(&objet_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end())
      << "PeekObjectRefStream API can be used only when the stream has been "
         "created and not removed.";
  next_object_id = stream_it->second.PeekNextItem();

  // Temporarily own the ref since the corresponding reference is probably
  // not reported yet.
  TemporarilyOwnGeneratorReturnRefIfNeededInternal(next_object_id, generator_id);
  return next_object_id;
}

bool TaskManager::ObjectRefStreamExists(const ObjectID &generator_id) {
  absl::MutexLock lock(&objet_ref_stream_ops_mu_);
  auto it = object_ref_streams_.find(generator_id);
  return it != object_ref_streams_.end();
}

void TaskManager::MarkEndOfStream(const ObjectID &generator_id,
                                  int64_t end_of_stream_index) {
  absl::MutexLock lock(&objet_ref_stream_ops_mu_);
  ObjectID last_object_id;

  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    // Stream has been already deleted. Do not handle it.
    return;
  }

  stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);
  RAY_LOG(DEBUG) << "Write EoF to the object ref stream. Index: " << end_of_stream_index
                 << ". Last object id: " << last_object_id;

  if (!last_object_id.IsNil()) {
    reference_counter_->OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);
    RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
    // Put a dummy object at the end of the stream. We don't need to check if
    // the object should be stored in plasma because the end of the stream is a
    // fake ObjectRef that should never be read by the application.
    in_memory_store_->Put(error, last_object_id);
  }
}

bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  const auto &task_id = generator_id.TaskId();
  int64_t item_index = request.item_index();
  uint64_t attempt_number = request.attempt_number();
  // Every generated object has the same task id.
  RAY_LOG(DEBUG) << "Received an intermediate result of index " << item_index
                 << " generator_id: " << generator_id;

  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
      if (it->second.spec.AttemptNumber() > attempt_number) {
        // Generator task reports can arrive at any time. If the first attempt
        // fails, we may receive a report from the first executor after the
        // second attempt has started. In this case, we should ignore the first
        // attempt.
        return false;
      }
    }
  }

  // NOTE: If it is the first execution (e.g., CompletePendingTask has never been called),
  // it is always empty.
  const auto store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id);

  absl::MutexLock lock(&objet_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    // Stream has been already deleted. Do not handle it.
    return false;
  }

  // TODO(sang): Support the regular return values as well.
  size_t num_objects_written = 0;
  for (const auto &return_object : request.dynamic_return_objects()) {
    const auto object_id = ObjectID::FromBinary(return_object.object_id());
    RAY_LOG(DEBUG) << "Write an object " << object_id
                   << " to the object ref stream of id " << generator_id;
    bool index_not_used_yet = false;

    auto stream_it = object_ref_streams_.find(generator_id);
    if (stream_it != object_ref_streams_.end()) {
      index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);
    }
    // If the ref was written to a stream, we should also
    // own the dynamically generated task return.
    // NOTE: If we call this method while holding a lock, it can deadlock.
    if (index_not_used_yet) {
      reference_counter_->OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
      num_objects_written += 1;
    }
    // When an object is reported, the object is ready to be fetched.
    reference_counter_->UpdateObjectReady(object_id);
    HandleTaskReturn(object_id,
                     return_object,
                     NodeID::FromBinary(request.worker_addr().raylet_id()),
                     /*store_in_plasma*/ store_in_plasma_ids.count(object_id));
  }
  return num_objects_written != 0;
}

bool TaskManager::TemporarilyOwnGeneratorReturnRefIfNeeded(const ObjectID &object_id,
                                                           const ObjectID &generator_id) {
  absl::MutexLock lock(&objet_ref_stream_ops_mu_);
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

  // We shouldn't hold a lock when calling refernece counter API.
  if (inserted_to_stream) {
    RAY_LOG(DEBUG) << "Added streaming ref " << object_id;
    reference_counter_->OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
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
        reference_counter_->AddDynamicReturn(object_id, generator_id);
        dynamic_return_ids.push_back(object_id);
      }
      if (!HandleTaskReturn(object_id,
                            return_object,
                            NodeID::FromBinary(worker_addr.raylet_id()),
                            store_in_plasma_ids.count(object_id))) {
        if (first_execution) {
          dynamic_returns_in_plasma.push_back(object_id);
        }
      }
    }
  }

  for (const auto &return_object : reply.return_objects()) {
    const auto object_id = ObjectID::FromBinary(return_object.object_id());
    if (HandleTaskReturn(object_id,
                         return_object,
                         NodeID::FromBinary(worker_addr.raylet_id()),
                         store_in_plasma_ids.count(object_id))) {
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
    spec = it->second.spec;

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
        it->second.reconstructable_return_ids.insert(dynamic_return_id);
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
              it->second.reconstructable_return_ids.insert(
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
                     << it->second.reconstructable_return_ids.size()
                     << " plasma returns in scope";
      it->second.reconstructable_return_ids.erase(direct_return_id);
    }
    RAY_LOG(DEBUG) << "Task " << it->first << " now has "
                   << it->second.reconstructable_return_ids.size()
                   << " plasma returns in scope";
    it->second.num_successful_executions++;

    if (is_application_error) {
      SetTaskStatus(it->second,
                    rpc::TaskStatus::FAILED,
                    gcs::GetRayErrorInfo(rpc::ErrorType::TASK_EXECUTION_EXCEPTION,
                                         reply.task_execution_error()));
    } else {
      SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
    }
    num_pending_tasks_--;

    // A finished task can only be re-executed if it has some number of
    // retries left and returned at least one object that is still in use and
    // stored in plasma.
    bool task_retryable = it->second.num_retries_left != 0 &&
                          !it->second.reconstructable_return_ids.empty();
    if (task_retryable) {
      // Pin the task spec if it may be retried again.
      release_lineage = false;
      it->second.lineage_footprint_bytes = it->second.spec.GetMessage().ByteSizeLong();
      total_lineage_footprint_bytes_ += it->second.lineage_footprint_bytes;
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
          HandleTaskReturn(generator_return_id,
                           return_object,
                           NodeID::FromBinary(worker_addr.raylet_id()),
                           store_in_plasma_ids.count(generator_return_id));
        }
      }
    }
  }

  RemoveFinishedTaskReferences(spec, release_lineage, worker_addr, reply.borrowed_refs());
  if (min_lineage_bytes_to_evict > 0) {
    // Evict at least half of the current lineage.
    auto bytes_evicted = reference_counter_->EvictLineage(min_lineage_bytes_to_evict);
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
    RAY_CHECK(it->second.IsPending())
        << "Tried to retry task that was not pending " << task_id;
    spec = it->second.spec;
    num_retries_left = it->second.num_retries_left;
    num_oom_retries_left = it->second.num_oom_retries_left;
    if (task_failed_due_to_oom) {
      if (num_oom_retries_left > 0) {
        will_retry = true;
        it->second.num_oom_retries_left--;
      } else if (num_oom_retries_left == -1) {
        will_retry = true;
      } else {
        RAY_CHECK(num_oom_retries_left == 0);
      }
    } else {
      if (num_retries_left > 0) {
        will_retry = true;
        it->second.num_retries_left--;
      } else if (num_retries_left == -1) {
        will_retry = true;
      } else {
        RAY_CHECK(num_retries_left == 0);
      }
    }
    if (will_retry) {
      MarkTaskRetryOnFailed(it->second, error_info);
    }
  }

  // We should not hold the lock during these calls because they may trigger
  // callbacks in this or other classes.
  std::ostringstream stream;
  std::string num_retries_left_str =
      num_retries_left == -1 ? "infinite" : std::to_string(num_retries_left);
  RAY_LOG(INFO) << "task " << spec.TaskId() << " retries left: " << num_retries_left_str
                << ", oom retries left: " << num_oom_retries_left
                << ", task failed due to oom: " << task_failed_due_to_oom;
  if (will_retry) {
    RAY_LOG(INFO) << "Attempting to resubmit task " << spec.TaskId()
                  << " for attempt number: " << spec.AttemptNumber();
    // TODO(clarng): clean up and remove task_retry_delay_ms that is relied
    // on by some tests.
    int32_t delay_ms = task_failed_due_to_oom
                           ? ExponentialBackoff::GetBackoffMs(
                                 spec.AttemptNumber(),
                                 RayConfig::instance().task_oom_retry_delay_base_ms())
                           : RayConfig::instance().task_retry_delay_ms();
    retry_task_callback_(spec, /*object_recovery*/ false, delay_ms);
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
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to fail task that was not pending " << task_id;
    RAY_CHECK(it->second.IsPending())
        << "Tried to fail task that was not pending " << task_id;
    spec = it->second.spec;

    if (status && status->IsIntentionalSystemExit()) {
      // We don't mark intentional system exit as failures, such as tasks that
      // exit by exit_actor(), exit by ray.shutdown(), etc. These tasks are expected
      // to exit and not be marked as failure.
      SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
    } else {
      SetTaskStatus(
          it->second,
          rpc::TaskStatus::FAILED,
          (ray_error_info == nullptr
               ? gcs::GetRayErrorInfo(error_type, (status ? status->ToString() : ""))
               : *ray_error_info));
    }
    submissible_tasks_.erase(it);
    num_pending_tasks_--;

    // Throttled logging of task failure errors.
    auto debug_str = spec.DebugString();
    if (debug_str.find("__ray_terminate__") == std::string::npos &&
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
                               ReferenceCounter::ReferenceTableProto());

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
  RAY_LOG(DEBUG) << "Task attempt " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type) << " Fail immediately? "
                 << fail_immediately;
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
  reference_counter_->UpdateSubmittedTaskReferences(
      /*return_ids=*/{},
      /*argument_ids_to_add=*/contained_ids,
      /*argument_ids_to_remove=*/inlined_dependency_ids,
      &deleted);
  in_memory_store_->Delete(deleted);
}

void TaskManager::RemoveFinishedTaskReferences(
    TaskSpecification &spec,
    bool release_lineage,
    const rpc::Address &borrower_addr,
    const ReferenceCounter::ReferenceTableProto &borrowed_refs) {
  std::vector<ObjectID> plasma_dependencies;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      plasma_dependencies.push_back(spec.ArgId(i));
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

  std::vector<ObjectID> return_ids;
  size_t num_returns = spec.NumReturns();
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
  reference_counter_->UpdateFinishedTaskReferences(return_ids,
                                                   plasma_dependencies,
                                                   release_lineage,
                                                   borrower_addr,
                                                   borrowed_refs,
                                                   &deleted);
  in_memory_store_->Delete(deleted);
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
  for (const auto &plasma_id : it->second.reconstructable_return_ids) {
    RAY_LOG(DEBUG) << "Task " << task_id << " has " << plasma_id << " in scope";
  }
  it->second.reconstructable_return_ids.erase(object_id);
  RAY_LOG(DEBUG) << "Task " << task_id << " now has "
                 << it->second.reconstructable_return_ids.size()
                 << " plasma returns in scope";

  if (it->second.reconstructable_return_ids.empty() && !it->second.IsPending()) {
    // If the task can no longer be retried, decrement the lineage ref count
    // for each of the task's args.
    for (size_t i = 0; i < it->second.spec.NumArgs(); i++) {
      if (it->second.spec.ArgByRef(i)) {
        released_objects->push_back(it->second.spec.ArgId(i));
      } else {
        const auto &inlined_refs = it->second.spec.ArgInlinedRefs(i);
        for (const auto &inlined_ref : inlined_refs) {
          released_objects->push_back(ObjectID::FromBinary(inlined_ref.object_id()));
        }
      }
    }

    total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes;
    // The task has finished and none of the return IDs are in scope anymore,
    // so it is safe to remove the task spec.
    submissible_tasks_.erase(it);
  }

  return total_lineage_footprint_bytes_ - total_lineage_footprint_bytes_prev;
}

bool TaskManager::MarkTaskCanceled(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it != submissible_tasks_.end()) {
    it->second.num_retries_left = 0;
    it->second.num_oom_retries_left = 0;
  }
  return it != submissible_tasks_.end();
}

absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  bool first_execution;
  absl::flat_hash_set<ObjectID> store_in_plasma_ids = {};
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    // When a generator task is used, it is possible
    // this API is used after the task has been removed
    // from submissible_tasks_. Do nothing in this case.
    return {};
  }
  first_execution = it->second.num_successful_executions == 0;
  if (!first_execution) {
    store_in_plasma_ids = it->second.reconstructable_return_ids;
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
    if (store_in_plasma_ids.count(object_id)) {
      put_in_local_plasma_callback_(error, object_id);
    } else {
      in_memory_store_->Put(error, object_id);
    }
  }
  if (spec.ReturnsDynamic()) {
    for (const auto &dynamic_return_id : spec.DynamicReturnIds()) {
      if (store_in_plasma_ids.count(dynamic_return_id)) {
        put_in_local_plasma_callback_(error, dynamic_return_id);
      } else {
        in_memory_store_->Put(error, dynamic_return_id);
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
      if (store_in_plasma_ids.count(generator_return_id)) {
        put_in_local_plasma_callback_(error, generator_return_id);
      } else {
        in_memory_store_->Put(error, generator_return_id);
      }
    }
  }
}

absl::optional<TaskSpecification> TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return absl::optional<TaskSpecification>();
  }
  return it->second.spec;
}

std::vector<TaskID> TaskManager::GetPendingChildrenTasks(
    const TaskID &parent_task_id) const {
  std::vector<TaskID> ret_vec;
  absl::MutexLock lock(&mu_);
  for (auto it : submissible_tasks_) {
    if (it.second.IsPending() && (it.second.spec.ParentTaskId() == parent_task_id)) {
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
    ref->set_attempt_number(it->second.spec.AttemptNumber());
  }
}

void TaskManager::MarkDependenciesResolved(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }
  if (it->second.GetStatus() == rpc::TaskStatus::PENDING_ARGS_AVAIL) {
    SetTaskStatus(it->second, rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
  }
}

void TaskManager::MarkTaskWaitingForExecution(const TaskID &task_id,
                                              const NodeID &node_id,
                                              const WorkerID &worker_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }
  RAY_CHECK(it->second.GetStatus() == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
  it->second.SetNodeId(node_id);
  it->second.SetStatus(rpc::TaskStatus::SUBMITTED_TO_WORKER);
  RecordTaskStatusEvent(it->second.spec.AttemptNumber(),
                        it->second.spec,
                        rpc::TaskStatus::SUBMITTED_TO_WORKER,
                        /* include_task_info */ false,
                        worker::TaskStatusEvent::TaskStateUpdate(node_id, worker_id));
}

void TaskManager::MarkTaskRetryOnResubmit(TaskEntry &task_entry) {
  // Record the old attempt status as FINISHED.
  RecordTaskStatusEvent(
      task_entry.spec.AttemptNumber(), task_entry.spec, rpc::TaskStatus::FINISHED);
  task_entry.MarkRetryOnResubmit();

  // Mark the new status and also include task spec info for the new attempt.
  task_entry.SetStatus(rpc::TaskStatus::PENDING_ARGS_AVAIL);
  // NOTE(rickyx): We only increment the AttemptNumber on the task spec when
  // `retry_task_callback_` is invoked. In order to record the correct status change for
  // the new task attempt, we pass the the attempt number explicitly.
  RecordTaskStatusEvent(task_entry.spec.AttemptNumber() + 1,
                        task_entry.spec,
                        rpc::TaskStatus::PENDING_ARGS_AVAIL,
                        /* include_task_info */ true);
}

void TaskManager::MarkTaskRetryOnFailed(TaskEntry &task_entry,
                                        const rpc::RayErrorInfo &error_info) {
  // Record the old attempt status as FAILED.
  RecordTaskStatusEvent(task_entry.spec.AttemptNumber(),
                        task_entry.spec,
                        rpc::TaskStatus::FAILED,
                        /* include_task_info */ false,
                        worker::TaskStatusEvent::TaskStateUpdate(error_info));
  task_entry.MarkRetryOnFailed();

  // Mark the new status and also include task spec info for the new attempt.
  task_entry.SetStatus(rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
  RecordTaskStatusEvent(task_entry.spec.AttemptNumber() + 1,
                        task_entry.spec,
                        rpc::TaskStatus::PENDING_NODE_ASSIGNMENT,
                        /* include_task_info */ true);
}

void TaskManager::SetTaskStatus(
    TaskEntry &task_entry,
    rpc::TaskStatus status,
    const absl::optional<const rpc::RayErrorInfo> &error_info) {
  task_entry.SetStatus(status);
  RecordTaskStatusEvent(task_entry.spec.AttemptNumber(),
                        task_entry.spec,
                        status,
                        /* include_task_info */ false,
                        worker::TaskStatusEvent::TaskStateUpdate(error_info));
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
    const auto &task_spec = task_entry.spec;
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
    entry->set_task_id(task_spec.TaskId().Binary());
    entry->set_parent_task_id(task_spec.ParentTaskId().Binary());
    const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
    entry->mutable_required_resources()->insert(resources_map.begin(),
                                                resources_map.end());
    entry->mutable_runtime_env_info()->CopyFrom(task_spec.RuntimeEnvInfo());
  }
  reply->set_tasks_total(total);
}

void TaskManager::RecordMetrics() {
  absl::MutexLock lock(&mu_);
  task_counter_.FlushOnChangeCallbacks();
}

void TaskManager::RecordTaskStatusEvent(
    int32_t attempt_number,
    const TaskSpecification &spec,
    rpc::TaskStatus status,
    bool include_task_info,
    absl::optional<const worker::TaskStatusEvent::TaskStateUpdate> state_update) {
  if (!task_event_buffer_.Enabled()) {
    return;
  }
  auto task_event = std::make_unique<worker::TaskStatusEvent>(
      spec.TaskId(),
      spec.JobId(),
      attempt_number,
      status,
      /* timestamp */ absl::GetCurrentTimeNanos(),
      include_task_info ? std::make_shared<const TaskSpecification>(spec) : nullptr,
      std::move(state_update));

  task_event_buffer_.AddTaskEvent(std::move(task_event));
}

ObjectID TaskManager::TaskGeneratorId(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return ObjectID::Nil();
  }
  if (!it->second.spec.ReturnsDynamic()) {
    return ObjectID::Nil();
  }
  return it->second.spec.ReturnId(0);
}

}  // namespace core
}  // namespace ray
