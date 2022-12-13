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

#include "ray/core_worker/store_provider/plasma_store_provider.h"

#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace core {

void BufferTracker::Record(const ObjectID &object_id,
                           TrackedBuffer *buffer,
                           const std::string &call_site) {
  absl::MutexLock lock(&active_buffers_mutex_);
  active_buffers_[std::make_pair(object_id, buffer)] = call_site;
}

void BufferTracker::Release(const ObjectID &object_id, TrackedBuffer *buffer) {
  absl::MutexLock lock(&active_buffers_mutex_);
  auto key = std::make_pair(object_id, buffer);
  RAY_CHECK(active_buffers_.contains(key));
  active_buffers_.erase(key);
}

absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>>
BufferTracker::UsedObjects() const {
  absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> used;
  absl::MutexLock lock(&active_buffers_mutex_);
  for (const auto &entry : active_buffers_) {
    auto it = used.find(entry.first.first);
    if (it != used.end()) {
      // Prefer to keep entries that have non-empty callsites.
      if (!it->second.second.empty()) {
        continue;
      }
    }
    used[entry.first.first] = std::make_pair(entry.first.second->Size(), entry.second);
  }
  return used;
}

CoreWorkerPlasmaStoreProvider::CoreWorkerPlasmaStoreProvider(
    const std::string &store_socket,
    const std::shared_ptr<raylet::RayletClient> raylet_client,
    const std::shared_ptr<ReferenceCounter> reference_counter,
    std::function<Status()> check_signals,
    bool warmup,
    std::function<std::string()> get_current_call_site)
    : raylet_client_(raylet_client),
      reference_counter_(reference_counter),
      check_signals_(check_signals) {
  if (get_current_call_site != nullptr) {
    get_current_call_site_ = get_current_call_site;
  } else {
    get_current_call_site_ = []() { return "<no callsite callback>"; };
  }
  object_store_full_delay_ms_ = RayConfig::instance().object_store_full_delay_ms();
  buffer_tracker_ = std::make_shared<BufferTracker>();
  RAY_CHECK_OK(store_client_.Connect(store_socket));
  if (warmup) {
    RAY_CHECK_OK(WarmupStore());
  }
}

CoreWorkerPlasmaStoreProvider::~CoreWorkerPlasmaStoreProvider() {
  RAY_IGNORE_EXPR(store_client_.Disconnect());
}

Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id,
                                          const rpc::Address &owner_address,
                                          bool *object_exists) {
  RAY_CHECK(!object.IsInPlasmaError()) << object_id;
  std::shared_ptr<Buffer> data;
  RAY_RETURN_NOT_OK(Create(object.GetMetadata(),
                           object.HasData() ? object.GetData()->Size() : 0,
                           object_id,
                           owner_address,
                           &data,
                           /*created_by_worker=*/true));
  // data could be a nullptr if the ObjectID already existed, but this does
  // not throw an error.
  if (data != nullptr) {
    if (object.HasData()) {
      memcpy(data->Data(), object.GetData()->Data(), object.GetData()->Size());
    }
    RAY_RETURN_NOT_OK(Seal(object_id));
    if (object_exists) {
      *object_exists = false;
    }
  } else if (object_exists) {
    *object_exists = true;
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Create(const std::shared_ptr<Buffer> &metadata,
                                             const size_t data_size,
                                             const ObjectID &object_id,
                                             const rpc::Address &owner_address,
                                             std::shared_ptr<Buffer> *data,
                                             bool created_by_worker) {
  auto source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  if (!created_by_worker) {
    source = plasma::flatbuf::ObjectSource::RestoredFromStorage;
  }
  Status status =
      store_client_.CreateAndSpillIfNeeded(object_id,
                                           owner_address,
                                           data_size,
                                           metadata ? metadata->Data() : nullptr,
                                           metadata ? metadata->Size() : 0,
                                           data,
                                           source,
                                           /*device_num=*/0);

  if (status.IsObjectStoreFull()) {
    RAY_LOG(ERROR) << "Failed to put object " << object_id
                   << " in object store because it "
                   << "is full. Object size is " << data_size << " bytes.\n"
                   << "Plasma store status:\n"
                   << MemoryUsageString() << "\n---\n"
                   << "--- Tip: Use the `ray memory` command to list active objects "
                      "in the cluster."
                   << "\n---\n";

    // Replace the status with a more helpful error message.
    std::ostringstream message;
    message << "Failed to put object " << object_id << " in object store because it "
            << "is full. Object size is " << data_size << " bytes.";
    status = Status::ObjectStoreFull(message.str());
  } else if (status.IsObjectExists()) {
    RAY_LOG(WARNING) << "Trying to put an object that already existed in plasma: "
                     << object_id << ".";
    status = Status::OK();
  } else {
    RAY_RETURN_NOT_OK(status);
  }
  return status;
}

Status CoreWorkerPlasmaStoreProvider::Seal(const ObjectID &object_id) {
  return store_client_.Seal(object_id);
}

Status CoreWorkerPlasmaStoreProvider::Release(const ObjectID &object_id) {
  return store_client_.Release(object_id);
}

Status CoreWorkerPlasmaStoreProvider::FetchFromPlasmaStore(const std::vector<ObjectID> &batch_ids, bool in_direct_call, const TaskID &task_id) {
    const auto owner_addresses = reference_counter_->GetOwnerAddresses(batch_ids);
    RAY_RETURN_NOT_OK(
        raylet_client_->FetchOrReconstruct(batch_ids,
                                           owner_addresses,
                                           /*fetch_only*/true,
                                           /*mark_worker_blocked*/ !in_direct_call,
                                           task_id));

    return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::FetchAndGetFromPlasmaStore(
    absl::flat_hash_set<ObjectID> &remaining,
    const std::vector<ObjectID> &batch_ids,
    int64_t timeout_ms,
    bool fetch_only,
    bool in_direct_call,
    const TaskID &task_id,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  const auto owner_addresses = reference_counter_->GetOwnerAddresses(batch_ids);
  RAY_RETURN_NOT_OK(
      raylet_client_->FetchOrReconstruct(batch_ids,
                                         owner_addresses,
                                         fetch_only,
                                         /*mark_worker_blocked*/ !in_direct_call,
                                         task_id));

  std::vector<plasma::ObjectBuffer> plasma_results;
  RAY_RETURN_NOT_OK(store_client_.Get(batch_ids,
                                      timeout_ms,
                                      &plasma_results,
                                      /*is_from_worker=*/true));

  // Add successfully retrieved objects to the result map and remove them from
  // the set of IDs to get.
  for (size_t i = 0; i < plasma_results.size(); i++) {
    if (plasma_results[i].data != nullptr || plasma_results[i].metadata != nullptr) {
      const auto &object_id = batch_ids[i];
      std::shared_ptr<TrackedBuffer> data = nullptr;
      std::shared_ptr<Buffer> metadata = nullptr;
      if (plasma_results[i].data && plasma_results[i].data->Size()) {
        // We track the set of active data buffers in active_buffers_. On destruction,
        // the buffer entry will be removed from the set via callback.
        data = std::make_shared<TrackedBuffer>(
            plasma_results[i].data, buffer_tracker_, object_id);
        buffer_tracker_->Record(object_id, data.get(), get_current_call_site_());
      }
      if (plasma_results[i].metadata && plasma_results[i].metadata->Size()) {
        metadata = plasma_results[i].metadata;
      }
      const auto result_object = std::make_shared<RayObject>(
          data, metadata, std::vector<rpc::ObjectReference>());
      (*results)[object_id] = result_object;
      remaining.erase(object_id);
      if (result_object->IsException()) {
        RAY_CHECK(!result_object->IsInPlasmaError());
        *got_exception = true;
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::GetIfLocal(
    const std::vector<ObjectID> &object_ids,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  std::vector<plasma::ObjectBuffer> plasma_results;
  // Since this path is used only for spilling, we should set is_from_worker: false.
  RAY_RETURN_NOT_OK(store_client_.Get(object_ids,
                                      /*timeout_ms=*/0,
                                      &plasma_results,
                                      /*is_from_worker=*/false));

  for (size_t i = 0; i < object_ids.size(); i++) {
    if (plasma_results[i].data != nullptr || plasma_results[i].metadata != nullptr) {
      const auto &object_id = object_ids[i];
      std::shared_ptr<TrackedBuffer> data = nullptr;
      std::shared_ptr<Buffer> metadata = nullptr;
      if (plasma_results[i].data && plasma_results[i].data->Size()) {
        // We track the set of active data buffers in active_buffers_. On destruction,
        // the buffer entry will be removed from the set via callback.
        data = std::make_shared<TrackedBuffer>(
            plasma_results[i].data, buffer_tracker_, object_id);
        buffer_tracker_->Record(object_id, data.get(), get_current_call_site_());
      }
      if (plasma_results[i].metadata && plasma_results[i].metadata->Size()) {
        metadata = plasma_results[i].metadata;
      }
      const auto result_object = std::make_shared<RayObject>(
          data, metadata, std::vector<rpc::ObjectReference>());
      (*results)[object_id] = result_object;
    }
  }
  return Status::OK();
}

Status UnblockIfNeeded(const std::shared_ptr<raylet::RayletClient> &client,
                       const WorkerContext &ctx) {
  if (ctx.CurrentTaskIsDirectCall()) {
    // NOTE: for direct call actors, we still need to issue an unblock IPC to release
    // get subscriptions, even if the worker isn't blocked.
    if (ctx.ShouldReleaseResourcesOnBlockingCalls() || ctx.CurrentActorIsDirectCall()) {
      return client->NotifyDirectCallTaskUnblocked();
    } else {
      return Status::OK();  // We don't need to release resources.
    }
  } else {
    return client->NotifyUnblocked(ctx.GetCurrentTaskID());
  }
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  int64_t batch_size = RayConfig::instance().worker_fetch_request_size();
  std::vector<ObjectID> batch_ids;
  absl::flat_hash_set<ObjectID> remaining(object_ids.begin(), object_ids.end());

  // First, attempt to fetch all of the required objects once without reconstructing.
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  int64_t total_size = static_cast<int64_t>(object_ids.size());
  for (int64_t start = 0; start < total_size; start += batch_size) {
    batch_ids.clear();
    for (int64_t i = start; i < batch_size && i < total_size; i++) {
      batch_ids.push_back(id_vector[start + i]);
    }
    RAY_RETURN_NOT_OK(FetchAndGetFromPlasmaStore(remaining,
                                                 batch_ids,
                                                 /*timeout_ms=*/0,
                                                 /*fetch_only=*/true,
                                                 ctx.CurrentTaskIsDirectCall(),
                                                 ctx.GetCurrentTaskID(),
                                                 results,
                                                 got_exception));
  }

  // If all objects were fetched already, return. Note that we always need to
  // call UnblockIfNeeded() to cancel the get request.
  if (remaining.empty() || *got_exception) {
    return UnblockIfNeeded(raylet_client_, ctx);
  }

  // If not all objects were successfully fetched, repeatedly call FetchOrReconstruct
  // and Get from the local object store in batches. This loop will run indefinitely
  // until the objects are all fetched if timeout is -1.
  bool should_break = false;
  bool timed_out = false;
  int64_t remaining_timeout = timeout_ms;
  auto fetch_start_time_ms = current_time_ms();
  while (!remaining.empty() && !should_break) {
    batch_ids.clear();
    for (const auto &id : remaining) {
      if (int64_t(batch_ids.size()) == batch_size) {
        break;
      }
      batch_ids.push_back(id);
    }

    int64_t batch_timeout = std::max(RayConfig::instance().get_timeout_milliseconds(),
                                     int64_t(10 * batch_ids.size()));
    if (remaining_timeout >= 0) {
      batch_timeout = std::min(remaining_timeout, batch_timeout);
      remaining_timeout -= batch_timeout;
      timed_out = remaining_timeout <= 0;
    }

    size_t previous_size = remaining.size();
    // This is a separate IPC from the FetchAndGet in direct call mode.
    if (ctx.CurrentTaskIsDirectCall() && ctx.ShouldReleaseResourcesOnBlockingCalls()) {
      RAY_RETURN_NOT_OK(raylet_client_->NotifyDirectCallTaskBlocked(
          /*release_resources_during_plasma_fetch=*/false));
    }
    RAY_RETURN_NOT_OK(FetchAndGetFromPlasmaStore(remaining,
                                                 batch_ids,
                                                 batch_timeout,
                                                 /*fetch_only=*/false,
                                                 ctx.CurrentTaskIsDirectCall(),
                                                 ctx.GetCurrentTaskID(),
                                                 results,
                                                 got_exception));
    should_break = timed_out || *got_exception;

    if ((previous_size - remaining.size()) < batch_ids.size()) {
      WarnIfFetchHanging(fetch_start_time_ms, remaining);
    }
    if (check_signals_) {
      Status status = check_signals_();
      if (!status.ok()) {
        // TODO(edoakes): in this case which status should we return?
        RAY_RETURN_NOT_OK(UnblockIfNeeded(raylet_client_, ctx));
        return status;
      }
    }
    if (RayConfig::instance().yield_plasma_lock_workaround() && !should_break &&
        remaining.size() > 0) {
      // Yield the plasma lock to other threads. This is a temporary workaround since we
      // are holding the lock for a long time, so it can easily starve inbound RPC
      // requests to Release() buffers which only require holding the lock for brief
      // periods. See https://github.com/ray-project/ray/pull/16402 for more context.
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  if (!remaining.empty() && timed_out) {
    RAY_RETURN_NOT_OK(UnblockIfNeeded(raylet_client_, ctx));
    return Status::TimedOut("Get timed out: some object(s) not ready.");
  }

  // Notify unblocked because we blocked when calling FetchOrReconstruct with
  // fetch_only=false.
  return UnblockIfNeeded(raylet_client_, ctx);
}

Status CoreWorkerPlasmaStoreProvider::Contains(const ObjectID &object_id,
                                               bool *has_object) {
  return store_client_.Contains(object_id, has_object);
}

Status CoreWorkerPlasmaStoreProvider::Wait(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int num_objects,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_set<ObjectID> *ready) {
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());

  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  while (!should_break) {
    WaitResultPair result_pair;
    int64_t call_timeout = RayConfig::instance().get_timeout_milliseconds();
    if (remaining_timeout >= 0) {
      call_timeout = std::min(remaining_timeout, call_timeout);
      remaining_timeout -= call_timeout;
      should_break = remaining_timeout <= 0;
    }

    // This is a separate IPC from the Wait in direct call mode.
    if (ctx.CurrentTaskIsDirectCall() && ctx.ShouldReleaseResourcesOnBlockingCalls()) {
      RAY_RETURN_NOT_OK(raylet_client_->NotifyDirectCallTaskBlocked(
          /*release_resources_during_plasma_fetch=*/false));
    }
    const auto owner_addresses = reference_counter_->GetOwnerAddresses(id_vector);
    RAY_RETURN_NOT_OK(
        raylet_client_->Wait(id_vector,
                             owner_addresses,
                             num_objects,
                             call_timeout,
                             /*mark_worker_blocked*/ !ctx.CurrentTaskIsDirectCall(),
                             ctx.GetCurrentTaskID(),
                             &result_pair));

    if (result_pair.first.size() >= static_cast<size_t>(num_objects)) {
      should_break = true;
    }
    for (const auto &entry : result_pair.first) {
      ready->insert(entry);
    }
    if (check_signals_) {
      RAY_RETURN_NOT_OK(check_signals_());
    }
  }
  if (ctx.CurrentTaskIsDirectCall() && ctx.ShouldReleaseResourcesOnBlockingCalls()) {
    RAY_RETURN_NOT_OK(raylet_client_->NotifyDirectCallTaskUnblocked());
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Delete(
    const absl::flat_hash_set<ObjectID> &object_ids, bool local_only) {
  std::vector<ObjectID> object_id_vector(object_ids.begin(), object_ids.end());
  return raylet_client_->FreeObjects(object_id_vector, local_only);
}

std::string CoreWorkerPlasmaStoreProvider::MemoryUsageString() {
  return store_client_.DebugString();
}

absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>>
CoreWorkerPlasmaStoreProvider::UsedObjectsList() const {
  return buffer_tracker_->UsedObjects();
}

void CoreWorkerPlasmaStoreProvider::WarnIfFetchHanging(
    int64_t fetch_start_time_ms, const absl::flat_hash_set<ObjectID> &remaining) {
  int64_t duration_ms = current_time_ms() - fetch_start_time_ms;
  if (duration_ms > RayConfig::instance().fetch_warn_timeout_milliseconds()) {
    std::ostringstream oss;
    size_t printed = 0;
    for (auto &id : remaining) {
      if (printed >=
          RayConfig::instance().object_store_get_max_ids_to_print_in_warning()) {
        break;
      }
      if (printed > 0) {
        oss << ", ";
      }
      oss << id.Hex();
      printed++;
    }
    if (printed < remaining.size()) {
      oss << ", etc";
    }
    RAY_LOG(WARNING)
        << "Objects " << oss.str() << " are still not local after "
        << (duration_ms / 1000) << "s. "
        << "If this message continues to print, ray.get() is likely hung. Please file an "
           "issue at https://github.com/ray-project/ray/issues/.";
  }
}

Status CoreWorkerPlasmaStoreProvider::WarmupStore() {
  ObjectID object_id = ObjectID::FromRandom();
  std::shared_ptr<Buffer> data;
  RAY_RETURN_NOT_OK(Create(nullptr,
                           8,
                           object_id,
                           rpc::Address(),
                           &data,
                           /*created_by_worker=*/true));
  RAY_RETURN_NOT_OK(Seal(object_id));
  RAY_RETURN_NOT_OK(Release(object_id));
  RAY_RETURN_NOT_OK(Delete({object_id}, true));
  return Status::OK();
}

}  // namespace core
}  // namespace ray
