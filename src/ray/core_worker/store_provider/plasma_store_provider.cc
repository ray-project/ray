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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/raylet_ipc_client/raylet_ipc_client_interface.h"
#include "ray/util/time.h"
#include "src/ray/protobuf/common.pb.h"

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
    const std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client,
    ReferenceCounterInterface &reference_counter,
    std::function<Status()> check_signals,
    bool warmup,
    std::shared_ptr<plasma::PlasmaClientInterface> store_client,
    int64_t fetch_batch_size,
    std::function<std::string()> get_current_call_site)
    : raylet_ipc_client_(raylet_ipc_client),
      store_client_(std::move(store_client)),
      reference_counter_(reference_counter),
      check_signals_(std::move(check_signals)),
      fetch_batch_size_(fetch_batch_size) {
  if (get_current_call_site != nullptr) {
    get_current_call_site_ = get_current_call_site;
  } else {
    get_current_call_site_ = []() { return "<no callsite callback>"; };
  }
  object_store_full_delay_ms_ = RayConfig::instance().object_store_full_delay_ms();
  buffer_tracker_ = std::make_shared<BufferTracker>();
  if (!store_socket.empty()) {
    RAY_CHECK(store_client_ != nullptr) << "Plasma client must be provided";
    RAY_CHECK_OK(store_client_->Connect(store_socket));
  }
  if (warmup) {
    RAY_CHECK_OK(WarmupStore());
  }
}

CoreWorkerPlasmaStoreProvider::~CoreWorkerPlasmaStoreProvider() {
  store_client_->Disconnect();
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
                                             bool created_by_worker,
                                             bool is_mutable) {
  const auto source = created_by_worker
                          ? plasma::flatbuf::ObjectSource::CreatedByWorker
                          : plasma::flatbuf::ObjectSource::RestoredFromStorage;
  Status status =
      store_client_->CreateAndSpillIfNeeded(object_id,
                                            owner_address,
                                            is_mutable,
                                            data_size,
                                            metadata ? metadata->Data() : nullptr,
                                            metadata ? metadata->Size() : 0,
                                            data,
                                            source,
                                            /*device_num=*/0);

  if (status.IsObjectStoreFull()) {
    StatusOr<std::string> memory_usage = GetMemoryUsage();
    RAY_CHECK_OK(memory_usage.status()) << "Unable to communicate with the Plasma Store.";
    RAY_LOG(ERROR) << "Failed to put object " << object_id
                   << " in object store because it "
                   << "is full. Object size is " << data_size << " bytes.\n"
                   << "Plasma store status:\n"
                   << memory_usage.value() << "\n---\n"
                   << "--- Tip: Use the `ray memory` command to list active objects "
                      "in the cluster."
                   << "\n---\n";

    // Replace the status with a more helpful error message.
    std::ostringstream message;
    message << "Failed to put object " << object_id << " in object store because it "
            << "is full. Object size is " << data_size << " bytes.";
    status = Status::ObjectStoreFull(message.str());
  } else if (status.IsObjectExists()) {
    RAY_LOG_EVERY_MS(WARNING, 5000)
        << "Trying to put an object that already existed in plasma: " << object_id << ".";
    status = Status::OK();
  }
  return status;
}

Status CoreWorkerPlasmaStoreProvider::Seal(const ObjectID &object_id) {
  return store_client_->Seal(object_id);
}

Status CoreWorkerPlasmaStoreProvider::Release(const ObjectID &object_id) {
  return store_client_->Release(object_id);
}

Status CoreWorkerPlasmaStoreProvider::PullObjectsAndGetFromPlasmaStore(
    absl::flat_hash_set<ObjectID> &remaining,
    const std::vector<ObjectID> &batch_ids,
    int64_t timeout_ms,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  const auto owner_addresses = reference_counter_.GetOwnerAddresses(batch_ids);
  RAY_RETURN_NOT_OK(raylet_ipc_client_->AsyncGetObjects(batch_ids, owner_addresses));

  std::vector<plasma::ObjectBuffer> plasma_results;
  RAY_RETURN_NOT_OK(store_client_->Get(batch_ids, timeout_ms, &plasma_results));

  // Add successfully retrieved objects to the result map and remove them from
  // the set of IDs to get.
  for (size_t i = 0; i < plasma_results.size(); i++) {
    if (plasma_results[i].data != nullptr || plasma_results[i].metadata != nullptr) {
      const auto &object_id = batch_ids[i];
      std::shared_ptr<TrackedBuffer> data = nullptr;
      std::shared_ptr<Buffer> metadata = nullptr;
      if (plasma_results[i].data && plasma_results[i].data->Size() > 0) {
        // We track the set of active data buffers in active_buffers_. On destruction,
        // the buffer entry will be removed from the set via callback.
        data = std::make_shared<TrackedBuffer>(
            std::move(plasma_results[i].data), buffer_tracker_, object_id);
        buffer_tracker_->Record(object_id, data.get(), get_current_call_site_());
      }
      if (plasma_results[i].metadata && plasma_results[i].metadata->Size() > 0) {
        metadata = std::move(plasma_results[i].metadata);
      }
      auto result_object = std::make_shared<RayObject>(
          data, metadata, std::vector<rpc::ObjectReference>());
      remaining.erase(object_id);
      if (result_object->IsException()) {
        RAY_CHECK(!result_object->IsInPlasmaError());
        *got_exception = true;
      }
      (*results)[object_id] = std::move(result_object);
    }
  }

  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::GetIfLocal(
    const std::vector<ObjectID> &object_ids,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  std::vector<plasma::ObjectBuffer> plasma_results;
  RAY_RETURN_NOT_OK(store_client_->Get(object_ids, /*timeout_ms=*/0, &plasma_results));

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
      auto result_object = std::make_shared<RayObject>(
          data, metadata, std::vector<rpc::ObjectReference>());
      (*results)[object_id] = std::move(result_object);
    }
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::GetExperimentalMutableObject(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> *mutable_object) {
  return store_client_->GetExperimentalMutableObject(object_id, mutable_object);
}

Status UnblockIfNeeded(
    const std::shared_ptr<ipc::RayletIpcClientInterface> &raylet_client,
    const WorkerContext &ctx) {
  if (ctx.CurrentTaskIsDirectCall()) {
    // NOTE: for direct call actors, we still need to issue an unblock IPC to release
    // get subscriptions, even if the worker isn't blocked.
    if (ctx.ShouldReleaseResourcesOnBlockingCalls() || ctx.CurrentActorIsDirectCall()) {
      return raylet_client->NotifyWorkerUnblocked();
    } else {
      return Status::OK();  // We don't need to release resources.
    }
  } else {
    return raylet_client->CancelGetRequest();
  }
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  std::vector<ObjectID> batch_ids;
  absl::flat_hash_set<ObjectID> remaining(object_ids.begin(), object_ids.end());

  // Send initial requests to pull all objects in parallel.
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  int64_t total_size = static_cast<int64_t>(object_ids.size());
  for (int64_t start = 0; start < total_size; start += fetch_batch_size_) {
    batch_ids.clear();
    for (int64_t i = start; i < start + fetch_batch_size_ && i < total_size; i++) {
      batch_ids.push_back(id_vector[i]);
    }
    RAY_RETURN_NOT_OK(
        PullObjectsAndGetFromPlasmaStore(remaining,
                                         batch_ids,
                                         /*timeout_ms=*/0,
                                         // Mutable objects must be local before ray.get.
                                         results,
                                         got_exception));
  }

  // If all objects were fetched already, return. Note that we always need to
  // call UnblockIfNeeded() to cancel the get request.
  if (remaining.empty() || *got_exception) {
    return UnblockIfNeeded(raylet_ipc_client_, ctx);
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
      if (static_cast<int64_t>(batch_ids.size()) == fetch_batch_size_) {
        break;
      }
      batch_ids.push_back(id);
    }

    int64_t batch_timeout =
        std::max(RayConfig::instance().get_check_signal_interval_milliseconds(),
                 static_cast<int64_t>(10 * batch_ids.size()));
    if (remaining_timeout >= 0) {
      batch_timeout = std::min(remaining_timeout, batch_timeout);
      remaining_timeout -= batch_timeout;
      timed_out = remaining_timeout <= 0;
    }

    size_t previous_size = remaining.size();
    RAY_RETURN_NOT_OK(PullObjectsAndGetFromPlasmaStore(
        remaining, batch_ids, batch_timeout, results, got_exception));
    should_break = timed_out || *got_exception;

    if ((previous_size - remaining.size()) < batch_ids.size()) {
      WarnIfFetchHanging(fetch_start_time_ms, remaining);
    }
    if (check_signals_) {
      Status status = check_signals_();
      if (!status.ok()) {
        // TODO(edoakes): in this case which status should we return?
        RAY_RETURN_NOT_OK(UnblockIfNeeded(raylet_ipc_client_, ctx));
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
    RAY_RETURN_NOT_OK(UnblockIfNeeded(raylet_ipc_client_, ctx));
    return Status::TimedOut("Get timed out: some object(s) not ready.");
  }

  // Notify unblocked because we blocked when calling FetchOrReconstruct with
  // fetch_only=false.
  return UnblockIfNeeded(raylet_ipc_client_, ctx);
}

Status CoreWorkerPlasmaStoreProvider::Contains(const ObjectID &object_id,
                                               bool *has_object) {
  return store_client_->Contains(object_id, has_object);
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
  absl::flat_hash_set<ObjectID> ready_in_plasma;
  while (!should_break) {
    int64_t call_timeout = RayConfig::instance().get_check_signal_interval_milliseconds();
    if (remaining_timeout >= 0) {
      call_timeout = std::min(remaining_timeout, call_timeout);
      remaining_timeout -= call_timeout;
      should_break = remaining_timeout <= 0;
    }

    const auto owner_addresses = reference_counter_.GetOwnerAddresses(id_vector);
    RAY_ASSIGN_OR_RETURN(
        ready_in_plasma,
        raylet_ipc_client_->Wait(id_vector, owner_addresses, num_objects, call_timeout));

    if (ready_in_plasma.size() >= static_cast<size_t>(num_objects)) {
      should_break = true;
    }
    if (check_signals_) {
      RAY_RETURN_NOT_OK(check_signals_());
    }
  }
  for (const auto &entry : ready_in_plasma) {
    ready->insert(entry);
  }
  if (ctx.CurrentTaskIsDirectCall() && ctx.ShouldReleaseResourcesOnBlockingCalls()) {
    RAY_RETURN_NOT_OK(raylet_ipc_client_->NotifyWorkerUnblocked());
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Delete(
    const absl::flat_hash_set<ObjectID> &object_ids, bool local_only) {
  std::vector<ObjectID> object_id_vector(object_ids.begin(), object_ids.end());
  return raylet_ipc_client_->FreeObjects(object_id_vector, local_only);
}

StatusOr<std::string> CoreWorkerPlasmaStoreProvider::GetMemoryUsage() {
  return store_client_->GetMemoryUsage();
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
