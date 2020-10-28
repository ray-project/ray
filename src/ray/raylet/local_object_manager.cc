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

#include "ray/raylet/local_object_manager.h"

namespace ray {

namespace raylet {

void LocalObjectManager::PinObjects(const std::vector<ObjectID> &object_ids,
                                    std::vector<std::unique_ptr<RayObject>> &&objects) {
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    auto &object = objects[i];
    if (object == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }
    RAY_LOG(DEBUG) << "Pinning object " << object_id;
    pinned_objects_.emplace(object_id, std::move(object));
  }
}

void LocalObjectManager::WaitForObjectFree(const rpc::Address &owner_address,
                                           const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    // Send a long-running RPC request to the owner for each object. When we get a
    // response or the RPC fails (due to the owner crashing), unpin the object.
    // TODO(edoakes): we should be batching these requests instead of sending one per
    // pinned object.
    rpc::WaitForObjectEvictionRequest wait_request;
    wait_request.set_object_id(object_id.Binary());
    wait_request.set_intended_worker_id(owner_address.worker_id());
    auto owner_client = owner_client_pool_.GetOrConnect(owner_address);
    owner_client->WaitForObjectEviction(
        wait_request,
        [this, object_id](Status status, const rpc::WaitForObjectEvictionReply &reply) {
          if (!status.ok()) {
            RAY_LOG(WARNING) << "Worker failed. Unpinning object " << object_id;
          }
          ReleaseFreedObject(object_id);
        });
  }
}

void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "Unpinning object " << object_id;
  pinned_objects_.erase(object_id);

  // Try to evict all copies of the object from the cluster.
  if (free_objects_period_ms_ >= 0) {
    objects_to_free_.push_back(object_id);
  }
  if (objects_to_free_.size() == free_objects_batch_size_ ||
      free_objects_period_ms_ == 0) {
    FlushFreeObjects();
  }
}

void LocalObjectManager::FlushFreeObjects() {
  if (!objects_to_free_.empty()) {
    RAY_LOG(DEBUG) << "Freeing " << objects_to_free_.size() << " out-of-scope objects";
    on_objects_freed_(objects_to_free_);
    objects_to_free_.clear();
  }
  last_free_objects_at_ms_ = current_time_ms();
}

void LocalObjectManager::FlushFreeObjectsIfNeeded(int64_t now_ms) {
  if (free_objects_period_ms_ > 0 &&
      static_cast<int64_t>(now_ms - last_free_objects_at_ms_) > free_objects_period_ms_) {
    FlushFreeObjects();
  }
}

void LocalObjectManager::SpillObjects(const std::vector<ObjectID> &object_ids,
                                      std::function<void(const ray::Status &)> callback) {
  for (const auto &id : object_ids) {
    // We should not spill an object that we are not the primary copy for.
    if (pinned_objects_.count(id) == 0) {
      callback(
          Status::Invalid("Requested spill for object that is not marked as "
                          "the primary copy."));
    }
  }

  io_worker_pool_.PopIOWorker(
      [this, object_ids, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::SpillObjectsRequest request;
        for (const auto &object_id : object_ids) {
          RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
          request.add_object_ids_to_spill(object_id.Binary());
        }
        io_worker->rpc_client()->SpillObjects(
            request, [this, object_ids, callback, io_worker](
                         const ray::Status &status, const rpc::SpillObjectsReply &r) {
              io_worker_pool_.PushIOWorker(io_worker);
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send object spilling request: "
                               << status.ToString();
                if (callback) {
                  callback(status);
                }
              } else {
                AddSpilledUrls(object_ids, r, callback);
              }
            });
      });
}

void LocalObjectManager::AddSpilledUrls(
    const std::vector<ObjectID> &object_ids, const rpc::SpillObjectsReply &worker_reply,
    std::function<void(const ray::Status &)> callback) {
  auto num_remaining = std::make_shared<size_t>(object_ids.size());
  for (size_t i = 0; i < object_ids.size(); ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.spilled_objects_url(i);
    RAY_LOG(DEBUG) << "Object " << object_id << " spilled at " << object_url;
    // Write to object directory. Wait for the write to finish before
    // releasing the object to make sure that the spilled object can
    // be retrieved by other raylets.
    RAY_CHECK_OK(object_info_accessor_.AsyncAddSpilledUrl(
        object_id, object_url, [this, object_id, callback, num_remaining](Status status) {
          RAY_CHECK_OK(status);
          // Unpin the object.
          // NOTE(swang): Due to a race condition, the object may not be in
          // the map yet. In that case, the owner will respond to the
          // WaitForObjectEvictionRequest and we will unpin the object
          // then.
          pinned_objects_.erase(object_id);
          (*num_remaining)--;
          if (*num_remaining == 0 && callback) {
            callback(status);
          }
        }));
  }
}

void LocalObjectManager::AsyncRestoreSpilledObject(
    const ObjectID &object_id, const std::string &object_url,
    std::function<void(const ray::Status &)> callback) {
  RAY_LOG(DEBUG) << "Restoring spilled object " << object_id << " from URL "
                 << object_url;
  io_worker_pool_.PopIOWorker([this, object_url,
                               callback](std::shared_ptr<WorkerInterface> io_worker) {
    RAY_LOG(DEBUG) << "Sending restore spilled object request";
    rpc::RestoreSpilledObjectsRequest request;
    request.add_spilled_objects_url(std::move(object_url));
    io_worker->rpc_client()->RestoreSpilledObjects(
        request, [this, callback, io_worker](const ray::Status &status,
                                             const rpc::RestoreSpilledObjectsReply &r) {
          io_worker_pool_.PushIOWorker(io_worker);
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send restore spilled object request: "
                           << status.ToString();
          }
          if (callback) {
            callback(status);
          }
        });
  });
}

};  // namespace raylet

};  // namespace ray
