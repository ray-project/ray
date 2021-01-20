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
#include "ray/util/asio_util.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet {

void LocalObjectManager::PinObjects(const std::vector<ObjectID> &object_ids,
                                    std::vector<std::unique_ptr<RayObject>> &&objects) {
  RAY_CHECK(object_pinning_enabled_);
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
            RAY_LOG(DEBUG) << "Worker failed. Unpinning object " << object_id;
          }
          ReleaseFreedObject(object_id);
        });
  }
}

void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  // object_pinning_enabled_ flag is off when the --lru-evict flag is on.
  if (object_pinning_enabled_) {
    RAY_LOG(DEBUG) << "Unpinning object " << object_id;
    // The object should be in one of these stats. pinned, spilling, or spilled.
    RAY_CHECK((pinned_objects_.count(object_id) > 0) ||
              (spilled_objects_url_.count(object_id) > 0) ||
              (objects_pending_spill_.count(object_id) > 0));
    if (automatic_object_deletion_enabled_) {
      spilled_object_pending_delete_.push(object_id);
    }
    pinned_objects_.erase(object_id);
  }

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
  if (object_pinning_enabled_ && automatic_object_deletion_enabled_) {
    // Deletion wouldn't work when the object pinning is not enabled.
    ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_);
  }
  last_free_objects_at_ms_ = current_time_ms();
}

void LocalObjectManager::FlushFreeObjectsIfNeeded(int64_t now_ms) {
  if (free_objects_period_ms_ > 0 &&
      static_cast<int64_t>(now_ms - last_free_objects_at_ms_) > free_objects_period_ms_) {
    FlushFreeObjects();
  }
}

void LocalObjectManager::SpillObjectUptoMaxThroughput() {
  if (RayConfig::instance().object_spilling_config().empty() ||
      !RayConfig::instance().automatic_object_spilling_enabled()) {
    return;
  }

  // Spill as fast as we can using all our spill workers.
  bool can_spill_more = true;
  while (can_spill_more) {
    if (!SpillObjectsOfSize(min_spilling_size_)) {
      break;
    }
    {
      absl::MutexLock lock(&mutex_);
      num_active_workers_ += 1;
      can_spill_more = num_active_workers_ < max_active_workers_;
    }
  }
}

bool LocalObjectManager::IsSpillingInProgress() {
  absl::MutexLock lock(&mutex_);
  return num_active_workers_ > 0;
}

bool LocalObjectManager::SpillObjectsOfSize(int64_t num_bytes_to_spill) {
  if (RayConfig::instance().object_spilling_config().empty() ||
      !RayConfig::instance().automatic_object_spilling_enabled()) {
    return false;
  }

  RAY_LOG(DEBUG) << "Choosing objects to spill of total size " << num_bytes_to_spill;
  int64_t bytes_to_spill = 0;
  auto it = pinned_objects_.begin();
  std::vector<ObjectID> objects_to_spill;
  while (bytes_to_spill <= num_bytes_to_spill && it != pinned_objects_.end()) {
    if (is_plasma_object_spillable_(it->first)) {
      bytes_to_spill += it->second->GetSize();
      objects_to_spill.push_back(it->first);
    }
    it++;
  }
  if (!objects_to_spill.empty()) {
    RAY_LOG(DEBUG) << "Spilling objects of total size " << bytes_to_spill
                   << " num objects " << objects_to_spill.size();
    auto start_time = absl::GetCurrentTimeNanos();
    SpillObjectsInternal(objects_to_spill, [this, bytes_to_spill, objects_to_spill,
                                            start_time](const Status &status) {
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Error spilling objects " << status.ToString();
      } else {
        auto now = absl::GetCurrentTimeNanos();
        RAY_LOG(DEBUG) << "Spilled " << bytes_to_spill << " bytes in "
                       << (now - start_time) / 1e6 << "ms";
        spilled_bytes_total_ += bytes_to_spill;
        spilled_objects_total_ += objects_to_spill.size();
        // Adjust throughput timing to account for concurrent spill operations.
        spill_time_total_s_ += (now - std::max(start_time, last_spill_finish_ns_)) / 1e9;
        if (now - last_spill_log_ns_ > 1e9) {
          last_spill_log_ns_ = now;
          RAY_LOG(INFO) << "Spilled "
                        << static_cast<int>(spilled_bytes_total_ / (1024 * 1024))
                        << " MiB, " << spilled_objects_total_
                        << " objects, write throughput "
                        << static_cast<int>(spilled_bytes_total_ / (1024 * 1024) /
                                            spill_time_total_s_)
                        << " MiB/s";
        }
        last_spill_finish_ns_ = now;
      }
    });
    return true;
  }
  return false;
}

void LocalObjectManager::SpillObjects(const std::vector<ObjectID> &object_ids,
                                      std::function<void(const ray::Status &)> callback) {
  SpillObjectsInternal(object_ids, callback);
}

void LocalObjectManager::SpillObjectsInternal(
    const std::vector<ObjectID> &object_ids,
    std::function<void(const ray::Status &)> callback) {
  std::vector<ObjectID> objects_to_spill;
  // Filter for the objects that can be spilled.
  for (const auto &id : object_ids) {
    // We should not spill an object that we are not the primary copy for, or
    // objects that are already being spilled.
    if (pinned_objects_.count(id) == 0 && objects_pending_spill_.count(id) == 0) {
      if (callback) {
        callback(
            Status::Invalid("Requested spill for object that is not marked as "
                            "the primary copy."));
      }
      return;
    }

    // Add objects that we are the primary copy for, and that we are not
    // already spilling.
    auto it = pinned_objects_.find(id);
    if (it != pinned_objects_.end()) {
      RAY_LOG(DEBUG) << "Spilling object " << id;
      objects_to_spill.push_back(id);
      num_bytes_pending_spill_ += it->second->GetSize();
      objects_pending_spill_[id] = std::move(it->second);
      pinned_objects_.erase(it);
    }
  }

  if (objects_to_spill.empty()) {
    if (callback) {
      callback(Status::Invalid("All objects are already being spilled."));
    }
    return;
  }
  io_worker_pool_.PopSpillWorker(
      [this, objects_to_spill, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::SpillObjectsRequest request;
        for (const auto &object_id : objects_to_spill) {
          RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
          request.add_object_ids_to_spill(object_id.Binary());
        }
        io_worker->rpc_client()->SpillObjects(
            request, [this, objects_to_spill, callback, io_worker](
                         const ray::Status &status, const rpc::SpillObjectsReply &r) {
              {
                absl::MutexLock lock(&mutex_);
                num_active_workers_ -= 1;
              }
              io_worker_pool_.PushSpillWorker(io_worker);
              if (!status.ok()) {
                for (const auto &object_id : objects_to_spill) {
                  auto it = objects_pending_spill_.find(object_id);
                  RAY_CHECK(it != objects_pending_spill_.end());
                  pinned_objects_.emplace(object_id, std::move(it->second));
                  objects_pending_spill_.erase(it);
                }

                RAY_LOG(ERROR) << "Failed to send object spilling request: "
                               << status.ToString();
                if (callback) {
                  callback(status);
                }
              } else {
                AddSpilledUrls(objects_to_spill, r, callback);
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
        object_id, object_url,
        [this, object_id, object_url, callback, num_remaining](Status status) {
          RAY_CHECK_OK(status);
          // Unpin the object.
          auto it = objects_pending_spill_.find(object_id);
          RAY_CHECK(it != objects_pending_spill_.end());
          num_bytes_pending_spill_ -= it->second->GetSize();
          objects_pending_spill_.erase(it);

          // Update the object_id -> url_ref_count to use it for deletion later.
          // We need to track the references here because a single file can contain
          // multiple objects, and we shouldn't delete the file until
          // all the objects are gone out of scope.
          // object_url is equivalent to url_with_offset.
          auto parsed_url = ParseURL(object_url);
          const auto base_url_it = parsed_url->find("url");
          RAY_CHECK(base_url_it != parsed_url->end());
          if (!url_ref_count_.contains(base_url_it->second)) {
            url_ref_count_[base_url_it->second] = 1;
          } else {
            url_ref_count_[base_url_it->second] += 1;
          }
          spilled_objects_url_.emplace(object_id, object_url);

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
  if (objects_pending_restore_.count(object_id) > 0) {
    // If the same object is restoring, we dedup here.
    return;
  }
  RAY_CHECK(objects_pending_restore_.emplace(object_id).second)
      << "Object dedupe wasn't done properly. Please report if you see this issue.";
  io_worker_pool_.PopRestoreWorker([this, object_id, object_url, callback](
                                       std::shared_ptr<WorkerInterface> io_worker) {
    auto start_time = absl::GetCurrentTimeNanos();
    RAY_LOG(DEBUG) << "Sending restore spilled object request";
    rpc::RestoreSpilledObjectsRequest request;
    request.add_spilled_objects_url(std::move(object_url));
    request.add_object_ids_to_restore(object_id.Binary());
    io_worker->rpc_client()->RestoreSpilledObjects(
        request,
        [this, start_time, object_id, callback, io_worker](
            const ray::Status &status, const rpc::RestoreSpilledObjectsReply &r) {
          io_worker_pool_.PushRestoreWorker(io_worker);
          objects_pending_restore_.erase(object_id);
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send restore spilled object request: "
                           << status.ToString();
          } else {
            auto now = absl::GetCurrentTimeNanos();
            auto restored_bytes = r.bytes_restored_total();
            RAY_LOG(DEBUG) << "Restored " << restored_bytes << " in "
                           << (now - start_time) / 1e6 << "ms. Object id:" << object_id;
            restored_bytes_total_ += restored_bytes;
            restored_objects_total_ += 1;
            // Adjust throughput timing to account for concurrent restore operations.
            restore_time_total_s_ +=
                (now - std::max(start_time, last_restore_finish_ns_)) / 1e9;
            if (now - last_restore_log_ns_ > 1e9) {
              last_restore_log_ns_ = now;
              RAY_LOG(INFO) << "Restored "
                            << static_cast<int>(restored_bytes_total_ / (1024 * 1024))
                            << " MiB, " << restored_objects_total_
                            << " objects, read throughput "
                            << static_cast<int>(restored_bytes_total_ / (1024 * 1024) /
                                                restore_time_total_s_)
                            << " MiB/s";
            }
            last_restore_finish_ns_ = now;
          }
          if (callback) {
            callback(status);
          }
        });
  });
}

void LocalObjectManager::ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size) {
  std::vector<std::string> object_urls_to_delete;

  // Process upto batch size of objects to delete.
  while (!spilled_object_pending_delete_.empty() &&
         object_urls_to_delete.size() < max_batch_size) {
    auto &object_id = spilled_object_pending_delete_.front();
    // If the object is still spilling, do nothing. This will block other entries to be
    // processed, but it should be fine because the spilling will be eventually done, and
    // deleting objects is the low priority tasks.
    // This will instead enable simpler logic after this block.
    if (objects_pending_spill_.contains(object_id)) {
      break;
    }

    // Object id is either spilled or not spilled at this point.
    const auto spilled_objects_url_it = spilled_objects_url_.find(object_id);
    if (spilled_objects_url_it != spilled_objects_url_.end()) {
      // If the object was spilled, see if we can delete it. We should first check the ref
      // count.
      std::string &object_url = spilled_objects_url_it->second;
      // Note that here, we need to parse the object url to obtain the base_url.
      auto parsed_url = ParseURL(object_url);
      const auto base_url_it = parsed_url->find("url");
      RAY_CHECK(base_url_it != parsed_url->end());
      const auto &url_ref_count_it = url_ref_count_.find(base_url_it->second);
      RAY_CHECK(url_ref_count_it != url_ref_count_.end())
          << "url_ref_count_ should exist when spilled_objects_url_ exists. Please "
             "submit a Github issue if you see this error.";
      url_ref_count_it->second -= 1;

      // If there's no more refs, delete the object.
      if (url_ref_count_it->second == 0) {
        url_ref_count_.erase(url_ref_count_it);
        object_urls_to_delete.emplace_back(object_url);
      }
      spilled_objects_url_.erase(spilled_objects_url_it);
    }
    spilled_object_pending_delete_.pop();
  }
  if (object_urls_to_delete.size() > 0) {
    DeleteSpilledObjects(object_urls_to_delete);
  }
}

void LocalObjectManager::DeleteSpilledObjects(std::vector<std::string> &urls_to_delete) {
  io_worker_pool_.PopDeleteWorker(
      [this, urls_to_delete](std::shared_ptr<WorkerInterface> io_worker) {
        RAY_LOG(DEBUG) << "Sending delete spilled object request. Length: "
                       << urls_to_delete.size();
        rpc::DeleteSpilledObjectsRequest request;
        for (const auto &url : urls_to_delete) {
          request.add_spilled_objects_url(std::move(url));
        }
        io_worker->rpc_client()->DeleteSpilledObjects(
            request, [this, io_worker](const ray::Status &status,
                                       const rpc::DeleteSpilledObjectsReply &reply) {
              io_worker_pool_.PushDeleteWorker(io_worker);
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send delete spilled object request: "
                               << status.ToString();
              }
            });
      });
}

void LocalObjectManager::FillObjectSpillingStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_spill_time_total_s(spill_time_total_s_);
  stats->set_spilled_bytes_total(spilled_bytes_total_);
  stats->set_spilled_objects_total(spilled_objects_total_);
  stats->set_restore_time_total_s(restore_time_total_s_);
  stats->set_restored_bytes_total(restored_bytes_total_);
  stats->set_restored_objects_total(restored_objects_total_);
}

};  // namespace raylet

};  // namespace ray
