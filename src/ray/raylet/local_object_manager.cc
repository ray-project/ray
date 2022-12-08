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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet {

void LocalObjectManager::PinObjectsAndWaitForFree(
    const std::vector<ObjectID> &object_ids,
    std::vector<std::unique_ptr<RayObject>> &&objects,
    const rpc::Address &owner_address,
    const ObjectID &generator_id) {
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    auto &object = objects[i];
    if (object == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }

    const auto inserted = local_objects_.emplace(
        object_id, LocalObjectInfo(owner_address, generator_id, object->GetSize()));
    if (inserted.second) {
      // This is the first time we're pinning this object.
      RAY_LOG(DEBUG) << "Pinning object " << object_id;
      pinned_objects_size_ += object->GetSize();
      pinned_objects_.emplace(object_id, std::move(object));
    } else {
      auto original_worker_id =
          WorkerID::FromBinary(inserted.first->second.owner_address.worker_id());
      auto new_worker_id = WorkerID::FromBinary(owner_address.worker_id());
      if (original_worker_id != new_worker_id) {
        // TODO(swang): Handle this case. We should use the new owner address
        // and object copy.
        RAY_LOG(WARNING)
            << "Received PinObjects request from a different owner " << new_worker_id
            << " from the original " << original_worker_id << ". Object " << object_id
            << " may get freed while the new owner still has the object in scope.";
      }
      continue;
    }

    // Create a object eviction subscription message.
    auto wait_request = std::make_unique<rpc::WorkerObjectEvictionSubMessage>();
    wait_request->set_object_id(object_id.Binary());
    wait_request->set_intended_worker_id(owner_address.worker_id());
    if (!generator_id.IsNil()) {
      wait_request->set_generator_id(generator_id.Binary());
    }
    rpc::Address subscriber_address;
    subscriber_address.set_raylet_id(self_node_id_.Binary());
    subscriber_address.set_ip_address(self_node_address_);
    subscriber_address.set_port(self_node_port_);
    wait_request->mutable_subscriber_address()->CopyFrom(subscriber_address);

    // If the subscription succeeds, register the subscription callback.
    // Callback is invoked when the owner publishes the object to evict.
    auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
      RAY_CHECK(msg.has_worker_object_eviction_message());
      const auto object_eviction_msg = msg.worker_object_eviction_message();
      const auto object_id = ObjectID::FromBinary(object_eviction_msg.object_id());
      ReleaseFreedObject(object_id);
      core_worker_subscriber_->Unsubscribe(
          rpc::ChannelType::WORKER_OBJECT_EVICTION, owner_address, object_id.Binary());
    };

    // Callback that is invoked when the owner of the object id is dead.
    auto owner_dead_callback = [this, owner_address](const std::string &object_id_binary,
                                                     const Status &) {
      const auto object_id = ObjectID::FromBinary(object_id_binary);
      ReleaseFreedObject(object_id);
    };

    auto sub_message = std::make_unique<rpc::SubMessage>();
    sub_message->mutable_worker_object_eviction_message()->Swap(wait_request.get());

    RAY_CHECK(core_worker_subscriber_->Subscribe(std::move(sub_message),
                                                 rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                                 owner_address,
                                                 object_id.Binary(),
                                                 /*subscribe_done_callback=*/nullptr,
                                                 subscription_callback,
                                                 owner_dead_callback));
  }
}

void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  // Only free the object if it is not already freed.
  auto it = local_objects_.find(object_id);
  if (it == local_objects_.end() || it->second.is_freed) {
    return;
  }
  // Mark the object as freed. NOTE(swang): We have to mark this instead of
  // deleting the entry immediately in case the object is currently being
  // spilled. In that case, we should process the free event once the object
  // spill is complete.
  it->second.is_freed = true;

  RAY_LOG(DEBUG) << "Unpinning object " << object_id;
  // The object should be in one of these states: pinned, spilling, or spilled.
  RAY_CHECK((pinned_objects_.count(object_id) > 0) ||
            (spilled_objects_url_.count(object_id) > 0) ||
            (objects_pending_spill_.count(object_id) > 0));
  if (pinned_objects_.count(object_id)) {
    pinned_objects_size_ -= pinned_objects_[object_id]->GetSize();
    pinned_objects_.erase(object_id);
    local_objects_.erase(it);
  } else {
    // If the object is being spilled or is already spilled, then we will clean
    // up the local_objects_ entry once the spilled copy has been
    // freed.
    spilled_object_pending_delete_.push(object_id);
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
  ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_);
  last_free_objects_at_ms_ = current_time_ms();
}

void LocalObjectManager::SpillObjectUptoMaxThroughput() {
  if (RayConfig::instance().object_spilling_config().empty()) {
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
      can_spill_more = num_active_workers_ < max_active_workers_;
    }
  }
}

bool LocalObjectManager::IsSpillingInProgress() {
  absl::MutexLock lock(&mutex_);
  return num_active_workers_ > 0;
}

bool LocalObjectManager::SpillObjectsOfSize(int64_t num_bytes_to_spill) {
  if (RayConfig::instance().object_spilling_config().empty()) {
    return false;
  }

  RAY_LOG(DEBUG) << "Choosing objects to spill of total size " << num_bytes_to_spill;
  int64_t bytes_to_spill = 0;
  auto it = pinned_objects_.begin();
  std::vector<ObjectID> objects_to_spill;
  int64_t counts = 0;
  while (it != pinned_objects_.end() && counts < max_fused_object_count_) {
    if (is_plasma_object_spillable_(it->first)) {
      bytes_to_spill += it->second->GetSize();
      objects_to_spill.push_back(it->first);
    }
    it++;
    counts += 1;
  }
  if (objects_to_spill.empty()) {
    return false;
  }

  if (it == pinned_objects_.end() && bytes_to_spill < num_bytes_to_spill &&
      !objects_pending_spill_.empty()) {
    // We have gone through all spillable objects but we have not yet reached
    // the minimum bytes to spill and we are already spilling other objects.
    // Let those spill requests finish before we try to spill the current
    // objects. This gives us some time to decide whether we really need to
    // spill the current objects or if we can afford to wait for additional
    // objects to fuse with.
    return false;
  }
  RAY_LOG(DEBUG) << "Spilling objects of total size " << bytes_to_spill << " num objects "
                 << objects_to_spill.size();
  auto start_time = absl::GetCurrentTimeNanos();
  SpillObjectsInternal(
      objects_to_spill,
      [this, bytes_to_spill, objects_to_spill, start_time](const Status &status) {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Failed to spill objects: " << status.ToString();
        } else {
          auto now = absl::GetCurrentTimeNanos();
          RAY_LOG(DEBUG) << "Spilled " << bytes_to_spill << " bytes in "
                         << (now - start_time) / 1e6 << "ms";
          // Adjust throughput timing to account for concurrent spill operations.
          spill_time_total_s_ +=
              (now - std::max(start_time, last_spill_finish_ns_)) / 1e9;
          if (now - last_spill_log_ns_ > 1e9) {
            last_spill_log_ns_ = now;
            std::stringstream msg;
            // Keep :info_message: in sync with LOG_PREFIX_INFO_MESSAGE in
            // ray_constants.py.
            msg << ":info_message:Spilled "
                << static_cast<int>(spilled_bytes_total_ / (1024 * 1024)) << " MiB, "
                << spilled_objects_total_ << " objects, write throughput "
                << static_cast<int>(spilled_bytes_total_ / (1024 * 1024) /
                                    spill_time_total_s_)
                << " MiB/s.";
            if (next_spill_error_log_bytes_ > 0 &&
                spilled_bytes_total_ >= next_spill_error_log_bytes_) {
              // Add an advisory the first time this is logged.
              if (next_spill_error_log_bytes_ ==
                  RayConfig::instance().verbose_spill_logs()) {
                msg << " Set RAY_verbose_spill_logs=0 to disable this message.";
              }
              // Exponential backoff on the spill messages.
              next_spill_error_log_bytes_ *= 2;
              RAY_LOG(ERROR) << msg.str();
            } else {
              RAY_LOG(INFO) << msg.str();
            }
          }
          last_spill_finish_ns_ = now;
        }
      });
  return true;
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

      // Move a pinned object to the pending spill object.
      auto object_size = it->second->GetSize();
      num_bytes_pending_spill_ += object_size;
      objects_pending_spill_[id] = std::move(it->second);

      pinned_objects_size_ -= object_size;
      pinned_objects_.erase(it);
    }
  }

  if (objects_to_spill.empty()) {
    if (callback) {
      callback(Status::Invalid("All objects are already being spilled."));
    }
    return;
  }
  {
    absl::MutexLock lock(&mutex_);
    num_active_workers_ += 1;
  }
  io_worker_pool_.PopSpillWorker(
      [this, objects_to_spill, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::SpillObjectsRequest request;
        std::vector<ObjectID> requested_objects_to_spill;
        for (const auto &object_id : objects_to_spill) {
          auto it = objects_pending_spill_.find(object_id);
          RAY_CHECK(it != objects_pending_spill_.end());
          auto freed_it = local_objects_.find(object_id);
          // If the object hasn't already been freed, spill it.
          if (freed_it == local_objects_.end() || freed_it->second.is_freed) {
            num_bytes_pending_spill_ -= it->second->GetSize();
            objects_pending_spill_.erase(it);
          } else {
            auto ref = request.add_object_refs_to_spill();
            ref->set_object_id(object_id.Binary());
            ref->mutable_owner_address()->CopyFrom(freed_it->second.owner_address);
            RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
            requested_objects_to_spill.push_back(object_id);
          }
        }

        if (request.object_refs_to_spill_size() == 0) {
          {
            absl::MutexLock lock(&mutex_);
            num_active_workers_ -= 1;
          }
          io_worker_pool_.PushSpillWorker(io_worker);
          callback(Status::OK());
          return;
        }

        io_worker->rpc_client()->SpillObjects(
            request,
            [this, requested_objects_to_spill, callback, io_worker](
                const ray::Status &status, const rpc::SpillObjectsReply &r) {
              {
                absl::MutexLock lock(&mutex_);
                num_active_workers_ -= 1;
              }
              io_worker_pool_.PushSpillWorker(io_worker);
              size_t num_objects_spilled = status.ok() ? r.spilled_objects_url_size() : 0;
              // Object spilling is always done in the order of the request.
              // For example, if an object succeeded, it'll guarentee that all objects
              // before this will succeed.
              RAY_CHECK(num_objects_spilled <= requested_objects_to_spill.size());
              for (size_t i = num_objects_spilled; i != requested_objects_to_spill.size();
                   ++i) {
                const auto &object_id = requested_objects_to_spill[i];
                auto it = objects_pending_spill_.find(object_id);
                RAY_CHECK(it != objects_pending_spill_.end());
                pinned_objects_size_ += it->second->GetSize();
                num_bytes_pending_spill_ -= it->second->GetSize();
                pinned_objects_.emplace(object_id, std::move(it->second));
                objects_pending_spill_.erase(it);
              }

              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send object spilling request: "
                               << status.ToString();
              } else {
                OnObjectSpilled(requested_objects_to_spill, r);
              }
              if (callback) {
                callback(status);
              }
            });
      });

  // Deleting spilled objects can fall behind when there is a lot
  // of concurrent spilling and object frees. Clear the queue here
  // if needed.
  if (spilled_object_pending_delete_.size() >= free_objects_batch_size_) {
    ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_);
  }
}

void LocalObjectManager::OnObjectSpilled(const std::vector<ObjectID> &object_ids,
                                         const rpc::SpillObjectsReply &worker_reply) {
  for (size_t i = 0; i < static_cast<size_t>(worker_reply.spilled_objects_url_size());
       ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.spilled_objects_url(i);
    RAY_LOG(DEBUG) << "Object " << object_id << " spilled at " << object_url;

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

    // Mark that the object is spilled and unpin the pending requests.
    spilled_objects_url_.emplace(object_id, object_url);
    RAY_LOG(DEBUG) << "Unpinning pending spill object " << object_id;
    auto it = objects_pending_spill_.find(object_id);
    RAY_CHECK(it != objects_pending_spill_.end());
    const auto object_size = it->second->GetSize();
    num_bytes_pending_spill_ -= object_size;
    objects_pending_spill_.erase(it);

    // Update the internal spill metrics
    spilled_bytes_total_ += object_size;
    spilled_bytes_current_ += object_size;
    spilled_objects_total_++;

    // Asynchronously Update the spilled URL.
    auto freed_it = local_objects_.find(object_id);
    if (freed_it == local_objects_.end() || freed_it->second.is_freed) {
      RAY_LOG(DEBUG) << "Spilled object already freed, skipping send of spilled URL to "
                        "object directory for object "
                     << object_id;
      continue;
    }
    const auto &worker_addr = freed_it->second.owner_address;
    object_directory_->ReportObjectSpilled(
        object_id,
        self_node_id_,
        worker_addr,
        object_url,
        freed_it->second.generator_id.value_or(ObjectID::Nil()),
        is_external_storage_type_fs_);
  }
}

std::string LocalObjectManager::GetLocalSpilledObjectURL(const ObjectID &object_id) {
  if (!is_external_storage_type_fs_) {
    // If the external storage is cloud storage like S3, returns the empty string.
    // In that case, the URL is supposed to be obtained by OBOD.
    return "";
  }
  auto entry = spilled_objects_url_.find(object_id);
  if (entry != spilled_objects_url_.end()) {
    return entry->second;
  } else {
    return "";
  }
}

void LocalObjectManager::AsyncRestoreSpilledObject(
    const ObjectID &object_id,
    int64_t object_size,
    const std::string &object_url,
    std::function<void(const ray::Status &)> callback) {
  if (objects_pending_restore_.count(object_id) > 0) {
    // If the same object is restoring, we dedup here.
    return;
  }

  RAY_CHECK(objects_pending_restore_.emplace(object_id).second)
      << "Object dedupe wasn't done properly. Please report if you see this issue.";
  num_bytes_pending_restore_ += object_size;
  io_worker_pool_.PopRestoreWorker([this, object_id, object_size, object_url, callback](
                                       std::shared_ptr<WorkerInterface> io_worker) {
    auto start_time = absl::GetCurrentTimeNanos();
    RAY_LOG(DEBUG) << "Sending restore spilled object request";
    rpc::RestoreSpilledObjectsRequest request;
    request.add_spilled_objects_url(std::move(object_url));
    request.add_object_ids_to_restore(object_id.Binary());
    io_worker->rpc_client()->RestoreSpilledObjects(
        request,
        [this, start_time, object_id, object_size, callback, io_worker](
            const ray::Status &status, const rpc::RestoreSpilledObjectsReply &r) {
          io_worker_pool_.PushRestoreWorker(io_worker);
          num_bytes_pending_restore_ -= object_size;
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
    // processed, but it should be fine because the spilling will be eventually done,
    // and deleting objects is the low priority tasks. This will instead enable simpler
    // logic after this block.
    if (objects_pending_spill_.contains(object_id)) {
      break;
    }

    // Object id is either spilled or not spilled at this point.
    const auto spilled_objects_url_it = spilled_objects_url_.find(object_id);
    if (spilled_objects_url_it != spilled_objects_url_.end()) {
      // If the object was spilled, see if we can delete it. We should first check the
      // ref count.
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
        RAY_LOG(DEBUG) << "The URL " << object_url
                       << " is deleted because the references are out of scope.";
        object_urls_to_delete.emplace_back(object_url);
      }
      spilled_objects_url_.erase(spilled_objects_url_it);

      // Update current spilled objects metrics
      RAY_CHECK(local_objects_.contains(object_id))
          << "local objects should contain the spilled object: " << object_id;
      spilled_bytes_current_ -= local_objects_.at(object_id).object_size;
    } else {
      // If the object was not spilled, it gets pinned again. Unpin here to
      // prevent a memory leak.
      pinned_objects_.erase(object_id);
    }
    local_objects_.erase(object_id);
    spilled_object_pending_delete_.pop();
  }
  if (object_urls_to_delete.size() > 0) {
    DeleteSpilledObjects(std::move(object_urls_to_delete));
  }
}

void LocalObjectManager::DeleteSpilledObjects(std::vector<std::string> urls_to_delete,
                                              int64_t num_retries) {
  io_worker_pool_.PopDeleteWorker(
      [this, urls_to_delete, num_retries](std::shared_ptr<WorkerInterface> io_worker) {
        RAY_LOG(DEBUG) << "Sending delete spilled object request. Length: "
                       << urls_to_delete.size();
        rpc::DeleteSpilledObjectsRequest request;
        for (const auto &url : urls_to_delete) {
          request.add_spilled_objects_url(std::move(url));
        }
        io_worker->rpc_client()->DeleteSpilledObjects(
            request,
            [this, urls_to_delete = std::move(urls_to_delete), num_retries, io_worker](
                const ray::Status &status, const rpc::DeleteSpilledObjectsReply &reply) {
              io_worker_pool_.PushDeleteWorker(io_worker);
              if (!status.ok()) {
                num_failed_deletion_requests_ += 1;
                RAY_LOG(ERROR) << "Failed to send delete spilled object request: "
                               << status.ToString() << ", retry count: " << num_retries;

                if (num_retries > 0) {
                  // retry failed requests.
                  io_service_.post(
                      [this, urls_to_delete = std::move(urls_to_delete), num_retries]() {
                        DeleteSpilledObjects(urls_to_delete, num_retries - 1);
                      },
                      "LocaObjectManager.RetryDeleteSpilledObjects");
                }
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
  stats->set_object_store_bytes_primary_copy(pinned_objects_size_);
}

void LocalObjectManager::RecordMetrics() const {
  /// Record Metrics.
  if (spilled_bytes_total_ != 0 && spill_time_total_s_ != 0) {
    ray::stats::STATS_spill_manager_throughput_mb.Record(
        spilled_bytes_total_ / 1024 / 1024 / spill_time_total_s_, "Spilled");
  }
  if (restored_bytes_total_ != 0 && restore_time_total_s_ != 0) {
    ray::stats::STATS_spill_manager_throughput_mb.Record(
        restored_bytes_total_ / 1024 / 1024 / restore_time_total_s_, "Restored");
  }
  ray::stats::STATS_spill_manager_objects.Record(pinned_objects_.size(), "Pinned");
  ray::stats::STATS_spill_manager_objects.Record(objects_pending_restore_.size(),
                                                 "PendingRestore");
  ray::stats::STATS_spill_manager_objects.Record(objects_pending_spill_.size(),
                                                 "PendingSpill");

  ray::stats::STATS_spill_manager_objects_bytes.Record(pinned_objects_size_, "Pinned");
  ray::stats::STATS_spill_manager_objects_bytes.Record(num_bytes_pending_spill_,
                                                       "PendingSpill");
  ray::stats::STATS_spill_manager_objects_bytes.Record(num_bytes_pending_restore_,
                                                       "PendingRestore");
  ray::stats::STATS_spill_manager_objects_bytes.Record(spilled_bytes_total_, "Spilled");
  ray::stats::STATS_spill_manager_objects_bytes.Record(restored_objects_total_,
                                                       "Restored");

  ray::stats::STATS_spill_manager_request_total.Record(spilled_objects_total_, "Spilled");
  ray::stats::STATS_spill_manager_request_total.Record(restored_objects_total_,
                                                       "Restored");

  ray::stats::STATS_object_store_memory.Record(
      spilled_bytes_current_,
      {{ray::stats::LocationKey.name(), ray::stats::kObjectLocSpilled}});

  ray::stats::STATS_spill_manager_request_total.Record(num_failed_deletion_requests_,
                                                       "FailedDeletion");
}

int64_t LocalObjectManager::GetPrimaryBytes() const {
  return pinned_objects_size_ + num_bytes_pending_spill_;
}

bool LocalObjectManager::HasLocallySpilledObjects() const {
  if (!is_external_storage_type_fs_) {
    // External storage is not local.
    return false;
  }
  // Report non-zero usage when there are spilled / spill-pending live objects, to
  // prevent this node from being drained. Note that the value reported here is also
  // used for scheduling.
  return !spilled_objects_url_.empty();
}

std::string LocalObjectManager::DebugString() const {
  std::stringstream result;
  result << "LocalObjectManager:\n";
  result << "- num pinned objects: " << pinned_objects_.size() << "\n";
  result << "- pinned objects size: " << pinned_objects_size_ << "\n";
  result << "- num objects pending restore: " << objects_pending_restore_.size() << "\n";
  result << "- num objects pending spill: " << objects_pending_spill_.size() << "\n";
  result << "- num bytes pending spill: " << num_bytes_pending_spill_ << "\n";
  result << "- num bytes currently spilled: " << spilled_bytes_current_ << "\n";
  result << "- cumulative spill requests: " << spilled_objects_total_ << "\n";
  result << "- cumulative restore requests: " << restored_objects_total_ << "\n";
  result << "- spilled objects pending delete: " << spilled_object_pending_delete_.size()
         << "\n";
  return result.str();
}

};  // namespace raylet
};  // namespace ray
