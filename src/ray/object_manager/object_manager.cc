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

#include "ray/object_manager/object_manager.h"

#include <chrono>

#include "ray/common/common_protocol.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace ray {

ObjectStoreRunner::ObjectStoreRunner(const ObjectManagerConfig &config,
                                     SpillObjectsCallback spill_objects_callback,
                                     std::function<void()> object_store_full_callback,
                                     AddObjectCallback add_object_callback,
                                     DeleteObjectCallback delete_object_callback) {
  plasma::plasma_store_runner.reset(new plasma::PlasmaStoreRunner(
      config.store_socket_name, config.object_store_memory, config.huge_pages,
      config.plasma_directory, config.fallback_directory));
  // Initialize object store.
  store_thread_ =
      std::thread(&plasma::PlasmaStoreRunner::Start, plasma::plasma_store_runner.get(),
                  spill_objects_callback, object_store_full_callback, add_object_callback,
                  delete_object_callback);
  // Sleep for sometime until the store is working. This can suppress some
  // connection warnings.
  std::this_thread::sleep_for(std::chrono::microseconds(500));
}

ObjectStoreRunner::~ObjectStoreRunner() {
  plasma::plasma_store_runner->Stop();
  store_thread_.join();
  plasma::plasma_store_runner.reset();
}

ObjectManager::ObjectManager(
    instrumented_io_context &main_service, const NodeID &self_node_id,
    const ObjectManagerConfig &config, ObjectDirectoryInterface *object_directory,
    RestoreSpilledObjectCallback restore_spilled_object,
    std::function<std::string(const ObjectID &)> get_spilled_object_url,
    SpillObjectsCallback spill_objects_callback,
    std::function<void()> object_store_full_callback,
    AddObjectCallback add_object_callback, DeleteObjectCallback delete_object_callback,
    std::function<std::unique_ptr<RayObject>(const ObjectID &object_id)> pin_object)
    : main_service_(&main_service),
      self_node_id_(self_node_id),
      config_(config),
      object_directory_(object_directory),
      object_store_internal_(
          config, spill_objects_callback, object_store_full_callback,
          /*add_object_callback=*/
          [this, add_object_callback =
                     std::move(add_object_callback)](const ObjectInfo &object_info) {
            main_service_->post(
                [this, object_info,
                 add_object_callback = std::move(add_object_callback)]() {
                  HandleObjectAdded(object_info);
                  add_object_callback(object_info);
                },
                "ObjectManager.ObjectAdded");
          },
          /*delete_object_callback=*/
          [this, delete_object_callback =
                     std::move(delete_object_callback)](const ObjectID &object_id) {
            main_service_->post(
                [this, object_id,
                 delete_object_callback = std::move(delete_object_callback)]() {
                  HandleObjectDeleted(object_id);
                  delete_object_callback(object_id);
                },
                "ObjectManager.ObjectDeleted");
          }),
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size),
      rpc_work_(rpc_service_),
      object_manager_server_("ObjectManager", config_.object_manager_port,
                             config_.rpc_service_threads_number),
      object_manager_service_(rpc_service_, *this),
      client_call_manager_(main_service, config_.rpc_service_threads_number),
      restore_spilled_object_(restore_spilled_object),
      get_spilled_object_url_(get_spilled_object_url),
      pull_retry_timer_(*main_service_,
                        boost::posix_time::milliseconds(config.timer_freq_ms)) {
  RAY_CHECK(config_.rpc_service_threads_number > 0);

  push_manager_.reset(new PushManager(/* max_chunks_in_flight= */ std::max(
      static_cast<int64_t>(1L),
      static_cast<int64_t>(config_.max_bytes_in_flight / config_.object_chunk_size))));

  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });

  const auto &object_is_local = [this](const ObjectID &object_id) {
    return local_objects_.count(object_id) != 0;
  };
  const auto &send_pull_request = [this](const ObjectID &object_id,
                                         const NodeID &client_id) {
    SendPullRequest(object_id, client_id);
  };
  const auto &cancel_pull_request = [this](const ObjectID &object_id) {
    // We must abort this object because it may have only been partially
    // created and will cause a leak if we never receive the rest of the
    // object. This is a no-op if the object is already sealed or evicted.
    buffer_pool_.AbortCreate(object_id);
  };
  const auto &get_time = []() { return absl::GetCurrentTimeNanos() / 1e9; };
  int64_t available_memory = config.object_store_memory;
  if (available_memory < 0) {
    available_memory = 0;
  }
  pull_manager_.reset(new PullManager(
      self_node_id_, object_is_local, send_pull_request, cancel_pull_request,
      restore_spilled_object_, get_time, config.pull_timeout_ms, available_memory,
      [spill_objects_callback, object_store_full_callback]() {
        // TODO(swang): This copies the out-of-memory handling in the
        // CreateRequestQueue. It would be nice to unify these.
        object_store_full_callback();
        static_cast<void>(spill_objects_callback());
      },
      pin_object));
  // Start object manager rpc server and send & receive request threads
  StartRpcService();
}

ObjectManager::~ObjectManager() { StopRpcService(); }

void ObjectManager::Stop() { plasma::plasma_store_runner->Stop(); }

bool ObjectManager::IsPlasmaObjectSpillable(const ObjectID &object_id) {
  return plasma::plasma_store_runner->IsPlasmaObjectSpillable(object_id);
}

void ObjectManager::RunRpcService(int index) {
  SetThreadName("rpc.obj.mgr." + std::to_string(index));
  rpc_service_.run();
}

void ObjectManager::StartRpcService() {
  rpc_threads_.resize(config_.rpc_service_threads_number);
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i] = std::thread(&ObjectManager::RunRpcService, this, i);
  }
  object_manager_server_.RegisterService(object_manager_service_);
  object_manager_server_.Run();
}

void ObjectManager::StopRpcService() {
  rpc_service_.stop();
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i].join();
  }
  object_manager_server_.Shutdown();
}

void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  // Notify the object directory that the object has been added to this node.
  const ObjectID &object_id = object_info.object_id;
  RAY_LOG(DEBUG) << "Object added " << object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;
  ray::Status status =
      object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // Give the pull manager a chance to pin actively pulled objects.
  pull_manager_->PinNewObjectIfNeeded(object_id);

  // Handle the unfulfilled_push_requests_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &node_id = pair.first;
      main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                          "ObjectManager.ObjectAddedPush");
      // When push timeout is set to -1, there will be an empty timer in pair.second.
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  ray::Status status =
      object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);

  // Ask the pull manager to fetch this object again as soon as possible, if
  // it was needed by an active pull request.
  pull_manager_->ResetRetryTimer(object_id);
}

uint64_t ObjectManager::Pull(const std::vector<rpc::ObjectReference> &object_refs,
                             BundlePriority prio) {
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto request_id = pull_manager_->Pull(object_refs, prio, &objects_to_locate);

  const auto &callback = [this](const ObjectID &object_id,
                                const std::unordered_set<NodeID> &client_ids,
                                const std::string &spilled_url,
                                const NodeID &spilled_node_id, size_t object_size) {
    pull_manager_->OnLocationChange(object_id, client_ids, spilled_url, spilled_node_id,
                                    object_size);
  };

  for (const auto &ref : objects_to_locate) {
    // Subscribe to object notifications. A notification will be received every
    // time the set of node IDs for the object changes. Notifications will also
    // be received if the list of locations is empty. The set of node IDs has
    // no ordering guarantee between notifications.
    auto object_id = ObjectRefToId(ref);
    RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id, ref.owner_address(), callback));
  }

  return request_id;
}

void ObjectManager::CancelPull(uint64_t request_id) {
  const auto objects_to_cancel = pull_manager_->CancelPull(request_id);
  for (const auto &object_id : objects_to_cancel) {
    RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id));
  }
}

void ObjectManager::SendPullRequest(const ObjectID &object_id, const NodeID &client_id) {
  auto rpc_client = GetRpcClient(client_id);
  if (rpc_client) {
    // Try pulling from the client.
    rpc_service_.post(
        [this, object_id, client_id, rpc_client]() {
          rpc::PullRequest pull_request;
          pull_request.set_object_id(object_id.Binary());
          pull_request.set_node_id(self_node_id_.Binary());

          rpc_client->Pull(
              pull_request,
              [object_id, client_id](const Status &status, const rpc::PullReply &reply) {
                if (!status.ok()) {
                  RAY_LOG(WARNING) << "Send pull " << object_id << " request to client "
                                   << client_id << " failed due to" << status.message();
                }
              });
        },
        "ObjectManager.SendPull");
  } else {
    RAY_LOG(ERROR) << "Couldn't send pull request from " << self_node_id_ << " to "
                   << client_id << " of object " << object_id
                   << " , setup rpc connection failed.";
  }
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const NodeID &node_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  // Under this scenario, `HandlePushTaskTimeout` can be invoked
  // although timer cancels it.
  // 1. wait timer is done and the task is queued.
  // 2. While task is queued, timer->cancel() is invoked.
  // In this case this method can be invoked although it is not timed out.
  // https://www.boost.org/doc/libs/1_66_0/doc/html/boost_asio/reference/basic_deadline_timer/cancel/overload1.html.
  if (iter == unfulfilled_push_requests_.end()) {
    return;
  }
  size_t num_erased = iter->second.erase(node_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleSendFinished(const ObjectID &object_id, const NodeID &node_id,
                                       uint64_t chunk_index, double start_time,
                                       double end_time, ray::Status status) {
  RAY_LOG(DEBUG) << "HandleSendFinished on " << self_node_id_ << " to " << node_id
                 << " of object " << object_id << " chunk " << chunk_index
                 << ", status: " << status.ToString();
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
  }

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_send");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, node ID, chunk index, and status as a json list,
  // which will be parsed by the reader of the profile table.
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + node_id.Hex() + "\"," +
                               std::to_string(chunk_index) + ",\"" + status.ToString() +
                               "\"]");

  std::lock_guard<std::mutex> lock(profile_mutex_);
  profile_events_.push_back(profile_event);
}

void ObjectManager::HandleReceiveFinished(const ObjectID &object_id,
                                          const NodeID &node_id, uint64_t chunk_index,
                                          double start_time, double end_time) {
  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_receive");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, node ID, chunk index as a json list,
  // which will be parsed by the reader of the profile table.
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + node_id.Hex() + "\"," +
                               std::to_string(chunk_index) + "]");

  std::lock_guard<std::mutex> lock(profile_mutex_);
  profile_events_.push_back(profile_event);
}

void ObjectManager::Push(const ObjectID &object_id, const NodeID &node_id) {
  RAY_LOG(DEBUG) << "Push on " << self_node_id_ << " to " << node_id << " of object "
                 << object_id;
  if (local_objects_.count(object_id) != 0) {
    return PushLocalObject(object_id, node_id);
  }

  // Push from spilled object directly if the object is on local disk.
  auto object_url = get_spilled_object_url_(object_id);
  if (!object_url.empty() && RayConfig::instance().is_external_storage_type_fs()) {
    return PushFromFilesystem(object_id, node_id, object_url);
  }

  // Avoid setting duplicated timer for the same object and node pair.
  auto &nodes = unfulfilled_push_requests_[object_id];

  if (nodes.count(node_id) == 0) {
    // If config_.push_timeout_ms < 0, we give an empty timer
    // and the task will be kept infinitely.
    std::unique_ptr<boost::asio::deadline_timer> timer;
    if (config_.push_timeout_ms == 0) {
      // The Push request fails directly when config_.push_timeout_ms == 0.
      RAY_LOG(WARNING) << "Invalid Push request ObjectID " << object_id
                       << " due to direct timeout setting. (0 ms timeout)";
    } else if (config_.push_timeout_ms > 0) {
      // Put the task into a queue and wait for the notification of Object added.
      timer.reset(new boost::asio::deadline_timer(*main_service_));
      auto clean_push_period = boost::posix_time::milliseconds(config_.push_timeout_ms);
      timer->expires_from_now(clean_push_period);
      timer->async_wait(
          [this, object_id, node_id](const boost::system::error_code &error) {
            // Timer killing will receive the boost::asio::error::operation_aborted,
            // we only handle the timeout event.
            if (!error) {
              HandlePushTaskTimeout(object_id, node_id);
            }
          });
    }
    if (config_.push_timeout_ms != 0) {
      nodes.emplace(node_id, std::move(timer));
    }
  }
}

void ObjectManager::PushLocalObject(const ObjectID &object_id, const NodeID &node_id) {
  const ObjectInfo &object_info = local_objects_[object_id].object_info;
  uint64_t data_size = static_cast<uint64_t>(object_info.data_size);
  uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);

  rpc::Address owner_address;
  owner_address.set_raylet_id(object_info.owner_raylet_id.Binary());
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());

  std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status> reader_status =
      buffer_pool_.CreateObjectReader(object_id, owner_address);
  Status status = reader_status.second;
  if (!status.ok()) {
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
        << "Ignoring stale read request for already deleted object: " << object_id;
    return;
  }

  auto object_reader = std::move(reader_status.first);
  RAY_CHECK(object_reader) << "object_reader can't be null";

  if (object_reader->GetDataSize() != data_size ||
      object_reader->GetMetadataSize() != metadata_size) {
    if (object_reader->GetDataSize() == 0 && object_reader->GetMetadataSize() == 1) {
      // TODO(scv119): handle object size changes in a more graceful way.
      RAY_LOG(WARNING) << object_id
                       << " is marked as failed but object_manager has stale info "
                       << " with data size: " << data_size
                       << ", metadata size: " << metadata_size
                       << ". This is likely due to race condition."
                       << " Update the info and proceed sending failed object.";
      local_objects_[object_id].object_info.data_size = 0;
      local_objects_[object_id].object_info.metadata_size = 1;
    } else {
      RAY_LOG(FATAL) << "Object id:" << object_id
                     << "'s size mismatches our record. Expected data size: " << data_size
                     << ", expected metadata size: " << metadata_size
                     << ", actual data size: " << object_reader->GetDataSize()
                     << ", actual metadata size: " << object_reader->GetMetadataSize();
    }
  }

  PushObjectInternal(object_id, node_id,
                     std::make_shared<ChunkObjectReader>(std::move(object_reader),
                                                         config_.object_chunk_size));
}

void ObjectManager::PushFromFilesystem(const ObjectID &object_id, const NodeID &node_id,
                                       const std::string &spilled_url) {
  // SpilledObjectReader::CreateSpilledObjectReader does synchronous IO; schedule it off
  // main thread.
  rpc_service_.post(
      [this, object_id, node_id, spilled_url, chunk_size = config_.object_chunk_size]() {
        auto optional_spilled_object =
            SpilledObjectReader::CreateSpilledObjectReader(spilled_url);
        if (!optional_spilled_object.has_value()) {
          RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
              << "Ignoring stale read request for already deleted object: " << object_id;
          return;
        }
        auto chunk_object_reader = std::make_shared<ChunkObjectReader>(
            std::make_shared<SpilledObjectReader>(
                std::move(optional_spilled_object.value())),
            chunk_size);

        // Schedule PushObjectInternal back to main_service as PushObjectInternal access
        // thread unsafe datastructure.
        main_service_->post(
            [this, object_id, node_id,
             chunk_object_reader = std::move(chunk_object_reader)]() {
              PushObjectInternal(object_id, node_id, std::move(chunk_object_reader));
            },
            "ObjectManager.PushLocalSpilledObjectInternal");
      },
      "ObjectManager.CreateSpilledObject");
}

void ObjectManager::PushObjectInternal(const ObjectID &object_id, const NodeID &node_id,
                                       std::shared_ptr<ChunkObjectReader> chunk_reader) {
  auto rpc_client = GetRpcClient(node_id);
  if (!rpc_client) {
    // Push is best effort, so do nothing here.
    RAY_LOG(INFO)
        << "Failed to establish connection for Push with remote object manager.";
    return;
  }

  RAY_LOG(DEBUG) << "Sending object chunks of " << object_id << " to node " << node_id
                 << ", number of chunks: " << chunk_reader->GetNumChunks()
                 << ", total data size: " << chunk_reader->GetObject().GetObjectSize();

  auto push_id = UniqueID::FromRandom();
  push_manager_->StartPush(
      node_id, object_id, chunk_reader->GetNumChunks(), [=](int64_t chunk_id) {
        rpc_service_.post(
            [=]() {
              // Post to the multithreaded RPC event loop so that data is copied
              // off of the main thread.
              SendObjectChunk(push_id, object_id, node_id, chunk_id, rpc_client,
                              [=](const Status &status) {
                                // Post back to the main event loop because the
                                // PushManager is thread-safe.
                                main_service_->post(
                                    [this, node_id, object_id]() {
                                      push_manager_->OnChunkComplete(node_id, object_id);
                                    },
                                    "ObjectManager.Push");
                              },
                              std::move(chunk_reader));
            },
            "ObjectManager.Push");
      });
}

void ObjectManager::SendObjectChunk(const UniqueID &push_id, const ObjectID &object_id,
                                    const NodeID &node_id, uint64_t chunk_index,
                                    std::shared_ptr<rpc::ObjectManagerClient> rpc_client,
                                    std::function<void(const Status &)> on_complete,
                                    std::shared_ptr<ChunkObjectReader> chunk_reader) {
  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  rpc::PushRequest push_request;
  // Set request header
  push_request.set_push_id(push_id.Binary());
  push_request.set_object_id(object_id.Binary());
  push_request.mutable_owner_address()->CopyFrom(
      chunk_reader->GetObject().GetOwnerAddress());
  push_request.set_node_id(self_node_id_.Binary());
  push_request.set_data_size(chunk_reader->GetObject().GetObjectSize());
  push_request.set_metadata_size(chunk_reader->GetObject().GetMetadataSize());
  push_request.set_chunk_index(chunk_index);

  // read a chunk into push_request and handle errors.
  auto optional_chunk = chunk_reader->GetChunk(chunk_index);
  if (!optional_chunk.has_value()) {
    RAY_LOG(DEBUG) << "Read chunk " << chunk_index << " of object " << object_id
                   << " failed. It may have been evicted.";
    on_complete(Status::IOError("Failed to read spilled object"));
    return;
  }
  push_request.set_data(std::move(optional_chunk.value()));

  // record the time cost between send chunk and receive reply
  rpc::ClientCallback<rpc::PushReply> callback =
      [this, start_time, object_id, node_id, chunk_index, on_complete](
          const Status &status, const rpc::PushReply &reply) {
        // TODO: Just print warning here, should we try to resend this chunk?
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Send object " << object_id << " chunk to node " << node_id
                           << " failed due to" << status.message()
                           << ", chunk index: " << chunk_index;
        }
        double end_time = absl::GetCurrentTimeNanos() / 1e9;
        HandleSendFinished(object_id, node_id, chunk_index, start_time, end_time, status);
        on_complete(status);
      };

  rpc_client->Push(push_request, callback);
}

ray::Status ObjectManager::Wait(
    const std::vector<ObjectID> &object_ids,
    const std::unordered_map<ObjectID, rpc::Address> &owner_addresses, int64_t timeout_ms,
    uint64_t num_required_objects, const WaitCallback &callback) {
  UniqueID wait_id = UniqueID::FromRandom();
  RAY_LOG(DEBUG) << "Wait request " << wait_id << " on " << self_node_id_;
  RAY_RETURN_NOT_OK(AddWaitRequest(wait_id, object_ids, owner_addresses, timeout_ms,
                                   num_required_objects, callback));
  RAY_RETURN_NOT_OK(LookupRemainingWaitObjects(wait_id));
  // LookupRemainingWaitObjects invokes SubscribeRemainingWaitObjects once lookup has
  // been performed on all remaining objects.
  return ray::Status::OK();
}

ray::Status ObjectManager::AddWaitRequest(
    const UniqueID &wait_id, const std::vector<ObjectID> &object_ids,
    const std::unordered_map<ObjectID, rpc::Address> &owner_addresses, int64_t timeout_ms,
    uint64_t num_required_objects, const WaitCallback &callback) {
  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  RAY_CHECK(num_required_objects != 0);
  RAY_CHECK(num_required_objects <= object_ids.size())
      << num_required_objects << " " << object_ids.size();
  if (object_ids.size() == 0) {
    callback(std::vector<ObjectID>(), std::vector<ObjectID>());
  }

  // Initialize fields.
  active_wait_requests_.emplace(wait_id, WaitState(*main_service_, timeout_ms, callback));
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  wait_state.object_id_order = object_ids;
  wait_state.owner_addresses = owner_addresses;
  wait_state.timeout_ms = timeout_ms;
  wait_state.num_required_objects = num_required_objects;
  for (const auto &object_id : object_ids) {
    if (local_objects_.count(object_id) > 0) {
      wait_state.found.insert(object_id);
    } else {
      wait_state.remaining.insert(object_id);
    }
  }

  return ray::Status::OK();
}

ray::Status ObjectManager::LookupRemainingWaitObjects(const UniqueID &wait_id) {
  auto &wait_state = active_wait_requests_.find(wait_id)->second;

  if (wait_state.remaining.empty()) {
    WaitComplete(wait_id);
  } else {
    // We invoke lookup calls immediately after checking which objects are local to
    // obtain current information about the location of remote objects. Thus,
    // we obtain information about all given objects, regardless of their location.
    // This is required to ensure we do not bias returning locally available objects
    // as ready whenever Wait is invoked with a mixture of local and remote objects.
    for (const auto &object_id : wait_state.remaining) {
      // Lookup remaining objects.
      wait_state.requested_objects.insert(object_id);
      RAY_RETURN_NOT_OK(object_directory_->LookupLocations(
          object_id, wait_state.owner_addresses[object_id],
          [this, wait_id](const ObjectID &lookup_object_id,
                          const std::unordered_set<NodeID> &node_ids,
                          const std::string &spilled_url, const NodeID &spilled_node_id,
                          size_t object_size) {
            auto &wait_state = active_wait_requests_.find(wait_id)->second;
            // Note that the object is guaranteed to be added to local_objects_ before
            // the notification is triggered.
            if (local_objects_.count(lookup_object_id) > 0) {
              wait_state.remaining.erase(lookup_object_id);
              wait_state.found.insert(lookup_object_id);
            }
            RAY_LOG(DEBUG) << "Wait request " << wait_id << ": " << node_ids.size()
                           << " locations found for object " << lookup_object_id;
            wait_state.requested_objects.erase(lookup_object_id);
            if (wait_state.requested_objects.empty()) {
              SubscribeRemainingWaitObjects(wait_id);
            }
          }));
    }
  }
  return ray::Status::OK();
}

void ObjectManager::SubscribeRemainingWaitObjects(const UniqueID &wait_id) {
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  if (wait_state.found.size() >= wait_state.num_required_objects ||
      wait_state.timeout_ms == 0) {
    // Requirements already satisfied.
    WaitComplete(wait_id);
    return;
  }

  // There are objects remaining whose locations we don't know. Request their
  // locations from the object directory.
  for (const auto &object_id : wait_state.object_id_order) {
    if (wait_state.remaining.count(object_id) > 0) {
      RAY_LOG(DEBUG) << "Wait request " << wait_id << ": subscribing to object "
                     << object_id;
      wait_state.requested_objects.insert(object_id);
      // Subscribe to object notifications.
      RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
          wait_id, object_id, wait_state.owner_addresses[object_id],
          [this, wait_id](const ObjectID &subscribe_object_id,
                          const std::unordered_set<NodeID> &node_ids,
                          const std::string &spilled_url, const NodeID &spilled_node_id,
                          size_t object_size) {
            auto object_id_wait_state = active_wait_requests_.find(wait_id);
            if (object_id_wait_state == active_wait_requests_.end()) {
              // Depending on the timing of calls to the object directory, we
              // may get a subscription notification after the wait call has
              // already completed. If so, then don't process the
              // notification.
              return;
            }
            auto &wait_state = object_id_wait_state->second;
            // Note that the object is guaranteed to be added to local_objects_ before
            // the notification is triggered.
            if (local_objects_.count(subscribe_object_id) > 0) {
              RAY_LOG(DEBUG) << "Wait request " << wait_id
                             << ": subscription notification received for object "
                             << subscribe_object_id;
              wait_state.remaining.erase(subscribe_object_id);
              wait_state.found.insert(subscribe_object_id);
              wait_state.requested_objects.erase(subscribe_object_id);
              RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
                  wait_id, subscribe_object_id));
              if (wait_state.found.size() >= wait_state.num_required_objects) {
                WaitComplete(wait_id);
              }
            }
          }));
    }

    // If a timeout was provided, then set a timer. If we don't find locations
    // for enough objects by the time the timer expires, then we will return
    // from the Wait.
    if (wait_state.timeout_ms != -1) {
      auto timeout = boost::posix_time::milliseconds(wait_state.timeout_ms);
      wait_state.timeout_timer->expires_from_now(timeout);
      wait_state.timeout_timer->async_wait(
          [this, wait_id](const boost::system::error_code &error_code) {
            if (error_code.value() != 0) {
              return;
            }
            if (active_wait_requests_.find(wait_id) == active_wait_requests_.end()) {
              // When a subscription callback is triggered first, WaitComplete will be
              // called. The timer may at the same time goes off and may be an
              // interruption will post WaitComplete to main_service_ the second time.
              // This check will avoid the duplicated call of this function.
              return;
            }
            WaitComplete(wait_id);
          });
    }
  }
}

void ObjectManager::WaitComplete(const UniqueID &wait_id) {
  auto iter = active_wait_requests_.find(wait_id);
  RAY_CHECK(iter != active_wait_requests_.end());
  auto &wait_state = iter->second;
  // If we complete with outstanding requests, then timeout_ms should be non-zero or -1
  // (infinite wait time).
  if (!wait_state.requested_objects.empty()) {
    RAY_CHECK(wait_state.timeout_ms > 0 || wait_state.timeout_ms == -1);
  }
  // Unsubscribe to any objects that weren't found in the time allotted.
  for (const auto &object_id : wait_state.requested_objects) {
    RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(wait_id, object_id));
  }
  // Cancel the timer. This is okay even if the timer hasn't been started.
  // The timer handler will be given a non-zero error code. The handler
  // will do nothing on non-zero error codes.
  wait_state.timeout_timer->cancel();
  // Order objects according to input order.
  std::vector<ObjectID> found;
  std::vector<ObjectID> remaining;
  for (const auto &item : wait_state.object_id_order) {
    if (found.size() < wait_state.num_required_objects &&
        wait_state.found.count(item) > 0) {
      found.push_back(item);
    } else {
      remaining.push_back(item);
    }
  }
  wait_state.callback(found, remaining);
  active_wait_requests_.erase(wait_id);
  RAY_LOG(DEBUG) << "Wait request " << wait_id << " finished: found " << found.size()
                 << " remaining " << remaining.size();
}

/// Implementation of ObjectManagerServiceHandler
void ObjectManager::HandlePush(const rpc::PushRequest &request, rpc::PushReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const rpc::Address &owner_address = request.owner_address();
  const std::string &data = request.data();

  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  bool success = ReceiveObjectChunk(node_id, object_id, owner_address, data_size,
                                    metadata_size, chunk_index, data);
  num_chunks_received_total_++;
  if (!success) {
    num_chunks_received_total_failed_++;
    RAY_LOG(INFO) << "Received duplicate or cancelled chunk at index " << chunk_index
                  << " of object " << object_id << ": overall "
                  << num_chunks_received_total_failed_ << "/"
                  << num_chunks_received_total_ << " failed";
  }
  double end_time = absl::GetCurrentTimeNanos() / 1e9;

  HandleReceiveFinished(object_id, node_id, chunk_index, start_time, end_time);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

bool ObjectManager::ReceiveObjectChunk(const NodeID &node_id, const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       uint64_t data_size, uint64_t metadata_size,
                                       uint64_t chunk_index, const std::string &data) {
  RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << self_node_id_ << " from " << node_id
                 << " of object " << object_id << " chunk index: " << chunk_index
                 << ", chunk data size: " << data.size()
                 << ", object size: " << data_size;

  if (!pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Do not create the object.
    return false;
  }
  auto chunk_status = buffer_pool_.CreateChunk(object_id, owner_address, data_size,
                                               metadata_size, chunk_index);
  if (!pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Abort the object. We
    // have to check again here because the pull manager runs in a different
    // thread and the object may have been deactivated right before creating
    // the chunk.
    buffer_pool_.AbortCreate(object_id);
    return false;
  }

  if (chunk_status.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    buffer_pool_.WriteChunk(object_id, chunk_index, data);
    return true;
  } else {
    num_chunks_received_failed_due_to_plasma_++;
    RAY_LOG(INFO) << "Error receiving chunk:" << chunk_status.message();
    return false;
  }
}

void ObjectManager::HandlePull(const rpc::PullRequest &request, rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Received pull request from node " << node_id << " for object ["
                 << object_id << "].";

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("receive_pull_request");
  profile_event.set_start_time(absl::GetCurrentTimeNanos() / 1e9);
  profile_event.set_end_time(profile_event.start_time());
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + node_id.Hex() + "\"]");
  {
    std::lock_guard<std::mutex> lock(profile_mutex_);
    profile_events_.emplace_back(profile_event);
  }

  main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                      "ObjectManager.HandlePull");
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::HandleFreeObjects(const rpc::FreeObjectsRequest &request,
                                      rpc::FreeObjectsReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &e : request.object_ids()) {
    object_ids.emplace_back(ObjectID::FromBinary(e));
  }
  FreeObjects(object_ids, /* local_only */ true);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids,
                                bool local_only) {
  buffer_pool_.FreeObjects(object_ids);
  if (!local_only) {
    const auto remote_connections = object_directory_->LookupAllRemoteConnections();
    std::vector<std::shared_ptr<rpc::ObjectManagerClient>> rpc_clients;
    for (const auto &connection_info : remote_connections) {
      auto rpc_client = GetRpcClient(connection_info.node_id);
      if (rpc_client != nullptr) {
        rpc_clients.push_back(rpc_client);
      }
    }
    rpc_service_.post(
        [this, object_ids, rpc_clients]() {
          SpreadFreeObjectsRequest(object_ids, rpc_clients);
        },
        "ObjectManager.FreeObjects");
  }
}

void ObjectManager::SpreadFreeObjectsRequest(
    const std::vector<ObjectID> &object_ids,
    const std::vector<std::shared_ptr<rpc::ObjectManagerClient>> &rpc_clients) {
  // This code path should be called from node manager.
  rpc::FreeObjectsRequest free_objects_request;
  for (const auto &e : object_ids) {
    free_objects_request.add_object_ids(e.Binary());
  }

  for (auto &rpc_client : rpc_clients) {
    rpc_client->FreeObjects(free_objects_request, [](const Status &status,
                                                     const rpc::FreeObjectsReply &reply) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Send free objects request failed due to" << status.message();
      }
    });
  }
}

std::shared_ptr<rpc::ObjectManagerClient> ObjectManager::GetRpcClient(
    const NodeID &node_id) {
  auto it = remote_object_manager_clients_.find(node_id);
  if (it == remote_object_manager_clients_.end()) {
    RemoteConnectionInfo connection_info(node_id);
    object_directory_->LookupRemoteConnectionInfo(connection_info);
    if (!connection_info.Connected()) {
      return nullptr;
    }
    auto object_manager_client = std::make_shared<rpc::ObjectManagerClient>(
        connection_info.ip, connection_info.port, client_call_manager_);

    RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
                   << ", port: " << connection_info.port
                   << ", local port: " << GetServerPort();

    it = remote_object_manager_clients_.emplace(node_id, std::move(object_manager_client))
             .first;
  }
  return it->second;
}

std::shared_ptr<rpc::ProfileTableData> ObjectManager::GetAndResetProfilingInfo() {
  auto profile_info = std::make_shared<rpc::ProfileTableData>();
  profile_info->set_component_type("object_manager");
  profile_info->set_component_id(self_node_id_.Binary());

  {
    std::lock_guard<std::mutex> lock(profile_mutex_);
    for (auto const &profile_event : profile_events_) {
      profile_info->add_profile_events()->CopyFrom(profile_event);
    }
    profile_events_.clear();
  }

  return profile_info;
}

std::string ObjectManager::DebugString() const {
  std::stringstream result;
  result << "ObjectManager:";
  result << "\n- num local objects: " << local_objects_.size();
  result << "\n- num active wait requests: " << active_wait_requests_.size();
  result << "\n- num unfulfilled push requests: " << unfulfilled_push_requests_.size();
  result << "\n- num pull requests: " << pull_manager_->NumActiveRequests();
  result << "\n- num buffered profile events: " << profile_events_.size();
  result << "\n- num chunks received total: " << num_chunks_received_total_;
  result << "\n- num chunks received failed (all): " << num_chunks_received_total_failed_;
  result << "\n- num chunks received failed / cancelled: "
         << num_chunks_received_cancelled_;
  result << "\n- num chunks received failed / plasma error: "
         << num_chunks_received_failed_due_to_plasma_;
  result << "\nEvent stats:" << rpc_service_.StatsString();
  result << "\n" << push_manager_->DebugString();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << buffer_pool_.DebugString();
  result << "\n" << pull_manager_->DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() const {
  stats::ObjectStoreAvailableMemory().Record(config_.object_store_memory - used_memory_);
  stats::ObjectStoreUsedMemory().Record(used_memory_);
  stats::ObjectStoreFallbackMemory().Record(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats::ObjectStoreLocalObjects().Record(local_objects_.size());
  stats::ObjectManagerPullRequests().Record(pull_manager_->NumActiveRequests());
}

void ObjectManager::FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_object_store_bytes_used(used_memory_);
  stats->set_object_store_bytes_fallback(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats->set_object_store_bytes_avail(config_.object_store_memory);
  stats->set_num_local_objects(local_objects_.size());
  stats->set_consumed_bytes(plasma::plasma_store_runner->GetConsumedBytes());
}

void ObjectManager::Tick(const boost::system::error_code &e) {
  RAY_CHECK(!e) << "The raylet's object manager has failed unexpectedly with error: " << e
                << ". Please file a bug report on here: "
                   "https://github.com/ray-project/ray/issues";

  // Request the current available memory from the object
  // store.
  plasma::plasma_store_runner->GetAvailableMemoryAsync([this](size_t available_memory) {
    main_service_->post(
        [this, available_memory]() {
          pull_manager_->UpdatePullsBasedOnAvailableMemory(available_memory);
        },
        "ObjectManager.UpdateAvailableMemory");
  });

  pull_manager_->Tick();

  auto interval = boost::posix_time::milliseconds(config_.timer_freq_ms);
  pull_retry_timer_.expires_from_now(interval);
  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });
}

}  // namespace ray
