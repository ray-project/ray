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
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace ray {

ObjectStoreRunner::ObjectStoreRunner(const ObjectManagerConfig &config,
                                     SpillObjectsCallback spill_objects_callback,
                                     std::function<void()> object_store_full_callback,
                                     AddObjectCallback add_object_callback,
                                     DeleteObjectCallback delete_object_callback) {
  plasma::plasma_store_runner.reset(
      new plasma::PlasmaStoreRunner(config.store_socket_name,
                                    config.object_store_memory,
                                    config.huge_pages,
                                    config.plasma_directory,
                                    config.fallback_directory));
  // Initialize object store.
  store_thread_ = std::thread(&plasma::PlasmaStoreRunner::Start,
                              plasma::plasma_store_runner.get(),
                              spill_objects_callback,
                              object_store_full_callback,
                              add_object_callback,
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
    instrumented_io_context &main_service,
    const NodeID &self_node_id,
    const ObjectManagerConfig &config,
    IObjectDirectory *object_directory,
    RestoreSpilledObjectCallback restore_spilled_object,
    std::function<std::string(const ObjectID &)> get_spilled_object_url,
    SpillObjectsCallback spill_objects_callback,
    std::function<void()> object_store_full_callback,
    AddObjectCallback add_object_callback,
    DeleteObjectCallback delete_object_callback,
    std::function<std::unique_ptr<RayObject>(const ObjectID &object_id)> pin_object,
    const std::function<void(const ObjectID &)> fail_pull_request)
    : main_service_(&main_service),
      self_node_id_(self_node_id),
      config_(config),
      object_directory_(object_directory),
      object_store_internal_(
          config,
          spill_objects_callback,
          object_store_full_callback,
          /*add_object_callback=*/
          [this, add_object_callback = std::move(add_object_callback)](
              const ObjectInfo &object_info) {
            main_service_->post(
                [this,
                 object_info,
                 add_object_callback = std::move(add_object_callback)]() {
                  HandleObjectAdded(object_info);
                  add_object_callback(object_info);
                },
                "ObjectManager.ObjectAdded");
          },
          /*delete_object_callback=*/
          [this, delete_object_callback = std::move(delete_object_callback)](
              const ObjectID &object_id) {
            main_service_->post(
                [this,
                 object_id,
                 delete_object_callback = std::move(delete_object_callback)]() {
                  HandleObjectDeleted(object_id);
                  delete_object_callback(object_id);
                },
                "ObjectManager.ObjectDeleted");
          }),
      buffer_pool_store_client_(std::make_shared<plasma::PlasmaClient>()),
      buffer_pool_(buffer_pool_store_client_, config_.object_chunk_size),
      rpc_work_(rpc_service_),
      object_manager_server_("ObjectManager",
                             config_.object_manager_port,
                             config_.object_manager_address == "127.0.0.1",
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
  pull_manager_.reset(new PullManager(self_node_id_,
                                      object_is_local,
                                      send_pull_request,
                                      cancel_pull_request,
                                      fail_pull_request,
                                      restore_spilled_object_,
                                      get_time,
                                      config.pull_timeout_ms,
                                      available_memory,
                                      pin_object,
                                      get_spilled_object_url));

  RAY_CHECK_OK(
      buffer_pool_store_client_->Connect(config_.store_socket_name.c_str(), "", 0, 300));

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
                                const NodeID &spilled_node_id,
                                bool pending_creation,
                                size_t object_size) {
    pull_manager_->OnLocationChange(object_id,
                                    client_ids,
                                    spilled_url,
                                    spilled_node_id,
                                    pending_creation,
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

void ObjectManager::HandleSendFinished(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       uint64_t chunk_index,
                                       double start_time,
                                       double end_time,
                                       ray::Status status) {
  RAY_LOG(DEBUG) << "HandleSendFinished on " << self_node_id_ << " to " << node_id
                 << " of object " << object_id << " chunk " << chunk_index
                 << ", status: " << status.ToString();
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
    RAY_LOG(DEBUG) << "Failed to send a push request for an object " << object_id
                   << " to " << node_id << ". Chunk index: " << chunk_index;
  }
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
    // TODO(scv119): handle object size changes in a more graceful way.
    RAY_LOG(WARNING) << "Object id:" << object_id
                     << "'s size mismatches our record. Expected data size: " << data_size
                     << ", expected metadata size: " << metadata_size
                     << ", actual data size: " << object_reader->GetDataSize()
                     << ", actual metadata size: " << object_reader->GetMetadataSize()
                     << ". This is likely due to a race condition."
                     << " We will update the object size and proceed sending the object.";
    local_objects_[object_id].object_info.data_size = 0;
    local_objects_[object_id].object_info.metadata_size = 1;
  }

  PushObjectInternal(object_id,
                     node_id,
                     std::make_shared<ChunkObjectReader>(std::move(object_reader),
                                                         config_.object_chunk_size),
                     /*from_disk=*/false);
}

void ObjectManager::PushFromFilesystem(const ObjectID &object_id,
                                       const NodeID &node_id,
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
            [this,
             object_id,
             node_id,
             chunk_object_reader = std::move(chunk_object_reader)]() {
              PushObjectInternal(object_id,
                                 node_id,
                                 std::move(chunk_object_reader),
                                 /*from_disk=*/true);
            },
            "ObjectManager.PushLocalSpilledObjectInternal");
      },
      "ObjectManager.CreateSpilledObject");
}

void ObjectManager::PushObjectInternal(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       std::shared_ptr<ChunkObjectReader> chunk_reader,
                                       bool from_disk) {
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
              SendObjectChunk(
                  push_id,
                  object_id,
                  node_id,
                  chunk_id,
                  rpc_client,
                  [=](const Status &status) {
                    // Post back to the main event loop because the
                    // PushManager is thread-safe.
                    main_service_->post(
                        [this, node_id, object_id]() {
                          push_manager_->OnChunkComplete(node_id, object_id);
                        },
                        "ObjectManager.Push");
                  },
                  chunk_reader,
                  from_disk);
            },
            "ObjectManager.Push");
      });
}

void ObjectManager::SendObjectChunk(const UniqueID &push_id,
                                    const ObjectID &object_id,
                                    const NodeID &node_id,
                                    uint64_t chunk_index,
                                    std::shared_ptr<rpc::ObjectManagerClient> rpc_client,
                                    std::function<void(const Status &)> on_complete,
                                    std::shared_ptr<ChunkObjectReader> chunk_reader,
                                    bool from_disk) {
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
  if (from_disk) {
    num_bytes_pushed_from_disk_ += push_request.data().length();
  } else {
    num_bytes_pushed_from_plasma_ += push_request.data().length();
  }

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

/// Implementation of ObjectManagerServiceHandler
void ObjectManager::HandlePush(const rpc::PushRequest &request,
                               rpc::PushReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const rpc::Address &owner_address = request.owner_address();
  const std::string &data = request.data();

  bool success = ReceiveObjectChunk(
      node_id, object_id, owner_address, data_size, metadata_size, chunk_index, data);
  num_chunks_received_total_++;
  if (!success) {
    num_chunks_received_total_failed_++;
    RAY_LOG(INFO) << "Received duplicate or cancelled chunk at index " << chunk_index
                  << " of object " << object_id << ": overall "
                  << num_chunks_received_total_failed_ << "/"
                  << num_chunks_received_total_ << " failed";
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

bool ObjectManager::ReceiveObjectChunk(const NodeID &node_id,
                                       const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       uint64_t data_size,
                                       uint64_t metadata_size,
                                       uint64_t chunk_index,
                                       const std::string &data) {
  num_bytes_received_total_ += data.size();
  RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << self_node_id_ << " from " << node_id
                 << " of object " << object_id << " chunk index: " << chunk_index
                 << ", chunk data size: " << data.size()
                 << ", object size: " << data_size;

  if (!pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Do not create the object.
    return false;
  }
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index);
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
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
    return true;
  } else {
    num_chunks_received_failed_due_to_plasma_++;
    RAY_LOG(INFO) << "Error receiving chunk:" << chunk_status.message();
    return false;
  }
}

void ObjectManager::HandlePull(const rpc::PullRequest &request,
                               rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Received pull request from node " << node_id << " for object ["
                 << object_id << "].";

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
    rpc_client->FreeObjects(free_objects_request,
                            [](const Status &status, const rpc::FreeObjectsReply &reply) {
                              if (!status.ok()) {
                                RAY_LOG(WARNING)
                                    << "Send free objects request failed due to"
                                    << status.message();
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

std::string ObjectManager::DebugString() const {
  std::stringstream result;
  result << "ObjectManager:";
  result << "\n- num local objects: " << local_objects_.size();
  result << "\n- num unfulfilled push requests: " << unfulfilled_push_requests_.size();
  result << "\n- num pull requests: " << pull_manager_->NumActiveRequests();
  result << "\n- num chunks received total: " << num_chunks_received_total_;
  result << "\n- num chunks received failed (all): " << num_chunks_received_total_failed_;
  result << "\n- num chunks received failed / cancelled: "
         << num_chunks_received_cancelled_;
  result << "\n- num chunks received failed / plasma error: "
         << num_chunks_received_failed_due_to_plasma_;
  result << "\nEvent stats:" << rpc_service_.stats().StatsString();
  result << "\n" << push_manager_->DebugString();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << buffer_pool_.DebugString();
  result << "\n" << pull_manager_->DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() {
  pull_manager_->RecordMetrics();
  push_manager_->RecordMetrics();
  stats::ObjectStoreAvailableMemory().Record(config_.object_store_memory - used_memory_);
  stats::ObjectStoreUsedMemory().Record(used_memory_);
  stats::ObjectStoreFallbackMemory().Record(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats::ObjectStoreLocalObjects().Record(local_objects_.size());
  stats::ObjectManagerPullRequests().Record(pull_manager_->NumActiveRequests());

  ray::stats::STATS_object_manager_bytes.Record(num_bytes_pushed_from_plasma_,
                                                "PushedFromLocalPlasma");
  ray::stats::STATS_object_manager_bytes.Record(num_bytes_pushed_from_disk_,
                                                "PushedFromLocalDisk");
  ray::stats::STATS_object_manager_bytes.Record(num_bytes_received_total_, "Received");

  ray::stats::STATS_object_manager_received_chunks.Record(num_chunks_received_total_,
                                                          "Total");
  ray::stats::STATS_object_manager_received_chunks.Record(
      num_chunks_received_total_failed_, "FailedTotal");
  ray::stats::STATS_object_manager_received_chunks.Record(num_chunks_received_cancelled_,
                                                          "FailedCancelled");
  ray::stats::STATS_object_manager_received_chunks.Record(
      num_chunks_received_failed_due_to_plasma_, "FailedPlasmaFull");
}

void ObjectManager::FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_object_store_bytes_used(used_memory_);
  stats->set_object_store_bytes_fallback(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats->set_object_store_bytes_avail(config_.object_store_memory);
  stats->set_num_local_objects(local_objects_.size());
  stats->set_consumed_bytes(plasma::plasma_store_runner->GetConsumedBytes());
  stats->set_object_pulls_queued(pull_manager_->HasPullsQueued());
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
