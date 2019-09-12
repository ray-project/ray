#include "ray/object_manager/object_manager.h"
#include "ray/common/common_protocol.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace object_manager_protocol = ray::object_manager::protocol;

namespace ray {

ObjectManager::ObjectManager(asio::io_service &main_service,
                             const ObjectManagerConfig &config,
                             std::shared_ptr<ObjectDirectoryInterface> object_directory)
    : config_(config),
      object_directory_(std::move(object_directory)),
      store_notification_(main_service, config_.store_socket_name),
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size),
      rpc_work_(rpc_service_),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      object_manager_server_("ObjectManager", config_.object_manager_port),
      object_manager_service_(rpc_service_, *this),
      client_call_manager_(main_service) {
  RAY_CHECK(config_.rpc_service_threads_number > 0);
  client_id_ = object_directory_->GetLocalClientID();
  main_service_ = &main_service;
  store_notification_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        HandleObjectAdded(object_info);
      });
  store_notification_.SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });

  // Start object manager rpc server and send & receive request threads
  StartRpcService();
}

ObjectManager::~ObjectManager() { StopRpcService(); }

void ObjectManager::RegisterGcs() { object_directory_->RegisterBackend(); }

void ObjectManager::RunRpcService() { rpc_service_.run(); }

void ObjectManager::StartRpcService() {
  rpc_threads_.resize(config_.rpc_service_threads_number);
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i] = std::thread(&ObjectManager::RunRpcService, this);
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

void ObjectManager::HandleObjectAdded(
    const object_manager::protocol::ObjectInfoT &object_info) {
  // Notify the object directory that the object has been added to this node.
  ObjectID object_id = ObjectID::FromPlasmaIdBinary(object_info.object_id);
  RAY_LOG(DEBUG) << "Object added " << object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  ray::Status status =
      object_directory_->ReportObjectAdded(object_id, client_id_, object_info);

  // Handle the unfulfilled_push_requests_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &client_id = pair.first;
      main_service_->post([this, object_id, client_id]() { Push(object_id, client_id); });
      // When push timeout is set to -1, there will be an empty timer in pair.second.
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }

  // The object is local, so we no longer need to Pull it from a remote
  // manager. Cancel any outstanding Pull requests for this object.
  CancelPull(object_id);
}

void ObjectManager::NotifyDirectoryObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  ray::Status status =
      object_directory_->ReportObjectRemoved(object_id, client_id_, object_info);
}

ray::Status ObjectManager::SubscribeObjAdded(
    std::function<void(const object_manager::protocol::ObjectInfoT &)> callback) {
  store_notification_.SubscribeObjAdded(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  store_notification_.SubscribeObjDeleted(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "Pull on " << client_id_ << " of object " << object_id;
  // Check if object is already local.
  if (local_objects_.count(object_id) != 0) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return ray::Status::OK();
  }
  if (pull_requests_.find(object_id) != pull_requests_.end()) {
    return ray::Status::OK();
  }

  pull_requests_.emplace(object_id, PullRequest());
  // Subscribe to object notifications. A notification will be received every
  // time the set of client IDs for the object changes. Notifications will also
  // be received if the list of locations is empty. The set of client IDs has
  // no ordering guarantee between notifications.
  return object_directory_->SubscribeObjectLocations(
      object_directory_pull_callback_id_, object_id,
      [this](const ObjectID &object_id, const std::unordered_set<ClientID> &client_ids) {
        // Exit if the Pull request has already been fulfilled or canceled.
        auto it = pull_requests_.find(object_id);
        if (it == pull_requests_.end()) {
          return;
        }
        // Reset the list of clients that are now expected to have the object.
        // NOTE(swang): Since we are overwriting the previous list of clients,
        // we may end up sending a duplicate request to the same client as
        // before.
        it->second.client_locations =
            std::vector<ClientID>(client_ids.begin(), client_ids.end());
        if (it->second.client_locations.empty()) {
          // The object locations are now empty, so we should wait for the next
          // notification about a new object location.  Cancel the timer until
          // the next Pull attempt since there are no more clients to try.
          if (it->second.retry_timer != nullptr) {
            it->second.retry_timer->cancel();
            it->second.timer_set = false;
          }
        } else {
          // New object locations were found, so begin trying to pull from a
          // client. This will be called every time a new client location
          // appears.
          TryPull(object_id);
        }
      });
}

void ObjectManager::TryPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  auto &client_vector = it->second.client_locations;

  // The timer should never fire if there are no expected client locations.
  if (client_vector.empty()) {
    return;
  }

  RAY_CHECK(local_objects_.count(object_id) == 0);
  // Make sure that there is at least one client which is not the local client.
  // TODO(rkn): It may actually be possible for this check to fail.
  if (client_vector.size() == 1 && client_vector[0] == client_id_) {
    RAY_LOG(ERROR) << "The object manager with client ID " << client_id_
                   << " is trying to pull object " << object_id
                   << " but the object table suggests that this object manager "
                   << "already has the object. The object may have been evicted.";
    it->second.timer_set = false;
    return;
  }

  // Choose a random client to pull the object from.
  // Generate a random index.
  std::uniform_int_distribution<int> distribution(0, client_vector.size() - 1);
  int client_index = distribution(gen_);
  ClientID client_id = client_vector[client_index];
  // If the object manager somehow ended up choosing itself, choose a different
  // object manager.
  if (client_id == client_id_) {
    std::swap(client_vector[client_index], client_vector[client_vector.size() - 1]);
    client_vector.pop_back();
    RAY_LOG(ERROR) << "The object manager with client ID " << client_id_
                   << " is trying to pull object " << object_id
                   << " but the object table suggests that this object manager "
                   << "already has the object.";
    client_id = client_vector[client_index % client_vector.size()];
    RAY_CHECK(client_id != client_id_);
  }

  RAY_LOG(DEBUG) << "Sending pull request from " << client_id_ << " to " << client_id
                 << " of object " << object_id;

  auto rpc_client = GetRpcClient(client_id);
  if (rpc_client) {
    // Try pulling from the client.
    rpc_service_.post([this, object_id, client_id, rpc_client]() {
      SendPullRequest(object_id, client_id, rpc_client);
    });
  } else {
    RAY_LOG(ERROR) << "Couldn't send pull request from " << client_id_ << " to "
                   << client_id << " of object " << object_id
                   << " , setup rpc connection failed.";
  }

  // If there are more clients to try, try them in succession, with a timeout
  // in between each try.
  if (!it->second.client_locations.empty()) {
    if (it->second.retry_timer == nullptr) {
      // Set the timer if we haven't already.
      it->second.retry_timer = std::unique_ptr<boost::asio::deadline_timer>(
          new boost::asio::deadline_timer(*main_service_));
    }

    // Wait for a timeout. If we receive the object or a caller Cancels the
    // Pull within the timeout, then nothing will happen. Otherwise, the timer
    // will fire and the next client in the list will be tried.
    boost::posix_time::milliseconds retry_timeout(config_.pull_timeout_ms);
    it->second.retry_timer->expires_from_now(retry_timeout);
    it->second.retry_timer->async_wait(
        [this, object_id](const boost::system::error_code &error) {
          if (!error) {
            // Try the Pull from the next client.
            TryPull(object_id);
          } else {
            // Check that the error was due to the timer being canceled.
            RAY_CHECK(error == boost::asio::error::operation_aborted);
          }
        });
    // Record that we set the timer until the next attempt.
    it->second.timer_set = true;
  } else {
    // The timer is not reset since there are no more clients to try. Go back
    // to waiting for more notifications. Once we receive a new object location
    // from the object directory, then the Pull will be retried.
    it->second.timer_set = false;
  }
};

void ObjectManager::SendPullRequest(
    const ObjectID &object_id, const ClientID &client_id,
    std::shared_ptr<rpc::ObjectManagerClient> rpc_client) {
  rpc::PullRequest pull_request;
  pull_request.set_object_id(object_id.Binary());
  pull_request.set_client_id(client_id_.Binary());

  rpc_client->Pull(pull_request, [object_id, client_id](const Status &status,
                                                        const rpc::PullReply &reply) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Send pull " << object_id << " request to client " << client_id
                       << " failed due to" << status.message();
    }
  });
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const ClientID &client_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  RAY_CHECK(iter != unfulfilled_push_requests_.end());
  uint num_erased = iter->second.erase(client_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleSendFinished(const ObjectID &object_id,
                                       const ClientID &client_id, uint64_t chunk_index,
                                       double start_time, double end_time,
                                       ray::Status status) {
  RAY_LOG(DEBUG) << "HandleSendFinished on " << client_id_ << " to " << client_id
                 << " of object " << object_id << " chunk " << chunk_index
                 << ", status: " << status.ToString();
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
  }

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_send");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, client ID, chunk index, and status as a json list,
  // which will be parsed by the reader of the profile table.
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + client_id.Hex() +
                               "\"," + std::to_string(chunk_index) + ",\"" +
                               status.ToString() + "\"]");

  std::lock_guard<std::mutex> lock(profile_mutex_);
  profile_events_.push_back(profile_event);
}

void ObjectManager::HandleReceiveFinished(const ObjectID &object_id,
                                          const ClientID &client_id, uint64_t chunk_index,
                                          double start_time, double end_time,
                                          ray::Status status) {
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
  }

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_receive");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, client ID, chunk index, and status as a json list,
  // which will be parsed by the reader of the profile table.

  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + client_id.Hex() +
                               "\"," + std::to_string(chunk_index) + ",\"" +
                               status.ToString() + "\"]");

  std::lock_guard<std::mutex> lock(profile_mutex_);
  profile_events_.push_back(profile_event);
}

void ObjectManager::Push(const ObjectID &object_id, const ClientID &client_id) {
  RAY_LOG(DEBUG) << "Push on " << client_id_ << " to " << client_id << " of object "
                 << object_id;
  if (local_objects_.count(object_id) == 0) {
    // Avoid setting duplicated timer for the same object and client pair.
    auto &clients = unfulfilled_push_requests_[object_id];
    if (clients.count(client_id) == 0) {
      // If config_.push_timeout_ms < 0, we give an empty timer
      // and the task will be kept infinitely.
      auto timer = std::unique_ptr<boost::asio::deadline_timer>();
      if (config_.push_timeout_ms == 0) {
        // The Push request fails directly when config_.push_timeout_ms == 0.
        RAY_LOG(WARNING) << "Invalid Push request ObjectID " << object_id
                         << " due to direct timeout setting. ";
      } else if (config_.push_timeout_ms > 0) {
        // Put the task into a queue and wait for the notification of Object added.
        timer.reset(new boost::asio::deadline_timer(*main_service_));
        auto clean_push_period = boost::posix_time::milliseconds(config_.push_timeout_ms);
        timer->expires_from_now(clean_push_period);
        timer->async_wait(
            [this, object_id, client_id](const boost::system::error_code &error) {
              // Timer killing will receive the boost::asio::error::operation_aborted,
              // we only handle the timeout event.
              if (!error) {
                HandlePushTaskTimeout(object_id, client_id);
              }
            });
      }
      if (config_.push_timeout_ms != 0) {
        clients.emplace(client_id, std::move(timer));
      }
    }
    return;
  }

  // If we haven't pushed this object to this same object manager yet, then push
  // it. If we have, but it was a long time ago, then push it. If we have and it
  // was recent, then don't do it again.
  auto &recent_pushes = local_objects_[object_id].recent_pushes;
  auto it = recent_pushes.find(client_id);
  if (it == recent_pushes.end()) {
    // We haven't pushed this specific object to this specific object manager
    // yet (or if we have then the object must have been evicted and recreated
    // locally).
    recent_pushes[client_id] = current_sys_time_ms();
  } else {
    int64_t current_time = current_sys_time_ms();
    if (current_time - it->second <=
        RayConfig::instance().object_manager_repeated_push_delay_ms()) {
      // We pushed this object to the object manager recently, so don't do it
      // again.
      RAY_LOG(DEBUG) << "Object " << object_id << " recently pushed to " << client_id;
      return;
    } else {
      it->second = current_time;
    }
  }

  auto rpc_client = GetRpcClient(client_id);
  if (rpc_client) {
    const object_manager::protocol::ObjectInfoT &object_info =
        local_objects_[object_id].object_info;
    uint64_t data_size =
        static_cast<uint64_t>(object_info.data_size + object_info.metadata_size);
    uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);
    uint64_t num_chunks = buffer_pool_.GetNumChunks(data_size);

    RAY_LOG(DEBUG) << "Sending object chunks of " << object_id << " to client "
                   << client_id << ", number of chunks: " << num_chunks
                   << ", total data size: " << data_size;

    UniqueID push_id = UniqueID::FromRandom();
    for (uint64_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
      rpc_service_.post([this, push_id, object_id, client_id, data_size, metadata_size,
                         chunk_index, rpc_client]() {
        auto st = SendObjectChunk(push_id, object_id, client_id, data_size, metadata_size,
                                  chunk_index, rpc_client);
        if (!st.ok()) {
          RAY_LOG(WARNING) << "Send object " << object_id << " chunk failed due to "
                           << st.message() << ", chunk index " << chunk_index;
        }
      });
    }
  } else {
    // Push is best effort, so do nothing here.
    RAY_LOG(ERROR)
        << "Failed to establish connection for Push with remote object manager.";
  }
}

ray::Status ObjectManager::SendObjectChunk(
    const UniqueID &push_id, const ObjectID &object_id, const ClientID &client_id,
    uint64_t data_size, uint64_t metadata_size, uint64_t chunk_index,
    std::shared_ptr<rpc::ObjectManagerClient> rpc_client) {
  double start_time = current_sys_time_seconds();
  rpc::PushRequest push_request;
  // Set request header
  push_request.set_push_id(push_id.Binary());
  push_request.set_object_id(object_id.Binary());
  push_request.set_client_id(client_id_.Binary());
  push_request.set_data_size(data_size);
  push_request.set_metadata_size(metadata_size);
  push_request.set_chunk_index(chunk_index);

  // Get data
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.GetChunk(object_id, data_size, metadata_size, chunk_index);
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;

  // Fail on status not okay. The object is local, and there is
  // no other anticipated error here.
  ray::Status status = chunk_status.second;
  if (!chunk_status.second.ok()) {
    RAY_LOG(WARNING) << "Attempting to push object " << object_id
                     << " which is not local. It may have been evicted.";
    RAY_RETURN_NOT_OK(status);
  }

  std::string buffer;
  buffer.resize(chunk_info.buffer_length);
  buffer.assign(chunk_info.data, chunk_info.data + chunk_info.buffer_length);
  push_request.set_data(std::move(buffer));

  // record the time cost between send chunk and receive reply
  rpc::ClientCallback<rpc::PushReply> callback = [this, start_time, object_id, client_id,
                                                  chunk_index](
                                                     const Status &status,
                                                     const rpc::PushReply &reply) {
    // TODO: Just print warning here, should we try to resend this chunk?
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Send object " << object_id << " chunk to client " << client_id
                       << " failed due to" << status.message()
                       << ", chunk index: " << chunk_index;
    }
    double end_time = current_sys_time_seconds();
    HandleSendFinished(object_id, client_id, chunk_index, start_time, end_time, status);
  };
  rpc_client->Push(push_request, callback);

  // Do this regardless of whether it failed or succeeded.
  buffer_pool_.ReleaseGetChunk(object_id, chunk_info.chunk_index);
  return Status::OK();
}

void ObjectManager::CancelPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
      object_directory_pull_callback_id_, object_id));
  pull_requests_.erase(it);
}

ray::Status ObjectManager::Wait(const std::vector<ObjectID> &object_ids,
                                int64_t timeout_ms, uint64_t num_required_objects,
                                bool wait_local, const WaitCallback &callback) {
  UniqueID wait_id = UniqueID::FromRandom();
  RAY_LOG(DEBUG) << "Wait request " << wait_id << " on " << client_id_;
  RAY_RETURN_NOT_OK(AddWaitRequest(wait_id, object_ids, timeout_ms, num_required_objects,
                                   wait_local, callback));
  RAY_RETURN_NOT_OK(LookupRemainingWaitObjects(wait_id));
  // LookupRemainingWaitObjects invokes SubscribeRemainingWaitObjects once lookup has
  // been performed on all remaining objects.
  return ray::Status::OK();
}

ray::Status ObjectManager::AddWaitRequest(const UniqueID &wait_id,
                                          const std::vector<ObjectID> &object_ids,
                                          int64_t timeout_ms,
                                          uint64_t num_required_objects, bool wait_local,
                                          const WaitCallback &callback) {
  if (wait_local) {
    return ray::Status::NotImplemented("Wait for local objects is not yet implemented.");
  }

  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  RAY_CHECK(num_required_objects != 0);
  RAY_CHECK(num_required_objects <= object_ids.size());
  if (object_ids.size() == 0) {
    callback(std::vector<ObjectID>(), std::vector<ObjectID>());
  }

  // Initialize fields.
  active_wait_requests_.emplace(wait_id, WaitState(*main_service_, timeout_ms, callback));
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  wait_state.object_id_order = object_ids;
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
          object_id, [this, wait_id](const ObjectID &lookup_object_id,
                                     const std::unordered_set<ClientID> &client_ids) {
            auto &wait_state = active_wait_requests_.find(wait_id)->second;
            if (!client_ids.empty()) {
              wait_state.remaining.erase(lookup_object_id);
              wait_state.found.insert(lookup_object_id);
            }
            RAY_LOG(DEBUG) << "Wait request " << wait_id << ": " << client_ids.size()
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
          wait_id, object_id,
          [this, wait_id](const ObjectID &subscribe_object_id,
                          const std::unordered_set<ClientID> &client_ids) {
            if (!client_ids.empty()) {
              RAY_LOG(DEBUG) << "Wait request " << wait_id
                             << ": subscription notification received for object "
                             << subscribe_object_id;
              auto object_id_wait_state = active_wait_requests_.find(wait_id);
              if (object_id_wait_state == active_wait_requests_.end()) {
                // Depending on the timing of calls to the object directory, we
                // may get a subscription notification after the wait call has
                // already completed. If so, then don't process the
                // notification.
                return;
              }
              auto &wait_state = object_id_wait_state->second;
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
void ObjectManager::HandlePushRequest(const rpc::PushRequest &request,
                                      rpc::PushReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID client_id = ClientID::FromBinary(request.client_id());

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const std::string &data = request.data();

  double start_time = current_sys_time_seconds();
  auto status = ReceiveObjectChunk(client_id, object_id, data_size, metadata_size,
                                   chunk_index, data);
  double end_time = current_sys_time_seconds();

  HandleReceiveFinished(object_id, client_id, chunk_index, start_time, end_time, status);
  send_reply_callback(status, nullptr, nullptr);
}

ray::Status ObjectManager::ReceiveObjectChunk(const ClientID &client_id,
                                              const ObjectID &object_id,
                                              uint64_t data_size, uint64_t metadata_size,
                                              uint64_t chunk_index,
                                              const std::string &data) {
  RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << client_id_ << " from " << client_id
                 << " of object " << object_id << " chunk index: " << chunk_index
                 << ", chunk data size: " << data.size()
                 << ", object size: " << data_size;

  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.CreateChunk(object_id, data_size, metadata_size, chunk_index);
  ray::Status status;
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;
  if (chunk_status.second.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    std::memcpy(chunk_info.data, data.data(), chunk_info.buffer_length);
    buffer_pool_.SealChunk(object_id, chunk_index);
  } else {
    RAY_LOG(WARNING) << "ReceiveObjectChunk index " << chunk_index << " of object "
                     << object_id << " failed: " << chunk_status.second.message();
    // TODO(hme): If the object isn't local, create a pull request for this chunk.
  }
  return status;
}

void ObjectManager::HandlePullRequest(const rpc::PullRequest &request,
                                      rpc::PullReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID client_id = ClientID::FromBinary(request.client_id());
  RAY_LOG(DEBUG) << "Received pull request from client " << client_id << " for object ["
                 << object_id << "].";

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("receive_pull_request");
  profile_event.set_start_time(current_sys_time_seconds());
  profile_event.set_end_time(profile_event.start_time());
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + client_id.Hex() +
                               "\"]");
  {
    std::lock_guard<std::mutex> lock(profile_mutex_);
    profile_events_.emplace_back(profile_event);
  }

  main_service_->post([this, object_id, client_id]() { Push(object_id, client_id); });
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::HandleFreeObjectsRequest(const rpc::FreeObjectsRequest &request,
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
      auto rpc_client = GetRpcClient(connection_info.client_id);
      if (rpc_client != nullptr) {
        rpc_clients.push_back(rpc_client);
      }
    }
    rpc_service_.post([this, object_ids, rpc_clients]() {
      SpreadFreeObjectsRequest(object_ids, rpc_clients);
    });
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
    const ClientID &client_id) {
  auto it = remote_object_manager_clients_.find(client_id);
  if (it == remote_object_manager_clients_.end()) {
    RemoteConnectionInfo connection_info(client_id);
    object_directory_->LookupRemoteConnectionInfo(connection_info);
    if (!connection_info.Connected()) {
      return nullptr;
    }
    auto object_manager_client = std::make_shared<rpc::ObjectManagerClient>(
        connection_info.ip, connection_info.port, client_call_manager_);

    RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
                   << ", port: " << connection_info.port
                   << ", local port: " << GetServerPort();

    it = remote_object_manager_clients_
             .emplace(client_id, std::move(object_manager_client))
             .first;
  }
  return it->second;
}

rpc::ProfileTableData ObjectManager::GetAndResetProfilingInfo() {
  rpc::ProfileTableData profile_info;
  profile_info.set_component_type("object_manager");
  profile_info.set_component_id(client_id_.Binary());

  {
    std::lock_guard<std::mutex> lock(profile_mutex_);
    for (auto const &profile_event : profile_events_) {
      profile_info.add_profile_events()->CopyFrom(profile_event);
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
  result << "\n- num pull requests: " << pull_requests_.size();
  result << "\n- num buffered profile events: " << profile_events_.size();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << store_notification_.DebugString();
  result << "\n" << buffer_pool_.DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() const {
  stats::ObjectManagerStats().Record(local_objects_.size(),
                                     {{stats::ValueTypeKey, "num_local_objects"}});
  stats::ObjectManagerStats().Record(active_wait_requests_.size(),
                                     {{stats::ValueTypeKey, "num_active_wait_requests"}});
  stats::ObjectManagerStats().Record(
      unfulfilled_push_requests_.size(),
      {{stats::ValueTypeKey, "num_unfulfilled_push_requests"}});
  stats::ObjectManagerStats().Record(pull_requests_.size(),
                                     {{stats::ValueTypeKey, "num_pull_requests"}});
  stats::ObjectManagerStats().Record(profile_events_.size(),
                                     {{stats::ValueTypeKey, "num_profile_events"}});
}

}  // namespace ray
