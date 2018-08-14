#include "ray/object_manager/object_manager.h"
#include "common/common_protocol.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace object_manager_protocol = ray::object_manager::protocol;

namespace {

void CheckIOError(ray::Status &status, const std::string &operation) {
  RAY_CHECK(status.IsIOError());
  RAY_LOG(ERROR) << "Failed to contact remote object manager during " << operation;
}

}  // namespace

namespace ray {

ObjectManager::ObjectManager(asio::io_service &main_service,
                             const ObjectManagerConfig &config,
                             std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    // TODO(hme): Eliminate knowledge of GCS.
    : client_id_(gcs_client->client_table().GetLocalClientId()),
      config_(config),
      object_directory_(new ObjectDirectory(gcs_client)),
      store_notification_(main_service, config_.store_socket_name),
      // release_delay of 2 * config_.max_sends is to ensure the pool does not release
      // an object prematurely whenever we reach the maximum number of sends.
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size,
                   /*release_delay=*/2 * config_.max_sends),
      send_work_(send_service_),
      receive_work_(receive_service_),
      connection_pool_() {
  RAY_CHECK(config_.max_sends > 0);
  RAY_CHECK(config_.max_receives > 0);
  main_service_ = &main_service;
  store_notification_.SubscribeObjAdded(
      [this](const ObjectInfoT &object_info) { HandleObjectAdded(object_info); });
  store_notification_.SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
}

ObjectManager::ObjectManager(asio::io_service &main_service,
                             const ObjectManagerConfig &config,
                             std::unique_ptr<ObjectDirectoryInterface> od)
    : config_(config),
      object_directory_(std::move(od)),
      store_notification_(main_service, config_.store_socket_name),
      // release_delay of 2 * config_.max_sends is to ensure the pool does not release
      // an object prematurely whenever we reach the maximum number of sends.
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size,
                   /*release_delay=*/2 * config_.max_sends),
      send_work_(send_service_),
      receive_work_(receive_service_),
      connection_pool_() {
  RAY_CHECK(config_.max_sends > 0);
  RAY_CHECK(config_.max_receives > 0);
  // TODO(hme) Client ID is never set with this constructor.
  main_service_ = &main_service;
  store_notification_.SubscribeObjAdded(
      [this](const ObjectInfoT &object_info) { HandleObjectAdded(object_info); });
  store_notification_.SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
}

ObjectManager::~ObjectManager() { StopIOService(); }

void ObjectManager::RegisterGcs() { object_directory_->RegisterBackend(); }

void ObjectManager::StartIOService() {
  for (int i = 0; i < config_.max_sends; ++i) {
    send_threads_.emplace_back(std::thread(&ObjectManager::RunSendService, this));
  }
  for (int i = 0; i < config_.max_receives; ++i) {
    receive_threads_.emplace_back(std::thread(&ObjectManager::RunReceiveService, this));
  }
}

void ObjectManager::RunSendService() { send_service_.run(); }

void ObjectManager::RunReceiveService() { receive_service_.run(); }

void ObjectManager::StopIOService() {
  send_service_.stop();
  for (int i = 0; i < config_.max_sends; ++i) {
    send_threads_[i].join();
  }
  receive_service_.stop();
  for (int i = 0; i < config_.max_receives; ++i) {
    receive_threads_[i].join();
  }
}

void ObjectManager::HandleObjectAdded(const ObjectInfoT &object_info) {
  // Notify the object directory that the object has been added to this node.
  ObjectID object_id = ObjectID::from_binary(object_info.object_id);
  local_objects_[object_id] = object_info;
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
  local_objects_.erase(object_id);
  ray::Status status = object_directory_->ReportObjectRemoved(object_id, client_id_);
}

ray::Status ObjectManager::SubscribeObjAdded(
    std::function<void(const ObjectInfoT &)> callback) {
  store_notification_.SubscribeObjAdded(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  store_notification_.SubscribeObjDeleted(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
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
      [this](const std::vector<ClientID> &client_ids, const ObjectID &object_id) {
        // Exit if the Pull request has already been fulfilled or canceled.
        auto it = pull_requests_.find(object_id);
        if (it == pull_requests_.end()) {
          return;
        }
        // Reset the list of clients that are now expected to have the object.
        // NOTE(swang): Since we are overwriting the previous list of clients,
        // we may end up sending a duplicate request to the same client as
        // before.
        it->second.client_locations = client_ids;
        if (it->second.client_locations.empty()) {
          // The object locations are now empty, so we should wait for the next
          // notification about a new object location.  Cancel the timer until
          // the next Pull attempt since there are no more clients to try.
          if (it->second.retry_timer != nullptr) {
            it->second.retry_timer->cancel();
            it->second.timer_set = false;
          }
        } else {
          // New object locations were found.
          if (!it->second.timer_set) {
            // The timer was not set, which means that we weren't trying any
            // clients. We now have some clients to try, so begin trying to
            // Pull from one.  If we fail to receive an object within the pull
            // timeout, then this will try the rest of the clients in the list
            // in succession.
            TryPull(object_id);
          }
        }
      });
}

void ObjectManager::TryPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  // The timer should never fire if there are no expected client locations.
  RAY_CHECK(!it->second.client_locations.empty());
  RAY_CHECK(local_objects_.count(object_id) == 0);

  // Get the next client to try.
  const ClientID client_id = std::move(it->second.client_locations.back());
  it->second.client_locations.pop_back();
  if (client_id == client_id_) {
    // If we're trying to pull from ourselves, skip this client and try the
    // next one.
    RAY_LOG(ERROR) << client_id_ << " attempted to pull an object from itself.";
    const ClientID client_id = std::move(it->second.client_locations.back());
    it->second.client_locations.pop_back();
    RAY_CHECK(client_id != client_id_);
  }

  // Try pulling from the client.
  PullEstablishConnection(object_id, client_id);

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

void ObjectManager::PullEstablishConnection(const ObjectID &object_id,
                                            const ClientID &client_id) {
  // Acquire a message connection and send pull request.
  ray::Status status;
  std::shared_ptr<SenderConnection> conn;
  // TODO(hme): There is no cap on the number of pull request connections.
  connection_pool_.GetSender(ConnectionPool::ConnectionType::MESSAGE, client_id, &conn);

  if (conn == nullptr) {
    status = object_directory_->GetInformation(
        client_id,
        [this, object_id, client_id](const RemoteConnectionInfo &connection_info) {
          std::shared_ptr<SenderConnection> async_conn = CreateSenderConnection(
              ConnectionPool::ConnectionType::MESSAGE, connection_info);
          if (async_conn == nullptr) {
            return;
          }
          connection_pool_.RegisterSender(ConnectionPool::ConnectionType::MESSAGE,
                                          client_id, async_conn);
          Status pull_send_status = PullSendRequest(object_id, async_conn);
          if (!pull_send_status.ok()) {
            CheckIOError(pull_send_status, "Pull");
          }
        },
        []() {
          RAY_LOG(ERROR) << "Failed to establish connection with remote object manager.";
        });
  } else {
    status = PullSendRequest(object_id, conn);
    if (!status.ok()) {
      CheckIOError(status, "Pull");
    }
  }
}

ray::Status ObjectManager::PullSendRequest(const ObjectID &object_id,
                                           std::shared_ptr<SenderConnection> &conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = object_manager_protocol::CreatePullRequestMessage(
      fbb, fbb.CreateString(client_id_.binary()), fbb.CreateString(object_id.binary()));
  fbb.Finish(message);
  Status status = conn->WriteMessage(
      static_cast<int64_t>(object_manager_protocol::MessageType::PullRequest),
      fbb.GetSize(), fbb.GetBufferPointer());
  if (status.ok()) {
    connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::MESSAGE, conn);
  }
  return status;
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

void ObjectManager::Push(const ObjectID &object_id, const ClientID &client_id) {
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

  // TODO(hme): Cache this data in ObjectDirectory.
  // Okay for now since the GCS client caches this data.
  RAY_CHECK_OK(object_directory_->GetInformation(
      client_id,
      [this, object_id, client_id](const RemoteConnectionInfo &info) {
        const ObjectInfoT &object_info = local_objects_[object_id];
        uint64_t data_size =
            static_cast<uint64_t>(object_info.data_size + object_info.metadata_size);
        uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);
        uint64_t num_chunks = buffer_pool_.GetNumChunks(data_size);
        for (uint64_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
          send_service_.post([this, client_id, object_id, data_size, metadata_size,
                              chunk_index, info]() {
            ExecuteSendObject(client_id, object_id, data_size, metadata_size, chunk_index,
                              info);
          });
        }
      },
      []() {
        // Push is best effort, so do nothing here.
        RAY_LOG(ERROR)
            << "Failed to establish connection for Push with remote object manager.";
      }));
}

void ObjectManager::ExecuteSendObject(const ClientID &client_id,
                                      const ObjectID &object_id, uint64_t data_size,
                                      uint64_t metadata_size, uint64_t chunk_index,
                                      const RemoteConnectionInfo &connection_info) {
  RAY_LOG(DEBUG) << "ExecuteSendObject " << client_id << " " << object_id << " "
                 << chunk_index;
  ray::Status status;
  std::shared_ptr<SenderConnection> conn;
  connection_pool_.GetSender(ConnectionPool::ConnectionType::TRANSFER, client_id, &conn);
  if (conn == nullptr) {
    conn =
        CreateSenderConnection(ConnectionPool::ConnectionType::TRANSFER, connection_info);
    connection_pool_.RegisterSender(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                    conn);
    if (conn == nullptr) {
      return;
    }
  }
  status = SendObjectHeaders(object_id, data_size, metadata_size, chunk_index, conn);
  if (!status.ok()) {
    CheckIOError(status, "Push");
  }
}

ray::Status ObjectManager::SendObjectHeaders(const ObjectID &object_id,
                                             uint64_t data_size, uint64_t metadata_size,
                                             uint64_t chunk_index,
                                             std::shared_ptr<SenderConnection> &conn) {
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.GetChunk(object_id, data_size, metadata_size, chunk_index);
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;

  // Fail on status not okay. The object is local, and there is
  // no other anticipated error here.
  RAY_CHECK_OK(chunk_status.second);

  // Create buffer.
  flatbuffers::FlatBufferBuilder fbb;
  // TODO(hme): use to_flatbuf
  auto message = object_manager_protocol::CreatePushRequestMessage(
      fbb, fbb.CreateString(object_id.binary()), chunk_index, data_size, metadata_size);
  fbb.Finish(message);
  ray::Status status = conn->WriteMessage(
      static_cast<int64_t>(object_manager_protocol::MessageType::PushRequest),
      fbb.GetSize(), fbb.GetBufferPointer());
  if (!status.ok()) {
    return status;
  }
  return SendObjectData(object_id, chunk_info, conn);
}

ray::Status ObjectManager::SendObjectData(const ObjectID &object_id,
                                          const ObjectBufferPool::ChunkInfo &chunk_info,
                                          std::shared_ptr<SenderConnection> &conn) {
  boost::system::error_code error;
  std::vector<asio::const_buffer> buffer;
  buffer.push_back(asio::buffer(chunk_info.data, chunk_info.buffer_length));
  Status status = conn->WriteBuffer(buffer);

  // Do this regardless of whether it failed or succeeded.
  buffer_pool_.ReleaseGetChunk(object_id, chunk_info.chunk_index);

  if (status.ok()) {
    connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::TRANSFER, conn);
    RAY_LOG(DEBUG) << "SendCompleted " << client_id_ << " " << object_id << " "
                   << config_.max_sends;
  }
  return status;
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
  UniqueID wait_id = UniqueID::from_random();
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
          object_id, [this, wait_id](const std::vector<ClientID> &client_ids,
                                     const ObjectID &lookup_object_id) {
            auto &wait_state = active_wait_requests_.find(wait_id)->second;
            if (!client_ids.empty()) {
              wait_state.remaining.erase(lookup_object_id);
              wait_state.found.insert(lookup_object_id);
            }
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
  } else {
    // Wait may complete during the execution of any one of the following calls to
    // SubscribeObjectLocations, so copy the object ids that need to be iterated over.
    // Order matters for test purposes.
    std::vector<ObjectID> ordered_remaining_object_ids;
    for (const auto &object_id : wait_state.object_id_order) {
      if (wait_state.remaining.count(object_id) > 0) {
        ordered_remaining_object_ids.push_back(object_id);
      }
    }
    for (const auto &object_id : ordered_remaining_object_ids) {
      if (active_wait_requests_.find(wait_id) == active_wait_requests_.end()) {
        // This is possible if an object's location is obtained immediately,
        // within the current callstack. In this case, WaitComplete has been
        // invoked already, so we're done.
        return;
      }
      wait_state.requested_objects.insert(object_id);
      // Subscribe to object notifications.
      RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
          wait_id, object_id, [this, wait_id](const std::vector<ClientID> &client_ids,
                                              const ObjectID &subscribe_object_id) {
            if (!client_ids.empty()) {
              auto object_id_wait_state = active_wait_requests_.find(wait_id);
              // We never expect to handle a subscription notification for a wait that has
              // already completed.
              RAY_CHECK(object_id_wait_state != active_wait_requests_.end());
              auto &wait_state = object_id_wait_state->second;
              RAY_CHECK(wait_state.remaining.erase(subscribe_object_id));
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
    if (wait_state.timeout_ms != -1) {
      auto timeout = boost::posix_time::milliseconds(wait_state.timeout_ms);
      wait_state.timeout_timer->expires_from_now(timeout);
      wait_state.timeout_timer->async_wait(
          [this, wait_id](const boost::system::error_code &error_code) {
            if (error_code.value() != 0) {
              return;
            }
            WaitComplete(wait_id);
          });
    }
  }
}

void ObjectManager::WaitComplete(const UniqueID &wait_id) {
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
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
}

std::shared_ptr<SenderConnection> ObjectManager::CreateSenderConnection(
    ConnectionPool::ConnectionType type, RemoteConnectionInfo info) {
  std::shared_ptr<SenderConnection> conn =
      SenderConnection::Create(*main_service_, info.client_id, info.ip, info.port);
  if (conn == nullptr) {
    RAY_LOG(ERROR) << "Failed to connect to remote object manager.";
    return conn;
  }
  // Prepare client connection info buffer
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = (type == ConnectionPool::ConnectionType::TRANSFER);
  auto message = object_manager_protocol::CreateConnectClientMessage(
      fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Send synchronously.
  RAY_CHECK_OK(conn->WriteMessage(
      static_cast<int64_t>(object_manager_protocol::MessageType::ConnectClient),
      fbb.GetSize(), fbb.GetBufferPointer()));
  // The connection is ready; return to caller.
  return conn;
}

void ObjectManager::ProcessNewClient(TcpClientConnection &conn) {
  conn.ProcessMessages();
}

void ObjectManager::ProcessClientMessage(std::shared_ptr<TcpClientConnection> &conn,
                                         int64_t message_type, const uint8_t *message) {
  switch (message_type) {
  case static_cast<int64_t>(object_manager_protocol::MessageType::PushRequest): {
    ReceivePushRequest(conn, message);
    break;
  }
  case static_cast<int64_t>(object_manager_protocol::MessageType::PullRequest): {
    ReceivePullRequest(conn, message);
    break;
  }
  case static_cast<int64_t>(object_manager_protocol::MessageType::ConnectClient): {
    ConnectClient(conn, message);
    break;
  }
  case static_cast<int64_t>(object_manager_protocol::MessageType::FreeRequest): {
    ReceiveFreeRequest(conn, message);
    break;
  }
  case static_cast<int64_t>(protocol::MessageType::DisconnectClient): {
    // TODO(hme): Disconnect without depending on the node manager protocol.
    DisconnectClient(conn, message);
    break;
  }
  default: { RAY_LOG(FATAL) << "invalid request " << message_type; }
  }
}

void ObjectManager::ConnectClient(std::shared_ptr<TcpClientConnection> &conn,
                                  const uint8_t *message) {
  // TODO: trash connection on failure.
  auto info =
      flatbuffers::GetRoot<object_manager_protocol::ConnectClientMessage>(message);
  ClientID client_id = ObjectID::from_binary(info->client_id()->str());
  bool is_transfer = info->is_transfer();
  conn->SetClientID(client_id);
  if (is_transfer) {
    connection_pool_.RegisterReceiver(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                      conn);
  } else {
    connection_pool_.RegisterReceiver(ConnectionPool::ConnectionType::MESSAGE, client_id,
                                      conn);
  }
  conn->ProcessMessages();
}

void ObjectManager::DisconnectClient(std::shared_ptr<TcpClientConnection> &conn,
                                     const uint8_t *message) {
  connection_pool_.RemoveReceiver(conn);
}

void ObjectManager::ReceivePullRequest(std::shared_ptr<TcpClientConnection> &conn,
                                       const uint8_t *message) {
  // Serialize and push object to requesting client.
  auto pr = flatbuffers::GetRoot<object_manager_protocol::PullRequestMessage>(message);
  ObjectID object_id = ObjectID::from_binary(pr->object_id()->str());
  ClientID client_id = ClientID::from_binary(pr->client_id()->str());
  Push(object_id, client_id);
  conn->ProcessMessages();
}

void ObjectManager::ReceivePushRequest(std::shared_ptr<TcpClientConnection> &conn,
                                       const uint8_t *message) {
  // Serialize.
  auto object_header =
      flatbuffers::GetRoot<object_manager_protocol::PushRequestMessage>(message);
  ObjectID object_id = ObjectID::from_binary(object_header->object_id()->str());
  uint64_t chunk_index = object_header->chunk_index();
  uint64_t data_size = object_header->data_size();
  uint64_t metadata_size = object_header->metadata_size();
  receive_service_.post([this, object_id, data_size, metadata_size, chunk_index, conn]() {
    ExecuteReceiveObject(conn->GetClientID(), object_id, data_size, metadata_size,
                         chunk_index, *conn);
  });
}

void ObjectManager::ExecuteReceiveObject(const ClientID &client_id,
                                         const ObjectID &object_id, uint64_t data_size,
                                         uint64_t metadata_size, uint64_t chunk_index,
                                         TcpClientConnection &conn) {
  RAY_LOG(DEBUG) << "ExecuteReceiveObject " << client_id << " " << object_id << " "
                 << chunk_index;

  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.CreateChunk(object_id, data_size, metadata_size, chunk_index);
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;
  if (chunk_status.second.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    std::vector<boost::asio::mutable_buffer> buffer;
    buffer.push_back(asio::buffer(chunk_info.data, chunk_info.buffer_length));
    boost::system::error_code ec;
    conn.ReadBuffer(buffer, ec);
    if (ec.value() == boost::system::errc::success) {
      buffer_pool_.SealChunk(object_id, chunk_index);
    } else {
      buffer_pool_.AbortCreateChunk(object_id, chunk_index);
      // TODO(hme): This chunk failed, so create a pull request for this chunk.
    }
  } else {
    RAY_LOG(ERROR) << "Create Chunk Failed index = " << chunk_index << ": "
                   << chunk_status.second.message();
    // Read object into empty buffer.
    uint64_t buffer_length = buffer_pool_.GetBufferLength(chunk_index, data_size);
    std::vector<uint8_t> mutable_vec;
    mutable_vec.resize(buffer_length);
    std::vector<boost::asio::mutable_buffer> buffer;
    buffer.push_back(asio::buffer(mutable_vec, buffer_length));
    boost::system::error_code ec;
    conn.ReadBuffer(buffer, ec);
    if (ec.value() != boost::system::errc::success) {
      RAY_LOG(ERROR) << boost_to_ray_status(ec).ToString();
    }
    // TODO(hme): If the object isn't local, create a pull request for this chunk.
  }
  conn.ProcessMessages();
  RAY_LOG(DEBUG) << "ReceiveCompleted " << client_id_ << " " << object_id << " "
                 << "/" << config_.max_receives;
}

void ObjectManager::ReceiveFreeRequest(std::shared_ptr<TcpClientConnection> &conn,
                                       const uint8_t *message) {
  auto free_request =
      flatbuffers::GetRoot<object_manager_protocol::FreeRequestMessage>(message);
  std::vector<ObjectID> object_ids = from_flatbuf(*free_request->object_ids());
  // This RPC should come from another Object Manager.
  // Keep this request local.
  bool local_only = true;
  FreeObjects(object_ids, local_only);
  conn->ProcessMessages();
}

void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids,
                                bool local_only) {
  buffer_pool_.FreeObjects(object_ids);
  if (!local_only) {
    SpreadFreeObjectRequest(object_ids);
  }
}

void ObjectManager::SpreadFreeObjectRequest(const std::vector<ObjectID> &object_ids) {
  // This code path should be called from node manager.
  flatbuffers::FlatBufferBuilder fbb;
  flatbuffers::Offset<object_manager_protocol::FreeRequestMessage> request =
      object_manager_protocol::CreateFreeRequestMessage(fbb, to_flatbuf(fbb, object_ids));
  fbb.Finish(request);
  auto function_on_client = [this, &fbb](const RemoteConnectionInfo &connection_info) {
    std::shared_ptr<SenderConnection> conn;
    connection_pool_.GetSender(ConnectionPool::ConnectionType::MESSAGE,
                               connection_info.client_id, &conn);
    if (conn == nullptr) {
      conn = CreateSenderConnection(ConnectionPool::ConnectionType::MESSAGE,
                                    connection_info);
      connection_pool_.RegisterSender(ConnectionPool::ConnectionType::MESSAGE,
                                      connection_info.client_id, conn);
    }
    ray::Status status = conn->WriteMessage(
        static_cast<int64_t>(object_manager_protocol::MessageType::FreeRequest),
        fbb.GetSize(), fbb.GetBufferPointer());
    if (status.ok()) {
      connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::MESSAGE, conn);
    }
    // TODO(Yuhong): Implement ConnectionPool::RemoveSender and call it in "else".
  };
  object_directory_->RunFunctionForEachClient(function_on_client);
}

}  // namespace ray
