#include "ray/object_manager/object_manager.h"

namespace asio = boost::asio;

namespace object_manager_protocol = ray::object_manager::protocol;

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
      [this](const ObjectInfoT &object_info) { NotifyDirectoryObjectAdd(object_info); });
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
      [this](const ObjectInfoT &object_info) { NotifyDirectoryObjectAdd(object_info); });
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

void ObjectManager::NotifyDirectoryObjectAdd(const ObjectInfoT &object_info) {
  ObjectID object_id = ObjectID::from_binary(object_info.object_id);
  local_objects_[object_id] = object_info;
  ray::Status status =
      object_directory_->ReportObjectAdded(object_id, client_id_, object_info);
  // Handle the unfulfilled_push_tasks_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_tasks_.find(object_id);
  if (iter != unfulfilled_push_tasks_.end()) {
    for (auto &pair : iter->second) {
      main_service_->post(
          [this, object_id, pair]() { RAY_CHECK_OK(Push(object_id, pair.first)); });
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_tasks_.erase(iter);
  }
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
  ray::Status status_code = object_directory_->SubscribeObjectLocations(
      object_id,
      [this](const std::vector<ClientID> &client_ids, const ObjectID &object_id) {
        RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(object_id));
        GetLocationsSuccess(client_ids, object_id);
      });
  return status_code;
}

void ObjectManager::GetLocationsSuccess(const std::vector<ray::ClientID> &client_ids,
                                        const ray::ObjectID &object_id) {
  if (local_objects_.count(object_id) == 0) {
    // Only pull objects that aren't local.
    RAY_CHECK(!client_ids.empty());
    ClientID client_id = client_ids.front();
    ray::Status status_code = Pull(object_id, client_id);
    RAY_CHECK_OK(status_code);
  }
}

ray::Status ObjectManager::Pull(const ObjectID &object_id, const ClientID &client_id) {
  // Check if object is already local.
  if (local_objects_.count(object_id) != 0) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return ray::Status::OK();
  }
  // Check if we're pulling from self.
  if (client_id == client_id_) {
    RAY_LOG(ERROR) << client_id_ << " attempted to pull an object from itself.";
    return ray::Status::Invalid("A node cannot pull an object from itself.");
  }
  return PullEstablishConnection(object_id, client_id);
};

ray::Status ObjectManager::PullEstablishConnection(const ObjectID &object_id,
                                                   const ClientID &client_id) {
  // Acquire a message connection and send pull request.
  ray::Status status;
  std::shared_ptr<SenderConnection> conn;
  // TODO(hme): There is no cap on the number of pull request connections.
  status = connection_pool_.GetSender(ConnectionPool::ConnectionType::MESSAGE, client_id,
                                      &conn);
  // Currently, acquiring a connection should not fail.
  // No status from GetSender is returned which can be
  // handled without failing.
  RAY_CHECK_OK(status);

  if (conn == nullptr) {
    status = object_directory_->GetInformation(
        client_id,
        [this, object_id, client_id](const RemoteConnectionInfo &connection_info) {
          std::shared_ptr<SenderConnection> async_conn = CreateSenderConnection(
              ConnectionPool::ConnectionType::MESSAGE, connection_info);
          connection_pool_.RegisterSender(ConnectionPool::ConnectionType::MESSAGE,
                                          client_id, async_conn);
          Status pull_send_status = PullSendRequest(object_id, async_conn);
          RAY_CHECK_OK(pull_send_status);
        },
        [](const Status &status) {
          RAY_LOG(ERROR) << "Failed to establish connection with remote object manager.";
          RAY_CHECK_OK(status);
        });
  } else {
    status = PullSendRequest(object_id, conn);
  }
  return status;
}

ray::Status ObjectManager::PullSendRequest(const ObjectID &object_id,
                                           std::shared_ptr<SenderConnection> &conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = object_manager_protocol::CreatePullRequestMessage(
      fbb, fbb.CreateString(client_id_.binary()), fbb.CreateString(object_id.binary()));
  fbb.Finish(message);
  RAY_CHECK_OK(conn->WriteMessage(object_manager_protocol::MessageType_PullRequest,
                                  fbb.GetSize(), fbb.GetBufferPointer()));
  RAY_CHECK_OK(
      connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::MESSAGE, conn));
  return ray::Status::OK();
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const ClientID &client_id) {
  RAY_LOG(ERROR) << "Invalid Push request ObjectID: " << object_id
                 << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_tasks_.find(object_id);
  if (iter != unfulfilled_push_tasks_.end()) {
    iter->second.erase(client_id);
    if (iter->second.size() == 0) {
      unfulfilled_push_tasks_.erase(iter);
    }
  }
}

ray::Status ObjectManager::Push(const ObjectID &object_id, const ClientID &client_id) {
  if (local_objects_.count(object_id) == 0) {
    // Avoid setting duplicated timer for the same object and client pair.
    auto &clients = unfulfilled_push_tasks_[object_id];
    if (clients.count(client_id) == 0) {
      // If config_.push_timeout_ms < 0, we give an empty timer
      // and the task will be kept infinitely.
      auto timer = std::shared_ptr<boost::asio::deadline_timer>();
      if (config_.push_timeout_ms == 0) {
        // The Push request fails directly when config_.push_timeout_ms == 0.
        RAY_LOG(ERROR) << "Invalid Push request ObjectID: " << object_id
                       << " due to direct failure setting. ";
        return ray::Status::OK();
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
      clients.emplace(client_id, timer);
    }
    return ray::Status::OK();
  }

  // TODO(hme): Cache this data in ObjectDirectory.
  // Okay for now since the GCS client caches this data.
  Status status = object_directory_->GetInformation(
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
      [](const Status &status) {
        // Push is best effort, so do nothing here.
      });
  return status;
}

void ObjectManager::ExecuteSendObject(const ClientID &client_id,
                                      const ObjectID &object_id, uint64_t data_size,
                                      uint64_t metadata_size, uint64_t chunk_index,
                                      const RemoteConnectionInfo &connection_info) {
  RAY_LOG(DEBUG) << "ExecuteSendObject " << client_id << " " << object_id << " "
                 << chunk_index;
  ray::Status status;
  std::shared_ptr<SenderConnection> conn;
  status = connection_pool_.GetSender(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                      &conn);
  if (conn == nullptr) {
    conn =
        CreateSenderConnection(ConnectionPool::ConnectionType::TRANSFER, connection_info);
    connection_pool_.RegisterSender(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                    conn);
  }
  status = SendObjectHeaders(object_id, data_size, metadata_size, chunk_index, conn);
  RAY_CHECK_OK(status);
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
  ray::Status status =
      conn->WriteMessage(object_manager_protocol::MessageType_PushRequest, fbb.GetSize(),
                         fbb.GetBufferPointer());
  RAY_CHECK_OK(status);
  return SendObjectData(object_id, chunk_info, conn);
}

ray::Status ObjectManager::SendObjectData(const ObjectID &object_id,
                                          const ObjectBufferPool::ChunkInfo &chunk_info,
                                          std::shared_ptr<SenderConnection> &conn) {
  boost::system::error_code ec;
  std::vector<asio::const_buffer> buffer;
  buffer.push_back(asio::buffer(chunk_info.data, chunk_info.buffer_length));
  conn->WriteBuffer(buffer, ec);

  ray::Status status = ray::Status::OK();
  if (ec.value() != 0) {
    // Push failed. Deal with partial objects on the receiving end.
    // TODO(hme): Try to invoke disconnect on sender connection, then remove it.
    status = ray::Status::IOError(ec.message());
  }

  // Do this regardless of whether it failed or succeeded.
  buffer_pool_.ReleaseGetChunk(object_id, chunk_info.chunk_index);
  RAY_CHECK_OK(
      connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::TRANSFER, conn));
  RAY_LOG(DEBUG) << "SendCompleted " << client_id_ << " " << object_id << " "
                 << config_.max_sends;
  return status;
}

ray::Status ObjectManager::Cancel(const ObjectID &object_id) {
  ray::Status status = object_directory_->UnsubscribeObjectLocations(object_id);
  return status;
}

ray::Status ObjectManager::Wait(const std::vector<ObjectID> &object_ids,
                                uint64_t timeout_ms, int num_ready_objects,
                                const WaitCallback &callback) {
  // TODO: Implement wait.
  return ray::Status::OK();
}

std::shared_ptr<SenderConnection> ObjectManager::CreateSenderConnection(
    ConnectionPool::ConnectionType type, RemoteConnectionInfo info) {
  std::shared_ptr<SenderConnection> conn =
      SenderConnection::Create(*main_service_, info.client_id, info.ip, info.port);
  // Prepare client connection info buffer
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = (type == ConnectionPool::ConnectionType::TRANSFER);
  auto message = object_manager_protocol::CreateConnectClientMessage(
      fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Send synchronously.
  RAY_CHECK_OK(conn->WriteMessage(object_manager_protocol::MessageType_ConnectClient,
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
  case object_manager_protocol::MessageType_PushRequest: {
    ReceivePushRequest(conn, message);
    break;
  }
  case object_manager_protocol::MessageType_PullRequest: {
    ReceivePullRequest(conn, message);
    break;
  }
  case object_manager_protocol::MessageType_ConnectClient: {
    ConnectClient(conn, message);
    break;
  }
  case protocol::MessageType_DisconnectClient: {
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
  ray::Status push_status = Push(object_id, client_id);
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
    if (ec.value() == 0) {
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
    if (ec.value() != 0) {
      RAY_LOG(ERROR) << ec.message();
    }
    // TODO(hme): If the object isn't local, create a pull request for this chunk.
  }
  conn.ProcessMessages();
  RAY_LOG(DEBUG) << "ReceiveCompleted " << client_id_ << " " << object_id << " "
                 << "/" << config_.max_receives;
}

}  // namespace ray
