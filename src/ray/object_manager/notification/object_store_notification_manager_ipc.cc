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

#include "ray/object_manager/notification/object_store_notification_manager_ipc.h"

#include "ray/common/client_connection.h"
#include "ray/object_manager/plasma/plasma_generated.h"

namespace ray {

ObjectStoreNotificationManagerIPC::ObjectStoreNotificationManagerIPC(
    boost::asio::io_service &io_service, const std::string &store_socket_name,
    bool exit_on_error)
    : ObjectStoreNotificationManager(io_service),
      length_(0),
      exit_on_error_(exit_on_error) {
  local_stream_socket socket(io_service);
  RAY_CHECK_OK(ConnectSocketRetry(socket, store_socket_name));
  store_client_ = ServerConnection::Create(std::move(socket));
  // Subscribe messages.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = plasma::flatbuf::CreatePlasmaSubscribeRequest(fbb);
  fbb.Finish(message);
  RAY_CHECK_OK(store_client_->WriteMessage(
      static_cast<int64_t>(plasma::flatbuf::MessageType::PlasmaSubscribeRequest),
      fbb.GetSize(), fbb.GetBufferPointer()));
  RAY_CHECK_OK(store_client_->SetNonBlocking(true));
  NotificationWait();
}

ObjectStoreNotificationManagerIPC::~ObjectStoreNotificationManagerIPC() {
  store_client_->Close();
}

void ObjectStoreNotificationManagerIPC::Shutdown() { store_client_->Close(); }

void ObjectStoreNotificationManagerIPC::NotificationWait() {
  store_client_->ReadBufferAsync({boost::asio::buffer(&length_, sizeof(length_))},
                                 [this](const ray::Status &s) { ProcessStoreLength(s); });
}

void ObjectStoreNotificationManagerIPC::ProcessStoreLength(const ray::Status &s) {
  notification_.resize(length_);
  if (!s.ok()) {
    if (exit_on_error_) {
      // When shutting down a cluster, it's possible that the plasma store is killed
      // earlier than raylet. In this case we don't want raylet to crash, we instead
      // log an error message and exit.
      RAY_LOG(ERROR) << "Failed to process store length: " << s.ToString()
                     << ", most likely plasma store is down, raylet will exit";
      // Exit raylet process.
      _exit(kRayletStoreErrorExitCode);
    } else {
      // The log level is set to debug so user don't see it on ctrl+c exit.
      RAY_LOG(DEBUG) << "Failed to process store length: " << s.ToString()
                     << ", most likely plasma store is down. "
                     << "The error is silenced because exit_on_error_ "
                     << "flag is set.";
      return;
    }
  }

  store_client_->ReadBufferAsync(
      {boost::asio::buffer(notification_)},
      [this](const ray::Status &s) { ProcessStoreNotification(s); });
}

void ObjectStoreNotificationManagerIPC::ProcessStoreNotification(const ray::Status &s) {
  if (!s.ok()) {
    if (exit_on_error_) {
      RAY_LOG(FATAL)
          << "Problem communicating with the object store from raylet, check logs or "
          << "dmesg for previous errors: " << s.ToString();
    } else {
      // The log level is set to debug so user don't see it on ctrl+c exit.
      RAY_LOG(DEBUG)
          << "Problem communicating with the object store from raylet, check logs or "
          << "dmesg for previous errors: " << s.ToString()
          << " The error is silenced because exit_on_error_ "
          << "flag is set.";
      return;
    }
  }

  const auto &object_notification =
      flatbuffers::GetRoot<object_manager::protocol::PlasmaNotification>(
          notification_.data());
  for (size_t i = 0; i < object_notification->object_info()->size(); ++i) {
    auto object_info = object_notification->object_info()->Get(i);
    const ObjectID object_id = ObjectID::FromBinary(object_info->object_id()->str());
    if (object_info->is_deletion()) {
      ProcessStoreRemove(object_id);
    } else {
      object_manager::protocol::ObjectInfoT result;
      object_info->UnPackTo(&result);
      ProcessStoreAdd(result);
    }
  }
  NotificationWait();
}

}  // namespace ray
