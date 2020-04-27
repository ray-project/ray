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

#ifndef RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H
#define RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H

#include <list>
#include <memory>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"

#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/status.h"

#include "ray/object_manager/object_directory.h"

namespace ray {

/// \class ObjectStoreClientPool
///
/// Encapsulates notification handling from the object store.
class ObjectStoreNotificationManager {
 public:
  /// Constructor.
  ///
  /// \param io_service The asio service to be used.
  /// \param store_socket_name The store socket to connect to.
  /// \param exit_on_error The manager will exit with error when it fails
  ///                      to process messages from socket.
  ObjectStoreNotificationManager(boost::asio::io_service &io_service,
                                 const std::string &store_socket_name,
                                 bool exit_on_error = true);

  ~ObjectStoreNotificationManager();

  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  /// already exist in the local store
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeObjAdded(
      std::function<void(const object_manager::protocol::ObjectInfoT &)> callback);

  /// Subscribe to notifications of objects deleted from local store.
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeObjDeleted(std::function<void(const ray::ObjectID &)> callback);

  /// Explicitly shutdown the manager.
  void Shutdown();

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 private:
  /// Async loop for handling object store notifications.
  void NotificationWait();
  void ProcessStoreLength(const boost::system::error_code &error);
  void ProcessStoreNotification(const boost::system::error_code &error);

  /// Support for rebroadcasting object add/rem events.
  void ProcessStoreAdd(const object_manager::protocol::ObjectInfoT &object_info);
  void ProcessStoreRemove(const ObjectID &object_id);

  std::vector<std::function<void(const object_manager::protocol::ObjectInfoT &)>>
      add_handlers_;
  std::vector<std::function<void(const ray::ObjectID &)>> rem_handlers_;

  plasma::PlasmaClient store_client_;
  int64_t length_;
  int64_t num_adds_processed_;
  int64_t num_removes_processed_;
  std::vector<uint8_t> notification_;
  local_stream_socket socket_;

  /// Flag to indicate whether or not to exit the process when received socket
  /// error. When it is false, socket error will be ignored. This flag is needed
  /// when running object store notification manager in core worker. On core worker
  /// exit, plasma store will be killed before deconstructor of this manager. So we
  /// we have to silence the errors.
  bool exit_on_error_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H
