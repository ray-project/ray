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

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "ray/common/common_protocol.h"
#include "ray/common/status.h"
#include "ray/util/util.h"

#ifdef _WIN32
#include <win32fd.h>
#endif

namespace ray {

ObjectStoreNotificationManagerIPC::ObjectStoreNotificationManagerIPC(
    boost::asio::io_service &io_service, const std::string &store_socket_name,
    bool exit_on_error)
    : ObjectStoreNotificationManager(io_service),
      store_client_(),
      length_(0),
      socket_(io_service),
      exit_on_error_(exit_on_error) {
  RAY_CHECK_OK(store_client_.Connect(store_socket_name.c_str(), "", 0, 300));

  int fd;
  RAY_CHECK_OK(store_client_.Subscribe(&fd));
  boost::system::error_code ec;
#ifdef _WIN32
  boost::asio::detail::socket_type c_socket = fh_release(fd);
  WSAPROTOCOL_INFO pi;
  size_t n = sizeof(pi);
  char *p = reinterpret_cast<char *>(&pi);
  const int level = SOL_SOCKET;
  const int opt = SO_PROTOCOL_INFO;
  if (boost::asio::detail::socket_ops::getsockopt(c_socket, 0, level, opt, p, &n, ec) !=
      boost::asio::detail::socket_error_retval) {
    switch (pi.iAddressFamily) {
    case AF_INET:
      socket_.assign(boost::asio::ip::tcp::v4(), c_socket, ec);
      break;
    case AF_INET6:
      socket_.assign(boost::asio::ip::tcp::v6(), c_socket, ec);
      break;
    default:
      ec = boost::system::errc::make_error_code(
          boost::system::errc::address_family_not_supported);
      break;
    }
  }
#else
  socket_.assign(boost::asio::local::stream_protocol(), fd, ec);
#endif
  RAY_CHECK(!ec);
  NotificationWait();
}

ObjectStoreNotificationManagerIPC::~ObjectStoreNotificationManagerIPC() {
  RAY_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotificationManagerIPC::Shutdown() {
  RAY_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotificationManagerIPC::NotificationWait() {
  boost::asio::async_read(
      socket_, boost::asio::buffer(&length_, sizeof(length_)),
      boost::bind(&ObjectStoreNotificationManagerIPC::ProcessStoreLength, this,
                  boost::asio::placeholders::error));
}

void ObjectStoreNotificationManagerIPC::ProcessStoreLength(
    const boost::system::error_code &error) {
  notification_.resize(length_);
  if (error) {
    if (exit_on_error_) {
      // When shutting down a cluster, it's possible that the plasma store is killed
      // earlier than raylet. In this case we don't want raylet to crash, we instead
      // log an error message and exit.
      RAY_LOG(ERROR) << "Failed to process store length: "
                     << boost_to_ray_status(error).ToString()
                     << ", most likely plasma store is down, raylet will exit";
      // Exit raylet process.
      _exit(kRayletStoreErrorExitCode);
    } else {
      // The log level is set to debug so user don't see it on ctrl+c exit.
      RAY_LOG(DEBUG) << "Failed to process store length: "
                     << boost_to_ray_status(error).ToString()
                     << ", most likely plasma store is down. "
                     << "The error is silenced because exit_on_error_ "
                     << "flag is set.";
      return;
    }
  }

  boost::asio::async_read(
      socket_, boost::asio::buffer(notification_),
      boost::bind(&ObjectStoreNotificationManagerIPC::ProcessStoreNotification, this,
                  boost::asio::placeholders::error));
}

void ObjectStoreNotificationManagerIPC::ProcessStoreNotification(
    const boost::system::error_code &error) {
  if (error) {
    if (exit_on_error_) {
      RAY_LOG(FATAL)
          << "Problem communicating with the object store from raylet, check logs or "
          << "dmesg for previous errors: " << boost_to_ray_status(error).ToString();
    } else {
      // The log level is set to debug so user don't see it on ctrl+c exit.
      RAY_LOG(DEBUG)
          << "Problem communicating with the object store from raylet, check logs or "
          << "dmesg for previous errors: " << boost_to_ray_status(error).ToString()
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
