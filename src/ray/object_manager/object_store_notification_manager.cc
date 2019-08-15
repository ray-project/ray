#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "ray/common/status.h"

#include "ray/common/common_protocol.h"
#include "ray/object_manager/object_store_notification_manager.h"
#include "ray/util/util.h"

namespace ray {

ObjectStoreNotificationManager::ObjectStoreNotificationManager(
    boost::asio::io_service &io_service, const std::string &store_socket_name)
    : store_client_(),
      length_(0),
      num_adds_processed_(0),
      num_removes_processed_(0),
      socket_(io_service) {
  RAY_ARROW_CHECK_OK(store_client_.Connect(store_socket_name.c_str(), "", 0, 300));

  RAY_ARROW_CHECK_OK(store_client_.Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
}

ObjectStoreNotificationManager::~ObjectStoreNotificationManager() {
  RAY_ARROW_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotificationManager::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreNotificationManager::ProcessStoreLength,
                                      this, boost::asio::placeholders::error));
}

void ObjectStoreNotificationManager::ProcessStoreLength(
    const boost::system::error_code &error) {
  notification_.resize(length_);
  if (error) {
    RAY_LOG(FATAL)
        << "Problem communicating with the object store from raylet, check logs or "
        << "dmesg for previous errors: " << boost_to_ray_status(error).ToString();
  }
  boost::asio::async_read(
      socket_, boost::asio::buffer(notification_),
      boost::bind(&ObjectStoreNotificationManager::ProcessStoreNotification, this,
                  boost::asio::placeholders::error));
}

void ObjectStoreNotificationManager::ProcessStoreNotification(
    const boost::system::error_code &error) {
  if (error) {
    RAY_LOG(FATAL)
        << "Problem communicating with the object store from raylet, check logs or "
        << "dmesg for previous errors: " << boost_to_ray_status(error).ToString();
  }

  const auto &object_info =
      flatbuffers::GetRoot<object_manager::protocol::ObjectInfo>(notification_.data());
  const ObjectID object_id =
      ObjectID::FromPlasmaIdBinary(object_info->object_id()->str());
  if (object_info->is_deletion()) {
    ProcessStoreRemove(object_id);
  } else {
    object_manager::protocol::ObjectInfoT result;
    object_info->UnPackTo(&result);
    ProcessStoreAdd(result);
  }
  NotificationWait();
}

void ObjectStoreNotificationManager::ProcessStoreAdd(
    const object_manager::protocol::ObjectInfoT &object_info) {
  for (auto &handler : add_handlers_) {
    handler(object_info);
  }
  num_adds_processed_++;
}

void ObjectStoreNotificationManager::ProcessStoreRemove(const ObjectID &object_id) {
  for (auto &handler : rem_handlers_) {
    handler(object_id);
  }
  num_removes_processed_++;
}

void ObjectStoreNotificationManager::SubscribeObjAdded(
    std::function<void(const object_manager::protocol::ObjectInfoT &)> callback) {
  add_handlers_.push_back(std::move(callback));
}

void ObjectStoreNotificationManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  rem_handlers_.push_back(std::move(callback));
}

std::string ObjectStoreNotificationManager::DebugString() const {
  std::stringstream result;
  result << "ObjectStoreNotificationManager:";
  result << "\n- num adds processed: " << num_adds_processed_;
  result << "\n- num removes processed: " << num_removes_processed_;
  return result.str();
}

}  // namespace ray
