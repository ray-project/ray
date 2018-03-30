#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/common.h"
#include "common/common_protocol.h"

#include "ray/object_manager/object_store_notification_manager.h"

namespace ray {

ObjectStoreNotificationManager::ObjectStoreNotificationManager(
    boost::asio::io_service &io_service, const std::string &store_socket_name)
    : store_client_(), socket_(io_service) {
  ARROW_CHECK_OK(
      store_client_.Connect(store_socket_name.c_str(), "", PLASMA_DEFAULT_RELEASE_DELAY));

  ARROW_CHECK_OK(store_client_.Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
}

void ObjectStoreNotificationManager::Terminate() {
  ARROW_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotificationManager::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreNotificationManager::ProcessStoreLength,
                                      this, boost::asio::placeholders::error));
}

void ObjectStoreNotificationManager::ProcessStoreLength(
    const boost::system::error_code &error) {
  notification_.resize(length_);
  boost::asio::async_read(
      socket_, boost::asio::buffer(notification_),
      boost::bind(&ObjectStoreNotificationManager::ProcessStoreNotification, this,
                  boost::asio::placeholders::error));
}

void ObjectStoreNotificationManager::ProcessStoreNotification(
    const boost::system::error_code &error) {
  if (error) {
    throw std::runtime_error("ObjectStore may have died.");
  }

  const auto &object_info = flatbuffers::GetRoot<ObjectInfo>(notification_.data());
  const auto &object_id = from_flatbuf(*object_info->object_id());
  if (object_info->is_deletion()) {
    ProcessStoreRemove(object_id);
  } else {
    ProcessStoreAdd(object_id);
    // TODO(hme): Determine what data is actually needed by consumer of this notification.
    //    ProcessStoreAdd(
    //        object_id, object_info->data_size(),
    //        object_info->metadata_size(),
    //        (unsigned char *) object_info->digest()->data());
  }
  NotificationWait();
}

void ObjectStoreNotificationManager::ProcessStoreAdd(const ObjectID &object_id) {
  for (auto handler : add_handlers_) {
    handler(object_id);
  }
}

void ObjectStoreNotificationManager::ProcessStoreRemove(const ObjectID &object_id) {
  for (auto handler : rem_handlers_) {
    handler(object_id);
  }
}

void ObjectStoreNotificationManager::SubscribeObjAdded(
    std::function<void(const ObjectID &)> callback) {
  add_handlers_.push_back(callback);
}

void ObjectStoreNotificationManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  rem_handlers_.push_back(callback);
}

}  // namespace ray
