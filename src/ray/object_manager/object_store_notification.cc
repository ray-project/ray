#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common.h"
#include "common_protocol.h"
#include "ray/object_manager/object_store_notification.h"

namespace ray {

// TODO(hme): Dedicate this class to notifications.
// TODO(hme): Create object store client pool for object manager.
ObjectStoreNotification::ObjectStoreNotification(boost::asio::io_service &io_service, 
                                                 std::string &store_socket_name)
    : store_client_(), socket_(io_service) {
  ARROW_CHECK_OK(
      store_client_.Connect(store_socket_name.c_str(), "", PLASMA_DEFAULT_RELEASE_DELAY));

  // Connect to two clients, but subscribe to only one.
  ARROW_CHECK_OK(store_client_.Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
};

void ObjectStoreNotification::Terminate() {
  ARROW_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotification::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreNotification::ProcessStoreLength, this,
                                      boost::asio::placeholders::error));
}

void ObjectStoreNotification::ProcessStoreLength(const boost::system::error_code &error) {
  notification_.resize(length_);
  boost::asio::async_read(socket_, boost::asio::buffer(notification_),
                          boost::bind(&ObjectStoreNotification::ProcessStoreNotification, this,
                                      boost::asio::placeholders::error));
}

void ObjectStoreNotification::ProcessStoreNotification(const boost::system::error_code &error) {
  if (error) {
    throw std::runtime_error("ObjectStore may have died.");
  }

  auto object_info = flatbuffers::GetRoot<ObjectInfo>(notification_.data());
  ObjectID object_id = from_flatbuf(*object_info->object_id());
  if (object_info->is_deletion()) {
    ProcessStoreRemove(object_id);
  } else {
    ProcessStoreAdd(object_id);
    // why all these params?
    //    ProcessStoreAdd(
    //        object_id, object_info->data_size(),
    //        object_info->metadata_size(),
    //        (unsigned char *) object_info->digest()->data());
  }
  NotificationWait();
}

void ObjectStoreNotification::ProcessStoreAdd(const ObjectID &object_id) {
  for (auto handler : add_handlers_) {
    handler(object_id);
  }
};

void ObjectStoreNotification::ProcessStoreRemove(const ObjectID &object_id) {
  for (auto handler : rem_handlers_) {
    handler(object_id);
  }
};

void ObjectStoreNotification::SubscribeObjAdded(
    std::function<void(const ObjectID &)> callback) {
  add_handlers_.push_back(callback);
};

void ObjectStoreNotification::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  rem_handlers_.push_back(callback);
};

}  // namespace ray
