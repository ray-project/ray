#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common.h"
#include "common_protocol.h"
#include "ray/object_manager/object_store_client.h"

namespace ray {

// TODO(hme): Dedicate this class to notifications.
// TODO(hme): Create object store client pool for object manager.
ObjectStoreClient::ObjectStoreClient(boost::asio::io_service &io_service,
                                     std::string &store_socket_name)
    : client_one_(), client_two_(), socket_(io_service) {
  ARROW_CHECK_OK(
      client_two_.Connect(store_socket_name.c_str(), "", PLASMA_DEFAULT_RELEASE_DELAY));
  ARROW_CHECK_OK(
      client_one_.Connect(store_socket_name.c_str(), "", PLASMA_DEFAULT_RELEASE_DELAY));

  // Connect to two clients, but subscribe to only one.
  ARROW_CHECK_OK(client_one_.Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
};

void ObjectStoreClient::Terminate() {
  ARROW_CHECK_OK(client_two_.Disconnect());
  ARROW_CHECK_OK(client_one_.Disconnect());
}

void ObjectStoreClient::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreClient::ProcessStoreLength, this,
                                      boost::asio::placeholders::error));
}

void ObjectStoreClient::ProcessStoreLength(const boost::system::error_code &error) {
  notification_.resize(length_);
  boost::asio::async_read(socket_, boost::asio::buffer(notification_),
                          boost::bind(&ObjectStoreClient::ProcessStoreNotification, this,
                                      boost::asio::placeholders::error));
}

void ObjectStoreClient::ProcessStoreNotification(const boost::system::error_code &error) {
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

void ObjectStoreClient::ProcessStoreAdd(const ObjectID &object_id) {
  for (auto handler : add_handlers_) {
    handler(object_id);
  }
};

void ObjectStoreClient::ProcessStoreRemove(const ObjectID &object_id) {
  for (auto handler : rem_handlers_) {
    handler(object_id);
  }
};

void ObjectStoreClient::SubscribeObjAdded(
    std::function<void(const ObjectID &)> callback) {
  add_handlers_.push_back(callback);
};

void ObjectStoreClient::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  rem_handlers_.push_back(callback);
};

plasma::PlasmaClient &ObjectStoreClient::GetClient() { return client_one_; };

plasma::PlasmaClient &ObjectStoreClient::GetClientOther() { return client_two_; };
}  // namespace ray
