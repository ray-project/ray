#include <iostream>
#include "future"
#include "common_protocol.h"

#include <memory>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>

#include "common.h"
// #include "format/om_generated.h"
#include "object_store_client.h"


using namespace std;

namespace ray {

ObjectStoreClient::ObjectStoreClient(
    boost::asio::io_service &io_service, string &store_socket_name
) : socket_(io_service) {
  this->plasma_conn = new plasma::PlasmaClient();
  ARROW_CHECK_OK(this->plasma_conn->Connect(store_socket_name.c_str(),
                                            "",
                                            PLASMA_DEFAULT_RELEASE_DELAY));
  ARROW_CHECK_OK(this->plasma_conn->Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  this->NotificationWait();
};

void ObjectStoreClient::Terminate() {
  ARROW_CHECK_OK(this->plasma_conn->Disconnect());
  delete this->plasma_conn;
}

void ObjectStoreClient::NotificationWait() {
  boost::asio::async_read(socket_,
                          boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreClient::ProcessStoreLength,
                                      this,
                                      boost::asio::placeholders::error));
}

void ObjectStoreClient::ProcessStoreLength(const boost::system::error_code &error) {
  notification_.resize(length_);
  boost::asio::async_read(socket_, boost::asio::buffer(notification_),
                          boost::bind(&ObjectStoreClient::ProcessStoreNotification,
                                      this,
                                      boost::asio::placeholders::error)
  );
}

void ObjectStoreClient::ProcessStoreNotification(const boost::system::error_code &error) {
  if (error) {
    throw std::runtime_error("ObjectStore may have died.");
  }

  auto object_info = flatbuffers::GetRoot<ObjectInfo>(notification_.data());
  ObjectID object_id = from_flatbuf(*object_info->object_id());
  if (object_info->is_deletion()) {
    this->ProcessStoreRemove(object_id);
  } else {
    this->ProcessStoreAdd(object_id);
    // why all these params?
    //    this->ProcessStoreAdd(
    //        object_id, object_info->data_size(),
    //        object_info->metadata_size(),
    //        (unsigned char *) object_info->digest()->data());
  }
  this->NotificationWait();
}

void ObjectStoreClient::ProcessStoreAdd(const ObjectID& object_id){
  // TODO(hme): Send notification to gcs (ulc?)
  this->add_handler(object_id);
};

void ObjectStoreClient::ProcessStoreRemove(const ObjectID& object_id){
  this->rem_handler(object_id);
};

void ObjectStoreClient::SubscribeObjAdded(void (*callback)(const ObjectID&)) {
  cout << "HandlerAdded" << "\n";
  this->add_handler = callback;
};

void ObjectStoreClient::SubscribeObjDeleted(void (*callback)(const ObjectID&)) {
  this->rem_handler = callback;
};

}