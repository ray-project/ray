#include <iostream>

#include "common_protocol.h"

#include <memory>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>

#include "common.h"
// #include "format/om_generated.h"
#include "store_messenger.h"

using namespace std;

namespace ray {

StoreMessenger::StoreMessenger(
    boost::asio::io_service &io_service, string &store_socket_name
) : socket_(io_service) {
  this->plasma_conn = new plasma::PlasmaClient();
  ARROW_CHECK_OK(this->plasma_conn->Connect(store_socket_name.c_str(),
                                            "",
                                            PLASMA_DEFAULT_RELEASE_DELAY));
  int plasma_fd;
  ARROW_CHECK_OK(this->plasma_conn->Subscribe(&plasma_fd));
  socket_.assign(boost::asio::ip::tcp::v4(), plasma_fd);
  this->NotificationWait();
};

void StoreMessenger::Terminate() {
  ARROW_CHECK_OK(this->plasma_conn->Disconnect());
  delete this->plasma_conn;
}

void StoreMessenger::NotificationWait() {
  cout << "NotificationWait" << "\n";
  std::vector<boost::asio::mutable_buffer> buffer;
  boost::asio::async_read(socket_,
                          boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&StoreMessenger::ProcessStoreLength,
                                      this,
                                      boost::asio::placeholders::error));
}

void StoreMessenger::ProcessStoreLength(const boost::system::error_code &error) {
  cout << "ProcessStoreLength" << "\n";
  notification_.resize(length_);
  boost::asio::async_read(socket_, boost::asio::buffer(notification_),
                          boost::bind(&StoreMessenger::ProcessStoreNotification,
                                      this, boost::asio::placeholders::error)
  );
}

void StoreMessenger::ProcessStoreNotification(const boost::system::error_code &error) {
  cout << "ProcessStoreNotification" << "\n";
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

void StoreMessenger::ProcessStoreAdd(const ObjectID& object_id){
  // TODO(hme): Send notification to gcs (ulc?)
  this->add_handler(object_id);
};

void StoreMessenger::ProcessStoreRemove(const ObjectID& object_id){
  this->rem_handler(object_id);
};

Status StoreMessenger::SubscribeObjAdded(void (*callback)(const ObjectID&)) {
  cout << "HandlerAdded" << "\n";
  this->add_handler = callback;
  return Status::OK();
};

Status StoreMessenger::SubscribeObjDeleted(void (*callback)(const ObjectID&)) {
  this->rem_handler = callback;
  return Status::OK();
};


}