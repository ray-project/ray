#ifndef RAY_CONNECTION_POOL_H
#define RAY_CONNECTION_POOL_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "object_manager_client_connection.h"
#include "ray/id.h"
#include "ray/status.h"

namespace ray {

class ConnectionPool {

public:

  ConnectionPool(){

  }

  void Add(){

  }

 private:

  /// Note that (currently) receives take place on the main thread,
  /// and sends take place on a dedicated thread.
  std::unordered_map<ray::ClientID, SenderConnection::pointer, ray::UniqueIDHasher>
      message_send_connections_;
  std::unordered_map<ray::ClientID, SenderConnection::pointer, ray::UniqueIDHasher>
      transfer_send_connections_;

  std::unordered_map<ray::ClientID, std::shared_ptr<ObjectManagerClientConnection>, ray::UniqueIDHasher>
      message_receive_connections_;
  std::unordered_map<ray::ClientID, std::shared_ptr<ObjectManagerClientConnection>, ray::UniqueIDHasher>
      transfer_receive_connections_;

};

} // namespace ray

#endif //RAY_CONNECTION_POOL_H
