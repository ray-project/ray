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

#include "ray/common/network_util.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/logging.h"

using boost::asio::ip::tcp;

bool CheckPortFree(int port) {
  instrumented_io_context io_service;
  boost::system::error_code ec;
  tcp::acceptor acceptor(io_service);
  tcp::endpoint endpoint(tcp::v4(), port);

  acceptor.open(endpoint.protocol(), ec);
  if (ec) return false;

  // Check if the port is already in use
  acceptor.set_option(boost::asio::socket_base::reuse_address(false));
  acceptor.bind(endpoint, ec);
  if (ec) return false;

  // Listening helps ensure the port can be fully connectable
  acceptor.listen(boost::asio::socket_base::max_listen_connections, ec);
  if (ec) return false;

  acceptor.close();
  return true;
}
