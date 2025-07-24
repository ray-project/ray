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

#include "absl/strings/str_format.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/logging.h"

using boost::asio::ip::tcp;

bool CheckPortFree(int port) {
  instrumented_io_context io_service;
  tcp::socket socket(io_service);
  socket.open(boost::asio::ip::tcp::v4());
  boost::system::error_code ec;
  socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}

std::string BuildAddress(const std::string &host, int port) {
  if (host.find(':') != std::string::npos) {
      // If the host contains a colon, assume it's an IPv6 address
      return absl::StrFormat("[%s]:%d", host, port);
  } else {
      return absl::StrFormat("%s:%d", host, port);
  }
}
