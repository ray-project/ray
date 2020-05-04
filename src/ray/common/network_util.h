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

#ifndef RAY_COMMON_NETWORK_UTIL_H
#define RAY_COMMON_NETWORK_UTIL_H

#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/system/error_code.hpp>
#include "constants.h"

using boost::asio::deadline_timer;
using boost::asio::io_service;
using boost::asio::ip::tcp;

/// \class AsyncClient
///
/// This class provides the socket asynchronous interface with timeout: Connect.
class AsyncClient {
 public:
  AsyncClient() : socket_(io_service_), timer_(io_service_) {}

  /// This function is used to asynchronously connect a socket to the specified address
  /// with timeout.
  ///
  /// \param ip The ip that the rpc server is listening on.
  /// \param port The port that the rpc server is listening on.
  /// \param timeout_ms The maximum wait time in milliseconds.
  /// \param is_timeout Whether connection timeout.
  /// \param error_code Set to indicate what error occurred, if any.
  /// \return Whether the connection is successful.
  bool Connect(const std::string &ip, int port, int64_t timeout_ms, bool *is_timeout,
               boost::system::error_code *error_code = nullptr) {
    try {
      auto endpoint =
          boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(ip), port);

      bool is_connected = false;
      *is_timeout = false;
      socket_.async_connect(endpoint, boost::bind(&AsyncClient::ConnectHandle, this,
                                                  boost::asio::placeholders::error,
                                                  boost::ref(is_connected)));

      // Set a deadline for the asynchronous operation.
      timer_.expires_from_now(boost::posix_time::milliseconds(timeout_ms));
      timer_.async_wait(boost::bind(&AsyncClient::TimerHandle, this, is_timeout));

      do {
        io_service_.run_one();
      } while (!(*is_timeout) && !is_connected);

      timer_.cancel();

      if (error_code != nullptr) {
        *error_code = error_code_;
      }
      return is_connected;
    } catch (...) {
      return false;
    }
  }

  /// This function is used to obtain the local ip address of the client socket.
  ///
  /// \return The local ip address of the client socket.
  std::string GetLocalIPAddress() {
    return socket_.local_endpoint().address().to_string();
  }

 private:
  void ConnectHandle(boost::system::error_code error_code, bool &is_connected) {
    error_code_ = error_code;
    if (!error_code) {
      is_connected = true;
    }
  }

  void TimerHandle(bool *is_timeout) {
    socket_.close();
    *is_timeout = true;
  }

  boost::asio::io_service io_service_;
  tcp::socket socket_;
  deadline_timer timer_;
  boost::system::error_code error_code_;
};

/// A helper function to get a valid local ip.
/// We will connect google public dns server and get local ip from socket.
/// If dns server is unreachable, try to resolve hostname and get a valid ip by ping the
/// port of the local ip is listening on. If there is no valid local ip, `127.0.0.1` is
/// returned.
///
/// \param port The port that the local ip is listening on.
/// \param timeout_ms The maximum wait time in milliseconds.
/// \return A valid local ip.
std::string GetValidLocalIp(int port, int64_t timeout_ms) {
  AsyncClient async_client;
  boost::system::error_code error_code;
  std::string address;
  bool is_timeout;
  if (async_client.Connect(kPublicDNSServerIp, kPublicDNSServerPort, timeout_ms,
                           &is_timeout, &error_code)) {
    address = async_client.GetLocalIPAddress();
  } else {
    address = "127.0.0.1";

    if (is_timeout || error_code == boost::system::errc::host_unreachable) {
      boost::asio::ip::detail::endpoint primary_endpoint;
      boost::asio::io_context io_context;
      boost::asio::ip::tcp::resolver resolver(io_context);
      boost::asio::ip::tcp::resolver::query query(
          boost::asio::ip::host_name(), "",
          boost::asio::ip::resolver_query_base::flags::v4_mapped);
      boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query, error_code);
      boost::asio::ip::tcp::resolver::iterator end;  // End marker.
      if (!error_code) {
        while (iter != end) {
          boost::asio::ip::tcp::endpoint ep = *iter;
          if (ep.address().is_v4() && !ep.address().is_loopback() &&
              !ep.address().is_multicast()) {
            primary_endpoint.address(ep.address());
            primary_endpoint.port(ep.port());

            AsyncClient client;
            if (client.Connect(primary_endpoint.address().to_string(), port, timeout_ms,
                               &is_timeout)) {
              break;
            }
          }
          iter++;
        }
      } else {
        RAY_LOG(WARNING) << "Failed to resolve ip address, error = "
                         << strerror(error_code.value());
        iter = end;
      }

      if (iter != end) {
        address = primary_endpoint.address().to_string();
      }
    }
  }

  return address;
}

#endif  // RAY_COMMON_NETWORK_UTIL_H
