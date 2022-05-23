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

#pragma once

#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/bind/bind.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/system/error_code.hpp>
#include <string>

#ifndef _WIN32

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#endif

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/constants.h"

using boost::asio::deadline_timer;
using boost::asio::io_service;
using boost::asio::ip::tcp;

/// \class AsyncClient
///
/// This class provides the socket asynchronous interface with timeout: Connect.
class AsyncClient {
 public:
  AsyncClient() : socket_(io_service_), timer_(io_service_) {}

  ~AsyncClient() {
    io_service_.stop();
    socket_.close();
  }

  /// This function is used to asynchronously connect a socket to the specified address
  /// with timeout.
  ///
  /// \param ip The ip that the rpc server is listening on.
  /// \param port The port that the rpc server is listening on.
  /// \param timeout_ms The maximum wait time in milliseconds.
  /// \param is_timeout Whether connection timeout.
  /// \param error_code Set to indicate what error occurred, if any.
  /// \return Whether the connection is successful.
  bool Connect(const std::string &ip,
               int port,
               int64_t timeout_ms,
               bool *is_timeout,
               boost::system::error_code *error_code = nullptr) {
    try {
      auto endpoint =
          boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(ip), port);

      bool is_connected = false;
      *is_timeout = false;
      socket_.async_connect(endpoint,
                            boost::bind(&AsyncClient::ConnectHandle,
                                        this,
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

  instrumented_io_context io_service_;
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
/// \return a valid local ip.
std::string GetValidLocalIp(int port, int64_t timeout_ms);

bool CheckFree(int port);

/// \namespace NetIf
///
/// Namespace with implementations of helper function to get valid IPs
/// from network interfaces
namespace NetIf {
/// Priority of a IPs from a given interface to be chosen to serve something
enum class Priority { kVeryHigh, kHigh, kNormal, kExclude };

/// To keep one IP and its interface
typedef std::pair<std::string, std::string> NameAndIp;

/// To keep interface names prefixes and its priorities
typedef std::pair<std::string, Priority> PrefixAndPriority;

/// Interface name prefix and its priority
extern std::vector<PrefixAndPriority> prefixes_and_priorities;

/// A helper function to get IPs from local interfaces.
/// It also filters out IPs from interfaces with priority Priority::kExclude
/// and sort the IPs based on its interfaces priority.
/// If running on Windows, uses boost to try to resolve hostname
/// and don't filter candidates.
///
/// \return a vector with valid local IP candidates that were not filtered out
std::vector<boost::asio::ip::address> GetValidLocalIpCandidates();

/// Based on the prefix of the interface name, returns a level of priority.
///
/// \param if_name the name of the interface to be tested.
/// \return the priority of the interface.
Priority GetPriority(const std::string &if_name);

/// Helper function to be used with std::sort.
/// Lowest priority comes first
bool CompNamesAndIps(const NameAndIp &left, const NameAndIp &right);

/// Helper function to be used with std::sort.
/// Biggest prefix comes first
bool CompPrefixLen(const PrefixAndPriority &left, const PrefixAndPriority &right);

/// A helper tiny function to check if the interface name has a given prefix.
///
/// \param name the interface name to be checked.
/// \param prefix the prefix that will be looked in 'name'.
/// \return true if 'name' starts with 'prefix'.
bool NameStartsWith(const std::string &name, const std::string &prefix);
}  // namespace NetIf
