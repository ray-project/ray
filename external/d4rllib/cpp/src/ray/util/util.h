// Copyright 2020-2021 The Ray Authors.
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
#include <string>

namespace ray {
namespace internal {

/// IP address by which the local node can be reached *from* the `address`.
///
/// The behavior should be the same as `node_ip_address_from_perspective` from Ray Python
/// code. See
/// https://stackoverflow.com/questions/2674314/get-local-ip-address-using-boost-asio.
///
/// TODO(kfstorm): Make this function shared code and migrate Python & Java to use this
/// function.
///
/// \param address The IP address and port of any known live service on the network
/// you care about.
/// \return The IP address by which the local node can be reached from the address.
std::string GetNodeIpAddress(const std::string &address = "8.8.8.8:53");

std::string getLibraryPathEnv();

}  // namespace internal
}  // namespace ray
