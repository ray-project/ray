// Copyright 2025 The Ray Authors.
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

#include <array>
#include <optional>
#include <string>

namespace ray {

/// Build a network address string from host and port.
/// \param host The hostname or IP address.
/// \param port The port as a string.
/// \return Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
std::string BuildAddress(const std::string &host, const std::string &port);

/// Build a network address string from host and port.
/// \param host The hostname or IP address.
/// \param port The port number.
/// \return Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
std::string BuildAddress(const std::string &host, int port);

/// Parse a network address string into host and port.
/// \param address The address string to parse (e.g., "localhost:8000", "[::1]:8000").
/// \return Optional array with [host, port] if port found, nullopt if no colon separator.
std::optional<std::array<std::string, 2>> ParseAddress(const std::string &address);

/// Check whether the given port is available, via attempt to bind a socket to the port.
/// Notice, the check could be non-authentic if there're concurrent port assignments.
/// \param port The port number to check.
/// \return true if the port is available, false otherwise.
bool CheckPortFree(int port);

}  // namespace ray
