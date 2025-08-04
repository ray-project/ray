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

#include <string>
#include <vector>

/// Build a network address string from host and port.
/// \param host The hostname or IP address.
/// \param port The port number.
/// \return Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
std::string BuildAddress(const std::string &host, int port);

/// Build a network address string from host and port.
/// \param host The hostname or IP address.
/// \param port The port as a string.
/// \return Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
std::string BuildAddress(const std::string &host, const std::string &port);

/// Parse a network address string into host and port.
/// \param address The address string to parse (e.g., "localhost:8000", "[::1]:8000").
/// \return Vector with [host, port] if port found, or [address] if no colon separator.
std::vector<std::string> ParseAddress(const std::string &address);
