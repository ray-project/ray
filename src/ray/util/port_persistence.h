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

/**
 * @file port_persistence.h
 * @brief Local port discovery utilities for inter-component communication.
 *
 * These functions enable components to discover each other's dynamically
 * bound ports via the local filesystem. Typical usage pattern:
 *   - A component (e.g., dashboard agent) binds to port 0 and calls
 *     PersistPort() to write its actual port to a file.
 *   - Other components (e.g., raylet) call WaitForPersistedPort() to
 *     read the port.
 */

#include <string>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

/**
 * @brief Generate the standard filename for a port file.
 *
 * @param node_id The node ID of this node.
 * @param port_name The name of the port.
 * @return The filename in format "{port_name}_{node_id_hex}".
 */
std::string GetPortFileName(const NodeID &node_id, const std::string &port_name);

/**
 * @brief Persist a port number to a file.
 *
 * @param dir The directory where the port file will be created.
 * @param node_id The node ID of this node.
 * @param port_name The name of the port.
 * @param port The port number to persist.
 * @return Status::OK if the port was persisted successfully.
 * @return Status::IOError if the file could not be written.
 */
Status PersistPort(const std::string &dir,
                   const NodeID &node_id,
                   const std::string &port_name,
                   int port);

/**
 * @brief Wait for a persisted port file and return the port number.
 *
 * @param dir The directory where the port file is expected.
 * @param node_id The node ID to identify this port file.
 * @param port_name The name of the port (e.g., "dashboard_agent").
 * @param timeout_ms Maximum time to wait in milliseconds. Defaults to 15000.
 * @param poll_interval_ms Interval between filesystem checks in milliseconds.
 *        Defaults to 50.
 * @return The port number if successful.
 * @return StatusT::IOError if the file exists but cannot be read.
 * @return StatusT::TimedOut if the file does not appear within the timeout period.
 * @return StatusT::Invalid if the file content is not a valid port number.
 */
StatusSetOr<int, StatusT::IOError, StatusT::TimedOut, StatusT::Invalid>
WaitForPersistedPort(const std::string &dir,
                     const NodeID &node_id,
                     const std::string &port_name,
                     int timeout_ms = 15000,
                     int poll_interval_ms = 50);

}  // namespace ray
