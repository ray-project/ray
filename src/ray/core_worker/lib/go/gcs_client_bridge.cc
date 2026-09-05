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

// src/ray/core_worker/lib/go/gcs_client_bridge.cc
// GCS Client CGO bridge - main file: client create/destroy and base methods
#include "gcs_client_bridge.h"

#include <boost/asio/executor_work_guard.hpp>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "gcs_client_internal.h"
#include "gcs_client_utils.h"
#include "gcs_memory.h"
#include "ray/util/logging.h"
#include "ray/util/raii.h"

// Logging system RAII initializer (file scope, initialized before CGO calls)
static InitShutdownRAII g_ray_log_raii(ray::RayLog::StartRayLog,
                                       ray::RayLog::ShutDownRayLog,
                                       "gcs_client_bridge",
                                       ray::RayLogLevel::INFO,
                                       "",  // log_filepath
                                       "",  // err_log_filepath
                                       0,   // log_rotation_max_size
                                       1);  // log_rotation_file_num

// Memory management helpers: free strings allocated by strdup/malloc
void ray_gcs_free_string(const char *str) {
  if (str) {
    free(const_cast<char *>(str));
  }
}

extern "C" {

// === GcsClient create/destroy ===

CGcsClient *ray_gcs_client_create(const char *address,
                                  const char *cluster_id_hex,
                                  int64_t timeout_ms,
                                  char **error_out) {
  if (!address) {
    set_error(error_out, "Invalid arguments: address must not be null");
    return nullptr;
  }

  // Allow empty cluster_id_hex for temporary GCS client operations
  // (like GetNextJobID, GetNodeToConnect). This matches Java's behavior:
  // ToGcsClientOptions passes NilClusterID with allow_cluster_id_nil=true,
  // fetch_cluster_id_if_nil=false.
  if (!cluster_id_hex) {
    set_error(
        error_out,
        "Invalid arguments: cluster_id_hex must not be null (use empty string for nil)");
    return nullptr;
  }

  // Validate cluster_id_hex length only if non-empty
  if (cluster_id_hex[0] != '\0' && strlen(cluster_id_hex) != 56) {
    set_error(error_out, "Invalid cluster_id_hex: must be empty or 56 hex characters");
    return nullptr;
  }

  try {
    CGcsClient *client = new (std::nothrow) CGcsClient();
    if (!client) {
      set_error(error_out, "Failed to allocate memory for GcsClient");
      return nullptr;
    }

    client->address = address;
    client->cluster_id_hex = cluster_id_hex;
    client->timeout_ms = timeout_ms;

    std::string addr_str(address);
    size_t colon_pos = addr_str.find(':');
    if (colon_pos == std::string::npos) {
      set_error(error_out, "Invalid address format: expected host:port");
      delete client;
      return nullptr;
    }

    std::string host = addr_str.substr(0, colon_pos);
    int port = std::stoi(addr_str.substr(colon_pos + 1));

    ray::ClusterID cluster_id;
    // Only parse cluster_id_hex if non-empty
    if (cluster_id_hex[0] != '\0') {
      std::vector<uint8_t> cluster_id_bytes;
      std::string hex_str(cluster_id_hex);
      for (size_t i = 0; i < hex_str.length(); i += 2) {
        std::string byte_str = hex_str.substr(i, 2);
        uint8_t byte = static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
        cluster_id_bytes.push_back(byte);
      }
      cluster_id = ray::ClusterID::FromBinary(
          std::string(reinterpret_cast<const char *>(cluster_id_bytes.data()),
                      cluster_id_bytes.size()));
    } else {
      // Empty cluster_id_hex means NilClusterID
      cluster_id = ray::ClusterID::Nil();
    }

    client->io_service = std::make_unique<instrumented_io_context>();
    if (!client->io_service) {
      set_error(error_out, "Failed to allocate io_service");
      delete client;
      return nullptr;
    }

    ray::gcs::GcsClientOptions options(host, port, cluster_id, true, true);
    client->gcs_client = std::make_shared<ray::gcs::GcsClient>(options);

    std::promise<bool> io_ready;
    client->io_thread = std::make_unique<std::thread>([&]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          client->io_service->get_executor());
      io_ready.set_value(true);
      client->io_service->run();
    });
    io_ready.get_future().get();

    ray::Status status =
        client->gcs_client->Connect(*client->io_service, client->timeout_ms);
    if (!status.ok()) {
      set_error(error_out, ("Failed to connect to GCS: " + status.ToString()).c_str());
      delete client;
      return nullptr;
    }

    // Update cluster_id_hex to the actual ClusterID after Connect()
    // This ensures cluster_id_hex matches what GetClusterId() returns
    client->cluster_id_hex = client->gcs_client->GetClusterId().Hex();

    client->global_state_accessor =
        std::make_unique<ray::gcs::GlobalStateAccessor>(options);
    if (!client->global_state_accessor->Connect()) {
      set_error(error_out, "Failed to connect GlobalStateAccessor to GCS");
      delete client;
      return nullptr;
    }

    return client;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return nullptr;
  }
}

void ray_gcs_client_destroy(CGcsClient *client) {
  if (client) {
    if (client->gcs_client) {
      client->gcs_client->Disconnect();
      client->gcs_client.reset();
    }

    if (client->io_service) {
      client->io_service->stop();
    }
    if (client->io_thread && client->io_thread->joinable()) {
      client->io_thread->join();
    }

    delete client;
  }
}

// === GcsClient base methods ===

const char *ray_gcs_client_address(CGcsClient *client) {
  if (!client) {
    return nullptr;
  }
  return strdup(client->address.c_str());
}

const char *ray_gcs_client_cluster_id(CGcsClient *client) {
  if (!client || !client->gcs_client) {
    return nullptr;
  }
  // Fetch the actual ClusterID from GcsClient after Connect()
  // This returns the ClusterID fetched from GCS server (for Driver mode)
  // or the ClusterID passed at creation time (for Worker mode)
  ray::ClusterID cluster_id = client->gcs_client->GetClusterId();
  return strdup(cluster_id.Hex().c_str());
}

// === GcsClient Nodes ===

int ray_gcs_client_nodes_get_node_to_connect(CGcsClient *client,
                                             const char *node_ip_address,
                                             char **serialized_out,
                                             int *size_out,
                                             char **error_out) {
  if (!client || !node_ip_address || !serialized_out || !size_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::string node_info_str;
    ray::Status status = client->global_state_accessor->GetNodeToConnectForDriver(
        std::string(node_ip_address), &node_info_str);

    if (!status.ok()) {
      set_error(error_out, status.ToString().c_str());
      return 0;
    }

    *size_out = static_cast<int>(node_info_str.size());
    *serialized_out = static_cast<char *>(malloc(*size_out));
    if (!*serialized_out) {
      set_error(error_out, "Failed to allocate memory for serialized data");
      return 0;
    }
    memcpy(*serialized_out, node_info_str.data(), *size_out);
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

}  // extern "C"
