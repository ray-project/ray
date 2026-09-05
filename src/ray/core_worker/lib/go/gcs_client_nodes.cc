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

// src/ray/core_worker/lib/go/gcs_client_nodes.cc
// GCS Client CGO bridge - Nodes operations
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gcs_client_bridge.h"
#include "gcs_client_internal.h"
#include "gcs_client_utils.h"
#include "gcs_memory.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "src/ray/protobuf/gcs.pb.h"

extern "C" {

int ray_gcs_client_nodes_check_alive(CGcsClient *client,
                                     const char **node_ids_hex,
                                     int count,
                                     int *alive_out,
                                     char **error_out) {
  if (!client || !client->global_state_accessor || !node_ids_hex || !alive_out ||
      count <= 0) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::vector<std::string> all_node_info =
        client->global_state_accessor->GetAllNodeInfo();

    std::unordered_map<std::string, bool> node_alive_map;
    for (const auto &serialized : all_node_info) {
      ray::rpc::GcsNodeInfo node_info;
      if (node_info.ParseFromString(serialized)) {
        std::string node_id_hex =
            ray::NodeID::FromBinary(std::string(node_info.node_id())).Hex();
        node_alive_map[node_id_hex] = (node_info.state() == ray::rpc::GcsNodeInfo::ALIVE);
      }
    }

    for (int i = 0; i < count; i++) {
      std::string node_id_hex(node_ids_hex[i]);
      auto it = node_alive_map.find(node_id_hex);
      alive_out[i] = (it != node_alive_map.end() && it->second) ? 1 : 0;
    }

    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_nodes_get_all(CGcsClient *client,
                                 const char **node_ids_hex,
                                 int count,
                                 char ***serialized_out,
                                 int **sizes_out,
                                 int *count_out,
                                 char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out || !count_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::vector<std::string> all_node_info =
        client->global_state_accessor->GetAllNodeInfo();
    std::vector<std::string> filtered_nodes;

    if (count > 0 && node_ids_hex) {
      std::unordered_set<std::string> target_ids;
      for (int i = 0; i < count; i++) {
        target_ids.insert(std::string(node_ids_hex[i]));
      }
      for (const auto &serialized : all_node_info) {
        ray::rpc::GcsNodeInfo node_info;
        if (node_info.ParseFromString(serialized)) {
          std::string node_id_hex =
              ray::NodeID::FromBinary(std::string(node_info.node_id())).Hex();
          if (target_ids.count(node_id_hex)) {
            filtered_nodes.push_back(serialized);
          }
        }
      }
    } else {
      filtered_nodes = all_node_info;
    }

    if (!allocate_serialized_array(
            filtered_nodes, serialized_out, sizes_out, count_out)) {
      set_error(error_out, "Failed to allocate memory for serialized array");
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_nodes_drain(CGcsClient *client,
                               const char **node_ids_hex,
                               int count,
                               char ***drained_ids_hex_out,
                               int *drained_count_out,
                               char **error_out) {
  if (!client || !client->global_state_accessor || !drained_ids_hex_out ||
      !drained_count_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::unordered_map<ray::NodeID, int64_t> draining_nodes =
        client->global_state_accessor->GetDrainingNodes();

    std::vector<std::string> drained_ids;
    if (count > 0 && node_ids_hex) {
      for (int i = 0; i < count; i++) {
        std::string node_id_hex(node_ids_hex[i]);
        ray::NodeID node_id = ray::NodeID::FromHex(node_id_hex);
        if (draining_nodes.count(node_id)) {
          drained_ids.push_back(node_id_hex);
        }
      }
    } else {
      for (const auto &pair : draining_nodes) {
        drained_ids.push_back(pair.first.Hex());
      }
    }

    if (!allocate_string_array(drained_ids, drained_ids_hex_out, drained_count_out)) {
      set_error(error_out, "Failed to allocate memory for drained IDs array");
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

}  // extern "C"
