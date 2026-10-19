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

// src/ray/core_worker/lib/go/gcs_client_node_resources.cc
// GCS Client CGO bridge - NodeResources operations
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_client_internal.h"
#include "ray/core_worker/lib/go/gcs_client_utils.h"
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "src/ray/protobuf/gcs.pb.h"

extern "C" {

int ray_gcs_client_node_resources_get_available(CGcsClient *client,
                                                const char *node_id_hex,
                                                char **serialized_out,
                                                int *size_out,
                                                char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::vector<std::string> all_resources =
        client->global_state_accessor->GetAllAvailableResources();
    std::string target_node_id(node_id_hex ? node_id_hex : "");

    for (const auto &serialized : all_resources) {
      ray::rpc::AvailableResources resources;
      if (resources.ParseFromString(serialized)) {
        std::string node_id_hex_str =
            ray::NodeID::FromBinary(std::string(resources.node_id())).Hex();

        if (node_id_hex_str == target_node_id) {
          size_t data_size = serialized.size();
          // malloc(0) may return NULL on some platforms; use 1 byte for the
          // empty-string case so a zero-length buffer is not misreported as an
          // allocation failure.
          *serialized_out = static_cast<char *>(malloc(data_size ? data_size : 1));
          *size_out = static_cast<int>(data_size);
          if (!*serialized_out) {
            set_error(error_out, "Failed to allocate memory for serialized data");
            return 0;
          }
          memcpy(*serialized_out, serialized.c_str(), data_size);
          return 1;
        }
      }
    }

    *serialized_out = nullptr;
    *size_out = 0;
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_node_resources_get_all_available(CGcsClient *client,
                                                    char ***serialized_out,
                                                    int **sizes_out,
                                                    int *count_out,
                                                    char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out || !sizes_out ||
      !count_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::vector<std::string> all_resources =
        client->global_state_accessor->GetAllAvailableResources();

    if (!allocate_serialized_array(all_resources, serialized_out, sizes_out, count_out)) {
      set_error(error_out, "Failed to allocate memory for serialized array");
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_node_resources_get_all_total(CGcsClient *client,
                                                char ***serialized_out,
                                                int **sizes_out,
                                                int *count_out,
                                                char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out || !sizes_out ||
      !count_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::vector<std::string> all_resources =
        client->global_state_accessor->GetAllTotalResources();

    if (!allocate_serialized_array(all_resources, serialized_out, sizes_out, count_out)) {
      set_error(error_out, "Failed to allocate memory for serialized array");
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_node_resources_get_total(CGcsClient *client,
                                            const char *node_id_hex,
                                            char **serialized_out,
                                            int *size_out,
                                            char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }

  try {
    std::vector<std::string> all_resources =
        client->global_state_accessor->GetAllTotalResources();
    std::string target_node_id(node_id_hex ? node_id_hex : "");

    for (const auto &serialized : all_resources) {
      ray::rpc::TotalResources resources;
      if (resources.ParseFromString(serialized)) {
        std::string node_id_hex_str =
            ray::NodeID::FromBinary(std::string(resources.node_id())).Hex();

        if (node_id_hex_str == target_node_id) {
          size_t data_size = serialized.size();
          // malloc(0) may return NULL on some platforms; use 1 byte for the
          // empty-string case so a zero-length buffer is not misreported as an
          // allocation failure.
          *serialized_out = static_cast<char *>(malloc(data_size ? data_size : 1));
          *size_out = static_cast<int>(data_size);
          if (!*serialized_out) {
            set_error(error_out, "Failed to allocate memory for serialized data");
            return 0;
          }
          memcpy(*serialized_out, serialized.c_str(), data_size);
          return 1;
        }
      }
    }

    *serialized_out = nullptr;
    *size_out = 0;
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_node_resources_get_all_resource_usage(CGcsClient *client,
                                                         char **serialized_out,
                                                         int *size_out,
                                                         char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out || !size_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }
  try {
    std::unique_ptr<std::string> serialized =
        client->global_state_accessor->GetAllResourceUsage();
    if (!serialized) {
      *serialized_out = nullptr;
      *size_out = 0;
      return 1;
    }
    size_t data_size = serialized->size();
    // malloc(0) may return NULL on some platforms; use 1 byte for the
    // empty-string case so a zero-length buffer is not misreported as an
    // allocation failure.
    *serialized_out = static_cast<char *>(malloc(data_size ? data_size : 1));
    *size_out = static_cast<int>(data_size);
    if (!*serialized_out) {
      set_error(error_out, "Failed to allocate memory for serialized data");
      return 0;
    }
    memcpy(*serialized_out, serialized->c_str(), data_size);
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

int ray_gcs_client_node_resources_get_draining_nodes(CGcsClient *client,
                                                     char ***node_ids_hex_out,
                                                     int64_t **deadlines_ms_out,
                                                     int *count_out,
                                                     char **error_out) {
  if (!client || !client->global_state_accessor || !node_ids_hex_out ||
      !deadlines_ms_out || !count_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }
  try {
    std::unordered_map<ray::NodeID, int64_t> draining =
        client->global_state_accessor->GetDrainingNodes();
    std::vector<std::string> node_ids_hex;
    node_ids_hex.reserve(draining.size());
    std::vector<int64_t> deadlines;
    deadlines.reserve(draining.size());
    for (const auto &entry : draining) {
      node_ids_hex.push_back(entry.first.Hex());
      deadlines.push_back(entry.second);
    }
    if (!allocate_string_array(node_ids_hex, node_ids_hex_out, count_out)) {
      set_error(error_out, "Failed to allocate memory for draining node IDs");
      return 0;
    }
    int count = static_cast<int>(node_ids_hex.size());
    if (count > 0) {
      *deadlines_ms_out = static_cast<int64_t *>(malloc(count * sizeof(int64_t)));
      if (!*deadlines_ms_out) {
        for (int i = 0; i < count; i++) free((*node_ids_hex_out)[i]);
        free(*node_ids_hex_out);
        *node_ids_hex_out = nullptr;
        *count_out = 0;
        set_error(error_out, "Failed to allocate memory for draining deadlines");
        return 0;
      }
      memcpy(*deadlines_ms_out, deadlines.data(), count * sizeof(int64_t));
    } else {
      *deadlines_ms_out = nullptr;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

}  // extern "C"
