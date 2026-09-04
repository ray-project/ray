// src/ray/core_worker/lib/go/gcs_client_node_resources.cc
// GCS Client CGO 桥接 - NodeResources 操作
#include "gcs_client_bridge.h"
#include "gcs_client_utils.h"
#include "gcs_client_internal.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "src/ray/protobuf/gcs.pb.h"

extern "C" {

int ray_gcs_client_node_resources_get_available(CGcsClient* client,
                                                  const char* node_id_hex,
                                                  char** serialized_out,
                                                  int* size_out,
                                                  char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::vector<std::string> all_resources = client->global_state_accessor->GetAllAvailableResources();
        std::string target_node_id(node_id_hex ? node_id_hex : "");

        for (const auto& serialized : all_resources) {
            ray::rpc::AvailableResources resources;
            if (resources.ParseFromString(serialized)) {
                std::string node_id_hex_str = ray::NodeID::FromBinary(
                    std::string(resources.node_id())).Hex();

                if (node_id_hex_str == target_node_id) {
                    size_t data_size = serialized.size();
                    *serialized_out = static_cast<char*>(malloc(data_size));
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
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_node_resources_get_total(CGcsClient* client,
                                               const char* node_id_hex,
                                               char** serialized_out,
                                               int* size_out,
                                               char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::vector<std::string> all_resources = client->global_state_accessor->GetAllTotalResources();
        std::string target_node_id(node_id_hex ? node_id_hex : "");

        for (const auto& serialized : all_resources) {
            ray::rpc::TotalResources resources;
            if (resources.ParseFromString(serialized)) {
                std::string node_id_hex_str = ray::NodeID::FromBinary(
                    std::string(resources.node_id())).Hex();

                if (node_id_hex_str == target_node_id) {
                    size_t data_size = serialized.size();
                    *serialized_out = static_cast<char*>(malloc(data_size));
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
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

}  // extern "C"
