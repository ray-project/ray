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

// src/ray/core_worker/lib/go/gcs_client_workers.cc
// GCS Client CGO bridge - Workers operations
#include "gcs_client_bridge.h"
#include "gcs_client_utils.h"
#include "gcs_client_internal.h"
#include "gcs_memory.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"

extern "C" {

int ray_gcs_client_workers_get_all_worker_info(CGcsClient* client,
                                               char*** serialized_out,
                                               int** sizes_out,
                                               int* count_out,
                                               char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out || !count_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::vector<std::string> all_worker_info =
            client->global_state_accessor->GetAllWorkerInfo();

        if (!allocate_serialized_array(all_worker_info, serialized_out, sizes_out, count_out)) {
            set_error(error_out, "Failed to allocate memory for serialized array");
            return 0;
        }
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_workers_get_worker_info(CGcsClient* client,
                                           const char* worker_id_hex,
                                           char** serialized_out,
                                           int* size_out,
                                           char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out || !size_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::string worker_id_hex_str(worker_id_hex ? worker_id_hex : "");
        ray::WorkerID worker_id = ray::WorkerID::FromHex(worker_id_hex_str);

        std::unique_ptr<std::string> serialized =
            client->global_state_accessor->GetWorkerInfo(worker_id);

        if (!serialized) {
            *serialized_out = nullptr;
            *size_out = 0;
            return 1;
        }

        size_t data_size = serialized->size();
        *serialized_out = static_cast<char*>(malloc(data_size));
        *size_out = static_cast<int>(data_size);
        if (!*serialized_out) {
            set_error(error_out, "Failed to allocate memory for serialized data");
            return 0;
        }
        memcpy(*serialized_out, serialized->c_str(), data_size);

        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_workers_add_worker_info(CGcsClient* client,
                                           const char* serialized_data,
                                           int size,
                                           int* success_out,
                                           char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_data || !success_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::string data(serialized_data, size);
        bool success = client->global_state_accessor->AddWorkerInfo(data);
        *success_out = success ? 1 : 0;
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

}  // extern "C"
