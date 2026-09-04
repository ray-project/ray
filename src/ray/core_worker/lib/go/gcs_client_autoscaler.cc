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

// src/ray/core_worker/lib/go/gcs_client_autoscaler.cc
// GCS Client CGO bridge - Autoscaler operations
#include "gcs_client_bridge.h"
#include "gcs_client_utils.h"
#include "gcs_client_internal.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include "ray/util/logging.h"
#include "ray/gcs_rpc_client/gcs_client.h"

extern "C" {

// Thin CGO, thick Go: the C++ side only returns raw protobuf data; business logic (state judgment) lives in Go
int ray_gcs_client_autoscaler_get_status(CGcsClient* client,
                                         char** serialized_out,
                                         int* size_out,
                                         char** error_out) {
    if (!client || !client->gcs_client || !serialized_out || !size_out) {
        set_error(error_out, "Invalid arguments: client, serialized_out or size_out is null");
        return 0;
    }

    try {
        std::string serialized_reply;
        RAY_LOG(DEBUG) << "Getting autoscaler cluster status, timeout_ms=" << client->timeout_ms;
        ray::Status status = client->gcs_client->Autoscaler().GetClusterStatus(
            client->timeout_ms, serialized_reply);

        if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to get cluster status: " << status.ToString();
            set_error(error_out, ("Failed to get cluster status: " + status.ToString()).c_str());
            return 0;
        }

        RAY_LOG(DEBUG) << "Successfully got cluster status, data_size=" << serialized_reply.size();
        *size_out = static_cast<int>(serialized_reply.size());
        *serialized_out = static_cast<char*>(malloc(*size_out));
        if (!*serialized_out) {
            RAY_LOG(ERROR) << "Failed to allocate memory for serialized data, size=" << *size_out;
            set_error(error_out, "Failed to allocate memory for serialized data");
            return 0;
        }
        memcpy(*serialized_out, serialized_reply.data(), *size_out);
        return 1;
    } catch (const std::exception& e) {
        RAY_LOG(ERROR) << "Exception while getting cluster status: " << e.what();
        set_error(error_out, e.what());
        return 0;
    }
}

}  // extern "C"
