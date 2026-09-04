// src/ray/core_worker/lib/go/gcs_client_jobs.cc
// GCS Client CGO 桥接 - Jobs 操作
#include "gcs_client_bridge.h"
#include "gcs_client_utils.h"
#include "gcs_client_internal.h"
#include "gcs_memory.h"
#include <cstdlib>
#include <cstring>
#include <climits>
#include <string>
#include <vector>
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "ray/gcs_rpc_client/gcs_client.h"

extern "C" {

int ray_gcs_client_jobs_get_all_job_info(CGcsClient* client,
                                         int skip_submission_field,
                                         int skip_running_tasks_field,
                                         char*** serialized_out,
                                         int** sizes_out,
                                         int* count_out,
                                         char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out || !count_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::vector<std::string> all_job_info = client->global_state_accessor->GetAllJobInfo(
            skip_submission_field != 0, skip_running_tasks_field != 0);

        if (!allocate_serialized_array(all_job_info, serialized_out, sizes_out, count_out)) {
            set_error(error_out, "Failed to allocate memory for serialized array");
            return 0;
        }
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_jobs_get_next_job_id(CGcsClient* client,
                                        char* job_id_hex_out,
                                        char** error_out) {
    if (!client || !client->global_state_accessor || !job_id_hex_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        ray::JobID next_job_id = client->global_state_accessor->GetNextJobID();
        std::string job_id_hex = next_job_id.Hex();

        // JobID is 4 bytes, so hex string should be exactly 8 characters
        if (job_id_hex.length() == 8) {
            strncpy(job_id_hex_out, job_id_hex.c_str(), 8);
            job_id_hex_out[8] = '\0';
            return 1;
        } else {
            set_error(error_out, "Invalid job ID length");
            return 0;
        }
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_jobs_get_job_info(CGcsClient* client,
                                     const char* job_id_hex,
                                     char** serialized_out,
                                     int* size_out,
                                     char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::vector<std::string> all_job_info = client->global_state_accessor->GetAllJobInfo(false, false);
        std::string target_job_id(job_id_hex ? job_id_hex : "");

        for (const auto& serialized : all_job_info) {
            ray::rpc::JobTableData job_info;
            if (job_info.ParseFromString(serialized)) {
                std::string job_id_hex_str = ray::JobID::FromBinary(
                    std::string(job_info.job_id())).Hex();

                if (job_id_hex_str == target_job_id) {
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
