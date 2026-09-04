// src/ray/core_worker/lib/go/gcs_client_actors.cc
// GCS Client CGO 桥接 - Actors 操作
#include "gcs_client_bridge.h"
#include "gcs_client_utils.h"
#include "gcs_client_internal.h"
#include "gcs_memory.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <optional>
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"

extern "C" {

int ray_gcs_client_actors_get_actor_info(CGcsClient* client,
                                         const char* actor_id_hex,
                                         char** serialized_out,
                                         int* size_out,
                                         char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out || !size_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::string actor_id_hex_str(actor_id_hex ? actor_id_hex : "");
        ray::ActorID actor_id = ray::ActorID::FromHex(actor_id_hex_str);

        std::unique_ptr<std::string> serialized =
            client->global_state_accessor->GetActorInfo(actor_id);

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

int ray_gcs_client_actors_get_all_actor_info(CGcsClient* client,
                                              const char* job_id_hex,
                                              const char* actor_state,
                                              char*** serialized_out,
                                              int** sizes_out,
                                              int* count_out,
                                              char** error_out) {
    if (!client || !client->global_state_accessor || !serialized_out || !count_out) {
        set_error(error_out, "Invalid arguments");
        return 0;
    }

    try {
        std::optional<ray::ActorID> actor_id_filter = std::nullopt;
        std::optional<ray::JobID> job_id_filter = std::nullopt;
        std::optional<std::string> actor_state_filter = std::nullopt;

        if (job_id_hex && strlen(job_id_hex) > 0) {
            job_id_filter = ray::JobID::FromHex(std::string(job_id_hex));
        }
        if (actor_state && strlen(actor_state) > 0) {
            actor_state_filter = std::string(actor_state);
        }

        std::vector<std::string> all_actor_info =
            client->global_state_accessor->GetAllActorInfo(
                actor_id_filter, job_id_filter, actor_state_filter);

        if (!allocate_serialized_array(all_actor_info, serialized_out, sizes_out, count_out)) {
            set_error(error_out, "Failed to allocate memory for serialized array");
            return 0;
        }
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

}  // extern "C"
