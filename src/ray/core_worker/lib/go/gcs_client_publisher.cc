// src/ray/core_worker/lib/go/gcs_client_publisher.cc
// GCS Client CGO 桥接 - Publisher 操作
#include "gcs_client_bridge.h"
#include "gcs_client_internal.h"
#include "gcs_client_utils.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "src/ray/protobuf/logging.pb.h"

extern "C" {

int ray_gcs_client_publisher_publish_errors(CGcsClient* client,
                                            char** error_out) {
    if (!client) {
        set_error(error_out, "Invalid arguments: client is null");
        return 0;
    }

    try {
        // TODO: 调用 C++ Publisher 的 PublishErrors 方法
        // 当前返回成功占位
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_publisher_publish_logs(CGcsClient* client,
                                          char** error_out) {
    if (!client) {
        set_error(error_out, "Invalid arguments: client is null");
        return 0;
    }

    try {
        // TODO: 调用 C++ Publisher 的 PublishLogs 方法
        // 当前返回成功占位
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

int ray_gcs_client_publisher_publish_log_batch(CGcsClient* client,
                                               const char* key_id,
                                               const char* ip,
                                               const char* pid,
                                               const char* job_id,
                                               int is_error,
                                               const char** lines,
                                               int line_count,
                                               const char* actor_name,
                                               const char* task_name,
                                               int64_t timeout_ms,
                                               char** error_out) {
    if (!client || !client->gcs_client) {
        set_error(error_out, "Invalid arguments: client is null");
        return 0;
    }

    try {
        ray::rpc::LogBatch log_batch;
        if (ip) {
            log_batch.set_ip(ip);
        }
        if (pid) {
            log_batch.set_pid(pid);
        }
        if (job_id) {
            log_batch.set_job_id(job_id);
        }
        log_batch.set_is_error(is_error != 0);
        if (actor_name) {
            log_batch.set_actor_name(actor_name);
        }
        if (task_name) {
            log_batch.set_task_name(task_name);
        }
        for (int i = 0; i < line_count; i++) {
            if (lines && lines[i]) {
                log_batch.add_lines(lines[i]);
            }
        }

        auto status = client->gcs_client->Publisher().PublishLogs(
            key_id ? std::string(key_id) : std::string(),
            std::move(log_batch),
            timeout_ms);
        if (!status.ok()) {
            set_error(error_out, status.ToString().c_str());
            return 0;
        }
        return 1;
    } catch (const std::exception& e) {
        set_error(error_out, e.what());
        return 0;
    }
}

}  // extern "C"
