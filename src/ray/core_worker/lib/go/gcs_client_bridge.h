// src/ray/core_worker/lib/go/gcs_client_bridge.h
#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// === GcsClient ===
typedef struct CGcsClient CGcsClient;

// 创建/销毁
CGcsClient* ray_gcs_client_create(const char* address, const char* cluster_id_hex,
                                   int64_t timeout_ms, char** error_out);
void ray_gcs_client_destroy(CGcsClient* client);

// 基础方法
// 注意：返回的字符串使用 malloc 分配，调用者必须调用 free 释放
const char* ray_gcs_client_address(CGcsClient* client);  // owner: caller
const char* ray_gcs_client_cluster_id(CGcsClient* client);  // owner: caller

// 内存管理工具
// 释放 C++ 端分配的字符串（仅用于 strdup/malloc 返回的内存）
void ray_gcs_free_string(const char* str);

// InternalKV - 零拷贝返回
int ray_gcs_client_kv_get(CGcsClient* client, const char* ns, const char* key,
                          void** data_out, size_t* size_out, char** error_out);
int ray_gcs_client_kv_multi_get(CGcsClient* client, const char* ns, const char** keys, int key_count,
                                char*** keys_out, void*** values_out, size_t** sizes_out, int* count_out, char** error_out);
int ray_gcs_client_kv_put(CGcsClient* client, const char* ns, const char* key,
                          const void* value, size_t size, int overwrite,
                          int* success_out, char** error_out);
int ray_gcs_client_kv_del(CGcsClient* client, const char* ns, const char* key,
                          int del_by_prefix, int* count_out, char** error_out);
int ray_gcs_client_kv_keys(CGcsClient* client, const char* ns, const char* prefix,
                           char*** keys_out, int* count_out, char** error_out);
int ray_gcs_client_kv_exists(CGcsClient* client, const char* ns, const char* key,
                             int* exists_out, char** error_out);


// NodeResources
int ray_gcs_client_node_resources_get_available(CGcsClient* client,
                                                 const char* node_id_hex,
                                                 char** serialized_out,
                                                 int* size_out,
                                                 char** error_out);
int ray_gcs_client_node_resources_get_total(CGcsClient* client,
                                              const char* node_id_hex,
                                              char** serialized_out,
                                              int* size_out,
                                              char** error_out);

// Nodes
int ray_gcs_client_nodes_get_node_to_connect(CGcsClient* client,
                                              const char* node_ip_address,
                                              char** serialized_out,
                                              int* size_out,
                                              char** error_out);

// Jobs
int ray_gcs_client_jobs_get_all_job_info(CGcsClient* client,
                                         int skip_submission_field,
                                         int skip_running_tasks_field,
                                         char*** serialized_out,
                                         int** sizes_out,
                                         int* count_out,
                                         char** error_out);
int ray_gcs_client_jobs_get_next_job_id(CGcsClient* client,
                                        char* job_id_hex_out,
                                        char** error_out);
int ray_gcs_client_jobs_get_job_info(CGcsClient* client,
                                     const char* job_id_hex,
                                     char** serialized_out,
                                     int* size_out,
                                     char** error_out);
int ray_gcs_client_nodes_check_alive(CGcsClient* client, 
                                      const char** node_ids_hex, int count,
                                      int* alive_out, char** error_out);
int ray_gcs_client_nodes_get_all(CGcsClient* client,
                                 const char** node_ids_hex, int count,
                                 char*** serialized_out, int** sizes_out, int* count_out,
                                 char** error_out);
int ray_gcs_client_nodes_drain(CGcsClient* client,
                               const char** node_ids_hex, int count,
                               char*** drained_ids_hex_out, int* drained_count_out,
                               char** error_out);

// Actors
int ray_gcs_client_actors_get_actor_info(CGcsClient* client,
                                         const char* actor_id_hex,
                                         char** serialized_out,
                                         int* size_out,
                                         char** error_out);
int ray_gcs_client_actors_get_all_actor_info(CGcsClient* client,
                                             const char* job_id_hex,
                                             const char* actor_state,
                                             char*** serialized_out,
                                             int** sizes_out,
                                             int* count_out,
                                             char** error_out);

// Workers
int ray_gcs_client_workers_get_all_worker_info(CGcsClient* client,
                                               char*** serialized_out,
                                               int** sizes_out,
                                               int* count_out,
                                               char** error_out);
int ray_gcs_client_workers_get_worker_info(CGcsClient* client,
                                           const char* worker_id_hex,
                                           char** serialized_out,
                                           int* size_out,
                                           char** error_out);
int ray_gcs_client_workers_add_worker_info(CGcsClient* client,
                                           const char* serialized_data,
                                           int size,
                                           int* success_out,
                                           char** error_out);


// Publisher
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
                                               char** error_out);

// Autoscaler
// 返回 AutoscalingState 的 protobuf 序列化数据，由 Go 端解析并判断状态
// serialized_out: 输出参数，指向 protobuf 序列化数据（malloc 分配）
// size_out: 输出参数，序列化数据的大小（字节）
// 调用者必须调用 free() 释放 serialized_out
int ray_gcs_client_autoscaler_get_status(CGcsClient* client,
                                         char** serialized_out,
                                         int* size_out,
                                         char** error_out);

// PlacementGroups
int ray_gcs_client_placement_groups_get_all(CGcsClient* client,
                                            char*** serialized_out,
                                            int** sizes_out,
                                            int* count_out,
                                            char** error_out);
int ray_gcs_client_placement_groups_get_by_id(CGcsClient* client,
                                              const char* pg_id_hex,
                                              char** serialized_out,
                                              int* size_out,
                                              char** error_out);
int ray_gcs_client_placement_groups_get_by_name(CGcsClient* client,
                                                const char* name,
                                                const char* ray_namespace,
                                                char** serialized_out,
                                                int* size_out,
                                                char** error_out);
#ifdef __cplusplus
}
#endif
