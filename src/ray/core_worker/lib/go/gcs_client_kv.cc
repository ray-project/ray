// src/ray/core_worker/lib/go/gcs_client_kv.cc
// GCS Client CGO 桥接 - KV 操作
#include "gcs_client_bridge.h"
#include "gcs_client_utils.h"
#include "gcs_client_internal.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>
#include "ray/gcs_rpc_client/rpc_client.h"

extern "C" {

// === InternalKV 操作 ===

int ray_gcs_client_kv_get(CGcsClient* client,
                          const char* ns,
                          const char* key,
                          void** data_out,
                          size_t* size_out,
                          char** error_out) {
    if (!client || !client->gcs_client || !key || !data_out || !size_out) {
        set_error(error_out, "Invalid arguments");
        return -1;
    }

    std::string ns_str(ns ? ns : "");
    std::string key_str(key);

    std::string value;
    auto status = client->gcs_client->InternalKV().Get(
        ns_str, key_str, ray::rpc::GetGcsTimeoutMs(), value);

    if (status.IsNotFound()) {
        // 保留与 Go 侧契约：错误串 "Key not found" 映射为 gcs.ErrKeyNotFound
        set_error(error_out, "Key not found");
        *data_out = nullptr;
        *size_out = 0;
        return -1;
    }
    if (!status.ok()) {
        set_error(error_out, status.ToString().c_str());
        *data_out = nullptr;
        *size_out = 0;
        return -1;
    }

    *size_out = value.size();
    if (*size_out == 0) {
        // 存在但值为空：Go 侧 (cData==nil||cSize==0) 返回 []byte{}, nil
        *data_out = nullptr;
        return 0;
    }
    *data_out = malloc(*size_out);
    if (!*data_out) {
        set_error(error_out, "Failed to allocate memory for value");
        *size_out = 0;
        return -1;
    }
    memcpy(*data_out, value.data(), *size_out);
    return 0;
}

int ray_gcs_client_kv_multi_get(CGcsClient* client,
                                const char* ns,
                                const char** keys,
                                int key_count,
                                char*** keys_out,
                                void*** values_out,
                                size_t** sizes_out,
                                int* count_out,
                                char** error_out) {
    if (!client || !client->gcs_client || !keys || key_count <= 0 ||
        !keys_out || !values_out || !sizes_out || !count_out) {
        set_error(error_out, "Invalid arguments");
        return -1;
    }

    std::string ns_str(ns ? ns : "");
    std::vector<std::string> key_vec;
    key_vec.reserve(key_count);
    for (int i = 0; i < key_count; i++) {
        if (keys[i]) key_vec.emplace_back(keys[i]);
    }
    if (key_vec.empty()) {
        *keys_out = nullptr;
        *values_out = nullptr;
        *sizes_out = nullptr;
        *count_out = 0;
        return 0;
    }

    std::unordered_map<std::string, std::string> values;
    auto status = client->gcs_client->InternalKV().MultiGet(
        ns_str, key_vec, ray::rpc::GetGcsTimeoutMs(), values);

    if (!status.ok()) {
        set_error(error_out, status.ToString().c_str());
        return -1;
    }

    int count = static_cast<int>(values.size());
    *count_out = count;
    if (count == 0) {
        *keys_out = nullptr;
        *values_out = nullptr;
        *sizes_out = nullptr;
        return 0;
    }

    char** key_arr = static_cast<char**>(malloc(count * sizeof(char*)));
    void** val_arr = static_cast<void**>(malloc(count * sizeof(void*)));
    size_t* size_arr = static_cast<size_t*>(malloc(count * sizeof(size_t)));
    if (!key_arr || !val_arr || !size_arr) {
        set_error(error_out, "Failed to allocate memory");
        free(key_arr);
        free(val_arr);
        free(size_arr);
        *keys_out = nullptr;
        *values_out = nullptr;
        *sizes_out = nullptr;
        return -1;
    }

    int i = 0;
    for (const auto& kv : values) {
        // 初始化为 nullptr，确保失败时 cleanup 可安全释放已分配元素
        key_arr[i] = nullptr;
        val_arr[i] = nullptr;
        size_arr[i] = 0;

        key_arr[i] = static_cast<char*>(malloc(kv.first.size() + 1));
        if (!key_arr[i]) {
            set_error(error_out, "Failed to allocate memory for key");
            for (int j = 0; j < i; j++) {
                free(key_arr[j]);
                free(val_arr[j]);
            }
            free(key_arr);
            free(val_arr);
            free(size_arr);
            *keys_out = nullptr;
            *values_out = nullptr;
            *sizes_out = nullptr;
            return -1;
        }
        strcpy(key_arr[i], kv.first.c_str());

        size_arr[i] = kv.second.size();
        if (size_arr[i] > 0) {
            val_arr[i] = malloc(size_arr[i]);
            if (!val_arr[i]) {
                set_error(error_out, "Failed to allocate memory for value");
                free(key_arr[i]);
                key_arr[i] = nullptr;
                for (int j = 0; j < i; j++) {
                    free(key_arr[j]);
                    free(val_arr[j]);
                }
                free(key_arr);
                free(val_arr);
                free(size_arr);
                *keys_out = nullptr;
                *values_out = nullptr;
                *sizes_out = nullptr;
                return -1;
            }
            memcpy(val_arr[i], kv.second.data(), size_arr[i]);
        }
        // size==0 时 val_arr[i] 保持 nullptr：Go 侧 size>0 才读取，free(nullptr) 安全
        i++;
    }

    *keys_out = key_arr;
    *values_out = val_arr;
    *sizes_out = size_arr;
    return 0;
}

int ray_gcs_client_kv_put(CGcsClient* client,
                          const char* ns,
                          const char* key,
                          const void* value,
                          size_t size,
                          int overwrite,
                          int* success_out,
                          char** error_out) {
    if (!client || !client->gcs_client || !key || !success_out) {
        set_error(error_out, "Invalid arguments");
        return -1;
    }

    std::string ns_str(ns ? ns : "");
    std::string key_str(key);
    std::string value_str;
    if (size > 0 && value) {
        value_str.assign(static_cast<const char*>(value), size);
    }

    bool added = false;
    auto status = client->gcs_client->InternalKV().Put(
        ns_str, key_str, value_str, overwrite != 0, ray::rpc::GetGcsTimeoutMs(), added);

    if (!status.ok()) {
        set_error(error_out, status.ToString().c_str());
        return -1;
    }
    // added=true 表示新增；overwrite=false 且键已存在时 added=false（非错误）
    *success_out = added ? 1 : 0;
    return 0;
}

int ray_gcs_client_kv_del(CGcsClient* client,
                          const char* ns,
                          const char* key,
                          int del_by_prefix,
                          int* count_out,
                          char** error_out) {
    if (!client || !client->gcs_client || !key || !count_out) {
        set_error(error_out, "Invalid arguments");
        return -1;
    }

    std::string ns_str(ns ? ns : "");
    std::string key_str(key);

    int num_deleted = 0;
    auto status = client->gcs_client->InternalKV().Del(
        ns_str, key_str, del_by_prefix != 0, ray::rpc::GetGcsTimeoutMs(), num_deleted);

    if (!status.ok()) {
        set_error(error_out, status.ToString().c_str());
        return -1;
    }
    *count_out = num_deleted;
    return 0;
}

int ray_gcs_client_kv_keys(CGcsClient* client,
                           const char* ns,
                           const char* prefix,
                           char*** keys_out,
                           int* count_out,
                           char** error_out) {
    if (!client || !client->gcs_client || !keys_out || !count_out) {
        set_error(error_out, "Invalid arguments");
        return -1;
    }

    std::string ns_str(ns ? ns : "");
    std::string prefix_str(prefix ? prefix : "");

    std::vector<std::string> keys;
    auto status = client->gcs_client->InternalKV().Keys(
        ns_str, prefix_str, ray::rpc::GetGcsTimeoutMs(), keys);

    if (!status.ok()) {
        set_error(error_out, status.ToString().c_str());
        return -1;
    }

    // 真实 API 返回的 key 已不含 namespace 前缀，直接输出即可
    if (!allocate_string_array(keys, keys_out, count_out)) {
        set_error(error_out, "Failed to allocate memory for keys array");
        return -1;
    }
    return 0;
}

int ray_gcs_client_kv_exists(CGcsClient* client,
                             const char* ns,
                             const char* key,
                             int* exists_out,
                             char** error_out) {
    if (!client || !client->gcs_client || !key || !exists_out) {
        set_error(error_out, "Invalid arguments");
        return -1;
    }

    std::string ns_str(ns ? ns : "");
    std::string key_str(key);

    bool exists = false;
    auto status = client->gcs_client->InternalKV().Exists(
        ns_str, key_str, ray::rpc::GetGcsTimeoutMs(), exists);

    if (!status.ok()) {
        set_error(error_out, status.ToString().c_str());
        return -1;
    }
    *exists_out = exists ? 1 : 0;
    return 0;
}

}  // extern "C"
