// src/ray/core_worker/lib/go/gcs_client_utils.cc
// CGO 桥接辅助函数实现
#include "gcs_client_utils.h"
#include <cstdlib>
#include <cstring>

// 辅助函数：设置错误消息
void set_error(char** error_out, const char* msg) {
    if (error_out && msg) {
        size_t len = strlen(msg);
        *error_out = static_cast<char*>(malloc(len + 1));
        if (*error_out) {
            strcpy(*error_out, msg);
        }
    }
}

// 辅助函数：分配序列化字符串数组（带大小）
bool allocate_serialized_array(
    const std::vector<std::string>& data,
    char*** serialized_out,
    int** sizes_out,
    int* count_out) {

    int result_count = static_cast<int>(data.size());
    *count_out = result_count;

    if (result_count == 0) {
        *serialized_out = nullptr;
        *sizes_out = nullptr;
        return true;
    }

    *serialized_out = static_cast<char**>(malloc(result_count * sizeof(char*)));
    *sizes_out = static_cast<int*>(malloc(result_count * sizeof(int)));

    if (!*serialized_out || !*sizes_out) {
        if (*serialized_out) free(*serialized_out);
        if (*sizes_out) free(*sizes_out);
        *serialized_out = nullptr;
        *sizes_out = nullptr;
        return false;
    }

    for (int i = 0; i < result_count; i++) {
        size_t data_size = data[i].size();
        (*serialized_out)[i] = static_cast<char*>(malloc(data_size));
        (*sizes_out)[i] = static_cast<int>(data_size);

        if (!(*serialized_out)[i]) {
            for (int j = 0; j < i; j++) {
                free((*serialized_out)[j]);
            }
            free(*serialized_out);
            free(*sizes_out);
            *serialized_out = nullptr;
            *sizes_out = nullptr;
            return false;
        }
        memcpy((*serialized_out)[i], data[i].c_str(), data_size);
    }

    return true;
}

// 辅助函数：分配字符串数组（不带大小，用于字符串 ID）
bool allocate_string_array(
    const std::vector<std::string>& data,
    char*** string_out,
    int* count_out) {

    int result_count = static_cast<int>(data.size());
    *count_out = result_count;

    if (result_count == 0) {
        *string_out = nullptr;
        return true;
    }

    *string_out = static_cast<char**>(malloc(result_count * sizeof(char*)));
    if (!*string_out) {
        return false;
    }

    for (int i = 0; i < result_count; i++) {
        size_t str_len = data[i].size();
        (*string_out)[i] = static_cast<char*>(malloc(str_len + 1));
        if (!(*string_out)[i]) {
            for (int j = 0; j < i; j++) {
                free((*string_out)[j]);
            }
            free(*string_out);
            *string_out = nullptr;
            return false;
        }
        strcpy((*string_out)[i], data[i].c_str());
    }

    return true;
}
