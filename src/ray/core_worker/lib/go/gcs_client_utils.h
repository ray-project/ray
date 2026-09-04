// src/ray/core_worker/lib/go/gcs_client_utils.h
// CGO 桥接辅助函数和通用工具
#pragma once

#include <vector>
#include <string>

// 辅助函数：设置错误消息
void set_error(char** error_out, const char* msg);

// 辅助函数：分配序列化字符串数组（带大小）
// 返回 false 表示内存分配失败
bool allocate_serialized_array(
    const std::vector<std::string>& data,
    char*** serialized_out,
    int** sizes_out,
    int* count_out);

// 辅助函数：分配字符串数组（不带大小，用于字符串 ID）
// 返回 false 表示内存分配失败
bool allocate_string_array(
    const std::vector<std::string>& data,
    char*** string_out,
    int* count_out);
