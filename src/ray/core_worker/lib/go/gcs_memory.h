// src/ray/core_worker/lib/go/gcs_memory.h
#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// 释放 C++ 分配的内存块 - Go 必须在使用完毕后调用
void ray_gcs_free_memory(void* ptr);

// 释放字符串数组（每个元素 + 数组本身）
void ray_gcs_free_string_array(char** arr, int count);

#ifdef __cplusplus
}
#endif
