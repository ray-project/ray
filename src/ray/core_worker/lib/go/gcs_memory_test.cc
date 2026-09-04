#include "gcs_memory.h"

#include <cstring>
#include <gtest/gtest.h>

// 测试释放单个内存块
TEST(GcsMemoryTest, FreeMemory) {
  // 分配内存
  void* ptr = malloc(100);
  ASSERT_NE(ptr, nullptr);

  // 写入测试数据
  memset(ptr, 0xAB, 100);

  // 释放内存 - 这是 Go CGO 会调用的函数
  ray_gcs_free_memory(ptr);

  // 注意：free 后访问内存是未定义行为，这里只验证函数不崩溃
  SUCCEED();
}

// 测试释放 NULL 指针
TEST(GcsMemoryTest, FreeMemoryNullPointer) {
  // 传递 NULL 指针应该安全处理
  ray_gcs_free_memory(nullptr);
  SUCCEED();
}

// 测试释放字符串数组
TEST(GcsMemoryTest, FreeStringArray) {
  const int count = 3;
  char** arr = static_cast<char**>(malloc(count * sizeof(char*)));
  ASSERT_NE(arr, nullptr);

  // 分配每个字符串
  for (int i = 0; i < count; i++) {
    arr[i] = static_cast<char*>(malloc(10));
    ASSERT_NE(arr[i], nullptr);
    snprintf(arr[i], 10, "str%d", i);
  }

  // 释放字符串数组
  ray_gcs_free_string_array(arr, count);

  SUCCEED();
}

// 测试释放空字符串数组
TEST(GcsMemoryTest, FreeStringArrayNull) {
  // 传递 NULL 数组应该安全处理
  ray_gcs_free_string_array(nullptr, 5);
  SUCCEED();
}

// 测试释放包含 NULL 元素的字符串数组
TEST(GcsMemoryTest, FreeStringArrayWithNullElements) {
  const int count = 3;
  char** arr = static_cast<char**>(malloc(count * sizeof(char*)));
  ASSERT_NE(arr, nullptr);

  // 部分元素为 NULL
  arr[0] = static_cast<char*>(malloc(10));
  arr[1] = nullptr;  // NULL 元素
  arr[2] = static_cast<char*>(malloc(10));

  // 释放字符串数组 - 应该正确处理 NULL 元素
  ray_gcs_free_string_array(arr, count);

  SUCCEED();
}
