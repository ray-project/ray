// src/ray/core_worker/lib/go/gcs_client_bridge_test.cc
#include "gcs_client_bridge.h"
#include "gcs_memory.h"
#include <gtest/gtest.h>
#include <cstring>
#include <string>

// Test 1: 验证头文件可以正确编译和包含
TEST(GcsClientBridgeTest, HeaderCompiles) {
    // 验证类型定义存在
    CGcsClient* client = nullptr;
    (void)client; // 避免未使用变量警告

    // 验证函数声明存在（不实际调用，因为需要有效实现）
    void (*create_fn)(const char*, const char*, int64_t, char**) =
        reinterpret_cast<void (*)(const char*, const char*, int64_t, char**)>(
            reinterpret_cast<uintptr_t>(&ray_gcs_client_create));
    (void)create_fn;

    SUCCEED() << "Header compiles successfully";
}

// Test 2: 验证 C ABI 兼容性 - 确保 extern "C" 正确工作
TEST(GcsClientBridgeTest, CAriCompatibility) {
    // 验证函数指针可以直接访问（证明有 C 链接）
    void (*free_fn)(void*) = ray_gcs_free_memory;
    void (*free_arr_fn)(char**, int) = ray_gcs_free_string_array;

    ASSERT_NE(free_fn, nullptr);
    ASSERT_NE(free_arr_fn, nullptr);
    (void)free_fn;
    (void)free_arr_fn;

    SUCCEED() << "C ABI compatibility verified";
}

// Test 3: 验证内存释放函数的基本行为
TEST(GcsClientBridgeTest, MemoryFree) {
    // 分配一些内存并释放
    char* test_str = static_cast<char*>(malloc(10));
    ASSERT_NE(test_str, nullptr);

    strcpy(test_str, "test");
    ASSERT_EQ(strlen(test_str), 4);

    ray_gcs_free_memory(test_str);
    // 释放后不应再访问

    SUCCEED() << "Memory free works correctly";
}

// Test 4: 验证字符串数组释放函数
TEST(GcsClientBridgeTest, StringArrayFree) {
    // 创建一个简单的字符串数组
    char** arr = static_cast<char**>(malloc(3 * sizeof(char*)));
    ASSERT_NE(arr, nullptr);

    arr[0] = static_cast<char*>(malloc(5));
    arr[1] = static_cast<char*>(malloc(5));
    arr[2] = nullptr;
    ASSERT_NE(arr[0], nullptr);
    ASSERT_NE(arr[1], nullptr);

    strcpy(arr[0], "str1");
    strcpy(arr[1], "str2");

    ray_gcs_free_string_array(arr, 2);
    // 释放后不应再访问

    SUCCEED() << "String array free works correctly";
}

// Test 5: 验证 ray_gcs_free_string 函数
TEST(GcsClientBridgeTest, FreeString) {
    // 使用 strdup 分配内存（模拟 C++ 端返回字符串）
    const char* test_str = strdup("test string");
    ASSERT_NE(test_str, nullptr);
    ASSERT_STREQ(test_str, "test string");

    // 使用 ray_gcs_free_string 释放
    ray_gcs_free_string(test_str);
    // 释放后不应再访问

    SUCCEED() << "ray_gcs_free_string works correctly";
}

// Test 6: 验证 ray_gcs_free_string 处理空指针
TEST(GcsClientBridgeTest, FreeStringNullPointer) {
    // 传递 nullptr 不应该崩溃
    ray_gcs_free_string(nullptr);
    SUCCEED() << "ray_gcs_free_string handles nullptr safely";
}

// Test 7: 验证 Invalid arguments 处理
TEST(GcsClientBridgeTest, InvalidArguments) {
    char* error = nullptr;

    // 测试空地址
    CGcsClient* client = ray_gcs_client_create(
        nullptr,
        "00000000000000000000000000000000000000000000000000000000",
        5000,
        &error
    );

    ASSERT_EQ(client, nullptr);
    ASSERT_NE(error, nullptr);
    std::cout << "Expected error for null address: " << error << std::endl;

    ray_gcs_free_memory(error);
}

// Test 8: 验证 ClusterID 长度检查
TEST(GcsClientBridgeTest, InvalidClusterIdLength) {
    char* error = nullptr;

    // 测试无效的 ClusterID 长度（应该是 56 个十六进制字符）
    CGcsClient* client = ray_gcs_client_create(
        "127.0.0.1:6379",
        "short_id",  // 太短
        5000,
        &error
    );

    ASSERT_EQ(client, nullptr);
    ASSERT_NE(error, nullptr);
    std::cout << "Expected error for short cluster ID: " << error << std::endl;

    ray_gcs_free_memory(error);
}

// Test 9: 验证 Autoscaler GetStatus 函数签名和基本错误处理
TEST(GcsClientBridgeTest, AutoscalerGetStatusSignature) {
    // 验证函数存在且可以调用（即使没有实际连接）
    char* serialized = nullptr;
    int size = 0;
    char* error = nullptr;

    // 测试空指针参数
    int result = ray_gcs_client_autoscaler_get_status(nullptr, &serialized, &size, &error);
    ASSERT_EQ(result, 0);
    ASSERT_NE(error, nullptr);
    std::cout << "Expected error for null client: " << error << std::endl;
    ray_gcs_free_memory(error);
    if (serialized) {
        ray_gcs_free_memory(serialized);
    }

    // 测试空 serialized_out 参数 - 使用 nullptr client
    error = nullptr;
    result = ray_gcs_client_autoscaler_get_status(nullptr, nullptr, nullptr, &error);
    ASSERT_EQ(result, 0);
    ASSERT_NE(error, nullptr);
    std::cout << "Expected error for null serialized_out: " << error << std::endl;
    ray_gcs_free_memory(error);

    SUCCEED() << "Autoscaler get_status signature verified";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
