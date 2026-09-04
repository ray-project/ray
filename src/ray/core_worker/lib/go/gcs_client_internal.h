// src/ray/core_worker/lib/go/gcs_client_internal.h
// GCS Client 内部结构定义 - 仅供内部实现使用
#pragma once

#include <string>
#include <memory>
#include <thread>
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "ray/asio/instrumented_io_context.h"

// 内部 CGO 桥接结构 - 不暴露给 Go 层
struct CGcsClient {
    std::string address;
    std::string cluster_id_hex;
    int64_t timeout_ms;

    // 实际的 GCS 客户端（通过 InternalKV() 访问真实的 GCS KV 存储）
    std::shared_ptr<ray::gcs::GcsClient> gcs_client;

    // GlobalStateAccessor 用于同步访问 GCS 数据
    std::unique_ptr<ray::gcs::GlobalStateAccessor> global_state_accessor;

    // io_service 生命周期管理：提升为成员变量避免悬空引用
    std::unique_ptr<instrumented_io_context> io_service;

    // io_service 后台运行线程
    std::unique_ptr<std::thread> io_thread;
};
