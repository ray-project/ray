// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// src/ray/core_worker/lib/go/gcs_client_internal.h
// GCS Client internal struct definitions - for internal implementation only
#pragma once

#include <string>
#include <memory>
#include <thread>
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/gcs_rpc_client/global_state_accessor.h"
#include "ray/asio/instrumented_io_context.h"

// Internal CGO bridge struct - not exposed to the Go layer
struct CGcsClient {
    std::string address;
    std::string cluster_id_hex;
    int64_t timeout_ms;

    // The actual GCS client (accesses real GCS KV storage via InternalKV())
    std::shared_ptr<ray::gcs::GcsClient> gcs_client;

    // GlobalStateAccessor for synchronous access to GCS data
    std::unique_ptr<ray::gcs::GlobalStateAccessor> global_state_accessor;

    // io_service lifetime management: promoted to a member to avoid dangling references
    std::unique_ptr<instrumented_io_context> io_service;

    // io_service background thread
    std::unique_ptr<std::thread> io_thread;
};
