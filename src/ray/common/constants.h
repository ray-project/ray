// Copyright 2017 The Ray Authors.
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

#pragma once

#include <limits.h>
#include <stdint.h>

/// Length of Ray full-length IDs in bytes.
constexpr size_t kUniqueIDSize = 28;

/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. This is the maximum width of the object index in bits.
constexpr int kObjectIdIndexSize = 32;
static_assert(kObjectIdIndexSize % CHAR_BIT == 0,
              "ObjectID prefix not a multiple of bytes");

/// Raylet exit code on plasma store socket error.
constexpr int kRayletStoreErrorExitCode = 100;

/// Prefix for the object table keys in redis.
constexpr char kObjectTablePrefix[] = "ObjectTable";
/// Prefix for the task table keys in redis.
constexpr char kTaskTablePrefix[] = "TaskTable";

constexpr char kWorkerDynamicOptionPlaceholder[] =
    "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER";

constexpr char kWorkerRayletConfigPlaceholder[] = "RAY_WORKER_RAYLET_CONFIG_PLACEHOLDER";

/// Public DNS address which is is used to connect and get local IP.
constexpr char kPublicDNSServerIp[] = "8.8.8.8";
constexpr int kPublicDNSServerPort = 53;

constexpr char kEnvVarKeyJobId[] = "RAY_JOB_ID";
