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

#include "ray/gcs/gcs_server/store_client_kv.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

struct ClusterConfig {
  std::string session_name;
  std::string session_dir;
  std::string temp_dir;
};

bool InitClusterConfig(const RedisClientOptions &options,
                       bool is_head,
                       ClusterConfig &config) {
  static const std::string kSessionNamespace = "session";
  RAY_LOG(INFO) << "AAAA";

  instrumented_io_context io_context;

  auto redis_client = std::make_shared<RedisClient>(options);
  auto status = redis_client->Connect(io_context);
  StoreClientInternalKV store_client{
      std::make_unique<ray::gcs::RedisStoreClient>(std::move(redis_client))};

  if (is_head) {
    RAY_LOG(INFO) << "????";
    RAY_CHECK(!config.session_name.empty()) << "Expected session name to be provided.";
    RAY_CHECK(!config.temp_dir.empty()) << "Expected temp dir to be provided.";
    RAY_LOG(INFO) << "hello";
    auto write_to_kv = [&]() {
      config.session_dir = ray::JoinPaths(config.temp_dir, config.session_name);
      store_client.Put(
          kSessionNamespace, "temp_dir", config.temp_dir, true, [&](bool success) {
            RAY_CHECK(success) << "Unable to persist temp dir.";
            store_client.Put(kSessionNamespace,
                             "session_dir",
                             config.session_dir,
                             true,
                             [&](bool success) {
                               RAY_CHECK(success) << "Unable to persist session dir.";
                             });
          });
    };

    store_client.Put(
        kSessionNamespace,
        "session_name",
        config.session_name,
        false,
        [&](bool overwrote) {
          if (!overwrote) {
            store_client.Get(
                kSessionNamespace,
                "session_name",
                [&](const std::optional<std::string> result) {
                  RAY_CHECK(!result.value().empty())
                      << "Unexpectedly failed to get session name from Redis.";
                  config.session_name = result.value();
                  write_to_kv();
                });
          } else {
            write_to_kv();
          }
        });

    io_context.run();
    return true;
  }

  io_context.run();
  return false;
}

}  // namespace gcs
}  // namespace ray