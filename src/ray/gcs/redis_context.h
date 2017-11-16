// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef RAY_GCS_REDIS_CONTEXT_H
#define RAY_GCS_REDIS_CONTEXT_H

#include <unordered_map>

#include "ray/id.h"
#include "ray/status.h"
#include "ray/util/logging.h"

struct redisContext;
struct redisAsyncContext;
struct aeEventLoop;

namespace ray {

namespace gcs {

class RedisCallbackManager {
 public:
  using RedisCallback = std::function<void(void)>;

  static RedisCallbackManager& instance() {
    static RedisCallbackManager instance;
    return instance;
  }

  int64_t add(const std::function<void(void)>& function);

  RedisCallback& get(int64_t callback_index);

 private:
  RedisCallbackManager() : num_callbacks(0) {};

  ~RedisCallbackManager() {
    printf("shut down callback manager\n");
  }

  int64_t num_callbacks;
  std::unordered_map<int64_t, std::unique_ptr<RedisCallback>> callbacks_;
};

class RedisContext {
 public:
  RedisContext() {}
  ~RedisContext();
  Status Connect(const std::string& address, int port);
  Status AttachToEventLoop(aeEventLoop* loop);
  Status RunAsync(const std::string& command, const UniqueID& id, uint8_t* data, int64_t length, int64_t callback_index);
 private:
  redisContext* context_;
  redisAsyncContext* async_context_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_CONTEXT_H
