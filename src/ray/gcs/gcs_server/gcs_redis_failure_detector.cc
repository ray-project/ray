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

#include "ray/gcs/gcs_server/gcs_redis_failure_detector.h"

#include "ray/common/ray_config.h"

namespace ray {
namespace gcs {

GcsRedisFailureDetector::GcsRedisFailureDetector(
    instrumented_io_context &io_service,
    std::shared_ptr<RedisContext> redis_context,
    std::function<void()> callback)
    : redis_context_(redis_context),
      periodical_runner_(io_service),
      callback_(std::move(callback)) {}

void GcsRedisFailureDetector::Start() {
  RAY_LOG(INFO) << "Starting redis failure detector.";
  periodical_runner_.RunFnPeriodically(
      [this] { DetectRedis(); },
      RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds(),
      "GcsRedisFailureDetector.deadline_timer.detect_redis_failure");
}

void GcsRedisFailureDetector::DetectRedis() {
  auto redis_callback = [this](const std::shared_ptr<CallbackReply> &reply) {
    if (reply->IsNil()) {
      RAY_LOG(ERROR) << "Redis is inactive.";
      callback_();
    }
  };

  Status status = redis_context_->RunArgvAsync({"PING"}, redis_callback);

  if (!status.ok()) {
    RAY_LOG(ERROR) << "Redis is disconnected.";
    callback_();
  }
}

}  // namespace gcs
}  // namespace ray
