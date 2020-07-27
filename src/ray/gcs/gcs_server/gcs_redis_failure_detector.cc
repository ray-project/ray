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

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {

GcsRedisFailureDetector::GcsRedisFailureDetector(
    boost::asio::io_service &io_service, std::shared_ptr<RedisContext> redis_context,
    std::function<void()> callback)
    : redis_context_(redis_context),
      detect_timer_(io_service),
      callback_(std::move(callback)) {}

void GcsRedisFailureDetector::Start() {
  RAY_LOG(INFO) << "Starting redis failure detector.";
  Tick();
}

void GcsRedisFailureDetector::DetectRedis() {
  auto *reply = reinterpret_cast<redisReply *>(
      redisCommand(redis_context_->sync_context(), "PING"));
  if (reply == nullptr || reply->type == REDIS_REPLY_NIL) {
    RAY_LOG(ERROR) << "Redis is inactive.";
    callback_();
  } else {
    freeReplyObject(reply);
  }
}

/// A periodic timer that checks for timed out clients.
void GcsRedisFailureDetector::Tick() {
  DetectRedis();
  ScheduleTick();
}

void GcsRedisFailureDetector::ScheduleTick() {
  auto detect_period = boost::posix_time::milliseconds(
      RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds());
  detect_timer_.expires_from_now(detect_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    }
    RAY_CHECK(!error) << "Detecting redis failed with error: " << error.message();
    Tick();
  });
}

}  // namespace gcs
}  // namespace ray