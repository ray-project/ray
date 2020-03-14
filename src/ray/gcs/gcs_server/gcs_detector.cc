#include "gcs_detector.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {
namespace gcs {

GcsDetector::GcsDetector(boost::asio::io_service &io_service,
                         std::shared_ptr<gcs::RedisGcsClient> gcs_client,
                         std::function<void()> destroy_callback)
    : gcs_client_(std::move(gcs_client)),
      detect_timer_(io_service),
      destroy_callback_(destroy_callback) {
  Start();
}

void GcsDetector::Start() {
  RAY_LOG(INFO) << "Starting gcs node manager.";
  Tick();
}

void GcsDetector::DetectGcs() {
  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(gcs_client_->primary_context()->sync_context(), "PING"));
  if (reply == nullptr || reply->type == REDIS_REPLY_NIL) {
    RAY_LOG(INFO) << "Failed..............";
    destroy_callback_();
  } else {
    std::string result(reply->str);
    freeReplyObject(reply);
  }
}

/// A periodic timer that checks for timed out clients.
void GcsDetector::Tick() {
  DetectGcs();
  ScheduleTick();
}

void GcsDetector::ScheduleTick() {
  auto detect_period = boost::posix_time::milliseconds(
      RayConfig::instance().gcs_detect_timeout_milliseconds());
  detect_timer_.expires_from_now(detect_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `heartbeat_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking gcs detect failed with error: " << error.message();
    Tick();
  });
}

}  // namespace gcs
}  // namespace ray