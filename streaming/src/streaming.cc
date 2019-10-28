#include <plasma/common.h>
#include <atomic>
#include <cstdlib>
#include <mutex>

#include "format/streaming_generated.h"
#include "streaming.h"

#include "ray/metrics/metrics_util.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace streaming {

StreamingConfig StreamingCommon::GetConfig() const { return config_; }

void StreamingCommon::SetConfig(const StreamingConfig &streaming_config) {
  STREAMING_CHECK(channel_state_ == StreamingChannelState::Init)
      << "set config must be at beginning";
  config_ = streaming_config;
  if (config_.GetStreaming_task_job_id().size() == 2 * kUniqueIDSize) {
    CreateRayletClient(ray::JobID::FromBinary(
        StreamingUtility::Hexqid2str(config_.GetStreaming_task_job_id())));
  } else {
    CreateRayletClient(ray::JobID::FromInt(-1));
  }
}

void StreamingCommon::SetConfig(const uint8_t *buffer_pointer, uint32_t buffer_len) {
  STREAMING_CHECK(channel_state_ == StreamingChannelState::Init)
      << "set config must be at beginning";
  if (!buffer_pointer) {
    STREAMING_LOG(WARNING) << "buffer pointer is null, but len is => " << buffer_len;
    return;
  }
  auto verifer = flatbuffers::Verifier(buffer_pointer, buffer_len);
  bool valid_fbs = streaming::fbs::VerifyStreamingConfigBuffer(verifer);
  if (!valid_fbs) {
    STREAMING_LOG(WARNING) << "invalid fbs buffer";
    return;
  }
  auto conf_fb_instance =
      streaming::fbs::GetStreamingConfig(reinterpret_cast<const void *>(buffer_pointer));
  auto str_config = conf_fb_instance->string_config();

  for (auto it = str_config->begin(); it != str_config->end(); it++) {
    const streaming::fbs::StreamingConfigKey &key = it->key();
    const std::string &value = it->value()->str();
    config_.ReloadProperty(key, value);
  }

  auto uint_config = conf_fb_instance->uint_config();

  for (auto it = uint_config->begin(); it != uint_config->end(); it++) {
    const streaming::fbs::StreamingConfigKey &key = it->key();
    const uint32_t value = it->value();
    config_.ReloadProperty(key, value);
  }

  config_.SetStreaming_role(conf_fb_instance->role());
  config_.SetStreaming_rollback_checkpoint_id(conf_fb_instance->checkpoint_id());

  ray::JobID task_job_id = ray::JobID::FromInt(-1);
  if (conf_fb_instance->task_job_id() &&
      conf_fb_instance->task_job_id()->size() == 2 * JobID::Size()) {
    config_.SetStreaming_task_job_id(conf_fb_instance->task_job_id()->str());
    task_job_id = ray::JobID::FromBinary(
        StreamingUtility::Hexqid2str(config_.GetStreaming_task_job_id()));
    STREAMING_LOG(INFO) << "str = > " << task_job_id << ", hex " << task_job_id.Hex();
  }
  CreateRayletClient(task_job_id);
}

StreamingCommon::~StreamingCommon() {
  if (raylet_client_ != nullptr) {
    delete raylet_client_;
    STREAMING_LOG(INFO) << "free new raylet client instance";
  }
}


void StreamingCommon::CreateRayletClient(const JobID &job_id) {
  if (raylet_client_ != nullptr) {
    return;
  }
  if (config_.GetStreaming_raylet_socket_path().size()) {
    // only java and python lang are supported
    STREAMING_LOG(INFO) << "Raylet socket name: "
                        << config_.GetStreaming_raylet_socket_path();
    // object id can't be converted to driver id, so that create new instance
    // from object id
    ray::JobID raylet_client_job_id = ray::JobID::FromBinary(job_id.Binary());
    raylet_client_ = new RayletClient(config_.GetStreaming_raylet_socket_path(),
                                      ray::WorkerID::FromRandom(), false,
                                      raylet_client_job_id, rpc::Language::CPP, -1, true);
    if (raylet_client_) {
      STREAMING_LOG(INFO) << "new raylet client succ, "
                          << reinterpret_cast<long>(raylet_client_);
    } else {
      STREAMING_LOG(WARNING) << "new raylet client failed, use old client";
    }
  }
}

StreamingChannelState StreamingCommon::GetChannelState() { return channel_state_; }

StreamingCommon::StreamingCommon()
    : channel_state_(StreamingChannelState::Init) {}


}  // namespace streaming
}  // namespace ray
