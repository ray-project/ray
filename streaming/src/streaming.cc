#include <atomic>
#include <cstdlib>
#include <mutex>

#include "format/streaming_generated.h"
#include "streaming.h"

namespace ray {
namespace streaming {

StreamingConfig StreamingCommon::GetConfig() const { return config_; }

void StreamingCommon::SetConfig(const StreamingConfig &streaming_config) {
  STREAMING_CHECK(channel_state_ == StreamingChannelState::Init)
      << "set config must be at beginning";
  config_ = streaming_config;
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

  config_.SetStreamingRole(conf_fb_instance->role());

  ray::JobID task_job_id = ray::JobID::FromInt(-1);
  if (conf_fb_instance->task_job_id() &&
      conf_fb_instance->task_job_id()->size() == 2 * JobID::Size()) {
    config_.SetStreamingTaskJobId(conf_fb_instance->task_job_id()->str());
    task_job_id = ray::JobID::FromBinary(
        StreamingUtility::Hexqid2str(config_.GetStreamingTaskJobId()));
    STREAMING_LOG(INFO) << "str = > " << task_job_id << ", hex " << task_job_id.Hex();
  }
}

StreamingCommon::~StreamingCommon() {}

StreamingChannelState StreamingCommon::GetChannelState() { return channel_state_; }

StreamingCommon::StreamingCommon() : channel_state_(StreamingChannelState::Init) {}

}  // namespace streaming
}  // namespace ray
