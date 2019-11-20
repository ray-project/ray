#include <atomic>
#include <cstdlib>
#include <mutex>

#include "streaming.h"
#include "streaming.pb.h"

namespace ray {
namespace streaming {

StreamingConfig StreamingCommon::GetConfig() const { return config_; }

void StreamingCommon::SetConfig(const StreamingConfig &streaming_config) {
  STREAMING_CHECK(channel_state_ == StreamingChannelState::Init)
      << "set config must be at beginning";
  config_ = streaming_config;
}

void StreamingCommon::SetConfig(const uint8_t *data, uint32_t size) {
  STREAMING_CHECK(channel_state_ == StreamingChannelState::Init)
      << "set config must be at beginning";
  if (!data) {
    STREAMING_LOG(WARNING) << "buffer pointer is null, but len is => " << size;
    return;
  }
  config_.FromProto(data, size);
}

StreamingCommon::~StreamingCommon() {}

StreamingChannelState StreamingCommon::GetChannelState() { return channel_state_; }

StreamingCommon::StreamingCommon() : channel_state_(StreamingChannelState::Init) {}

}  // namespace streaming
}  // namespace ray
