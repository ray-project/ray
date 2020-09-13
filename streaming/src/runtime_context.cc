#include "runtime_context.h"

#include "ray/common/id.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/common.pb.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

void RuntimeContext::SetConfig(const StreamingConfig &streaming_config) {
  STREAMING_CHECK(runtime_status_ == RuntimeStatus::Init)
      << "set config must be at beginning";
  config_ = streaming_config;
}

void RuntimeContext::SetConfig(const uint8_t *data, uint32_t size) {
  STREAMING_CHECK(runtime_status_ == RuntimeStatus::Init)
      << "set config must be at beginning";
  if (!data) {
    STREAMING_LOG(WARNING) << "buffer pointer is null, but len is => " << size;
    return;
  }
  config_.FromProto(data, size);
}

RuntimeContext::~RuntimeContext() {}

RuntimeContext::RuntimeContext() : runtime_status_(RuntimeStatus::Init) {}

}  // namespace streaming
}  // namespace ray
