#ifndef RAY_STREAMING_H
#define RAY_STREAMING_H
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <ostream>
#include <sstream>
#include <string>

#include "plasma/client.h"
#include "plasma/common.h"
#include "queue_interface.h"
#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"
#include "ray/raylet/raylet_client.h"

#include "streaming_asio.h"
#include "streaming_channel.h"
#include "streaming_config.h"
#include "streaming_logging.h"
#include "streaming_utility.h"

namespace ray {
namespace streaming {

enum class StreamingStatus : uint32_t {
  OK = 0,
  ReconstructTimeOut = 1,
  FullPlasmaStore = 2,
  QueueIdNotFound = 3,
  ResubscribeFailed = 4,
  EmptyRingBuffer = 5,
  FullChannel = 6,
  NoSuchItem = 7,
  InitQueueFailed = 8,
  GetBundleTimeOut = 9,
  SkipSendEmptyMessage = 10,
  Interrupted = 11,
  WaitQueueTimeOut = 12,
  OutOfMemory = 13,
  Invalid = 14,
  UnknownError = 15,
  TailStatus = 999,
  MIN = OK,
  MAX = TailStatus
};

static inline std::ostream &operator<<(std::ostream &os, const StreamingStatus &status) {
  os << static_cast<std::underlying_type<StreamingStatus>::type>(status);
  return os;
}

enum class StreamingChannelState : uint8_t {
  Init = 0,
  Running = 1,
  Interrupted = 2,
  Rescaling = 3
};

#define RETURN_IF_NOT_OK(STATUS_EXP)    \
  {                                     \
    StreamingStatus state = STATUS_EXP; \
    if (StreamingStatus::OK != state) { \
      return state;                     \
    }                                   \
  }

class StreamingCommon {
 public:
  StreamingCommon();
  virtual ~StreamingCommon();
  virtual StreamingConfig GetConfig() const;
  virtual void SetConfig(const StreamingConfig &config);
  virtual void SetConfig(const uint8_t *, uint32_t buffer_len);
  // Streaming raylet client reuse actor driver id that may necessary to
  // feature of ray queue cleanup queue.
  virtual void CreateRayletClient(const ray::JobID &job_id);
  StreamingChannelState GetChannelState();

  friend std::ostream &operator<<(std::ostream &os, const StreamingCommon &common);

 protected:
  StreamingConfig config_;
  StreamingChannelState channel_state_;
  RayletClient *raylet_client_ = nullptr;

 private:
  bool is_streaming_log_init_;
};

void set_streaming_log_config(
    const std::string &app_name = "streaming",
    const StreamingLogLevel &log_level = StreamingLogLevel::INFO,
    const int &log_buffer_flush_in_secs = 0,
    const std::string &log_dir = "/tmp/streaminglogs/");

void streaming_log_shutdown();

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_H
