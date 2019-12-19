#ifndef RAY_STREAMING_STATUS_H
#define RAY_STREAMING_STATUS_H
#include <ostream>
#include <sstream>
#include <string>

namespace ray {
namespace streaming {

enum class StreamingStatus : uint32_t {
  OK = 0,
  ReconstructTimeOut = 1,
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

#define RETURN_IF_NOT_OK(STATUS_EXP)    \
  {                                     \
    StreamingStatus state = STATUS_EXP; \
    if (StreamingStatus::OK != state) { \
      return state;                     \
    }                                   \
  }

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_STATUS_H
