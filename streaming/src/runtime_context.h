#pragma once

#include <string>

#include "common/status.h"
#include "config/streaming_config.h"

namespace ray {
namespace streaming {

enum class RuntimeStatus : uint8_t { Init = 0, Running = 1, Interrupted = 2 };

#define RETURN_IF_NOT_OK(STATUS_EXP)    \
  {                                     \
    StreamingStatus state = STATUS_EXP; \
    if (StreamingStatus::OK != state) { \
      return state;                     \
    }                                   \
  }

class RuntimeContext {
 public:
  RuntimeContext();
  virtual ~RuntimeContext();
  inline const StreamingConfig &GetConfig() const { return config_; };
  void SetConfig(const StreamingConfig &config);
  void SetConfig(const uint8_t *data, uint32_t buffer_len);
  inline RuntimeStatus GetRuntimeStatus() { return runtime_status_; }
  inline void SetRuntimeStatus(RuntimeStatus status) { runtime_status_ = status; }
  inline void MarkMockTest() { is_mock_test_ = true; }
  inline bool IsMockTest() { return is_mock_test_; }

 private:
  StreamingConfig config_;
  RuntimeStatus runtime_status_;
  bool is_mock_test_ = false;
};

}  // namespace streaming
}  // namespace ray
