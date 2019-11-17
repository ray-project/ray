#ifndef RAY_STREAMING_UTILITY_H
#define RAY_STREAMING_UTILITY_H
#include <string>

#include "streaming.h"

namespace ray {
namespace streaming {

typedef std::map<std::string, std::string> TagMap;

class StreamingUtility {
 public:
  static std::string Byte2hex(const uint8_t *data, uint32_t data_size);

  static std::string Hexqid2str(const std::string &q_id_hex);

  static std::string GetHostname();
};

class AutoSpinLock {
 public:
  explicit AutoSpinLock(std::atomic_flag &lock) : lock_(lock) {
    while (lock_.test_and_set(std::memory_order_acquire))
      ;
  }
  ~AutoSpinLock() { unlock(); }
  void unlock() { lock_.clear(std::memory_order_release); }

 private:
  std::atomic_flag &lock_;
};

inline int64_t current_sys_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_UTILITY_H
