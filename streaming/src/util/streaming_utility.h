#ifndef RAY_STREAMING_UTILITY_H
#define RAY_STREAMING_UTILITY_H
#include <string>
#include <unordered_map>

namespace ray {
namespace streaming {

class StreamingUtility {
 public:
  static std::string Byte2hex(const uint8_t *data, uint32_t data_size);

  static std::string Hexqid2str(const std::string &q_id_hex);

  static std::string GetHostname();
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_UTILITY_H
