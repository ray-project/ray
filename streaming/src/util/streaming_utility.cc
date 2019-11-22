#include <boost/asio/ip/host_name.hpp>
#include <unordered_set>

#include "streaming_utility.h"
namespace ray {
namespace streaming {
std::string StreamingUtility::Byte2hex(const uint8_t *data, uint32_t data_size) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (uint32_t i = 0; i < data_size; i++) {
    unsigned short val = data[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

std::string StreamingUtility::Hexqid2str(const std::string &q_id_hex) {
  std::string result;
  for (uint32_t i = 0; i < q_id_hex.size(); i += 2) {
    std::string byte = q_id_hex.substr(i, 2);
    char chr = static_cast<char>(std::strtol(byte.c_str(), nullptr, 16));
    result.push_back(chr);
  }
  return result;
}

std::string StreamingUtility::GetHostname() { return boost::asio::ip::host_name(); }

}  // namespace streaming
}  // namespace ray
