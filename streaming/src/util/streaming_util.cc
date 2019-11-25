#include <unordered_set>

#include "streaming_util.h"
namespace ray {
namespace streaming {

boost::any &Config::Get(ConfigEnum key) const {
  auto item = config_map_.find(key);
  STREAMING_CHECK(item != config_map_.end());
  return item->second;
}

boost::any Config::Get(ConfigEnum key, boost::any default_value) const {
  auto item = config_map_.find(key);
  if (item == config_map_.end()) {
    return default_value;
  }
  return item->second;
}

std::string Util::Byte2hex(const uint8_t *data, uint32_t data_size) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (uint32_t i = 0; i < data_size; i++) {
    unsigned short val = data[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

std::string Util::Hexqid2str(const std::string &q_id_hex) {
  std::string result;
  for (uint32_t i = 0; i < q_id_hex.size(); i += 2) {
    std::string byte = q_id_hex.substr(i, 2);
    char chr = static_cast<char>(std::strtol(byte.c_str(), nullptr, 16));
    result.push_back(chr);
  }
  return result;
}
}  // namespace streaming
}  // namespace ray
