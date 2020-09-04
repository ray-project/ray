#pragma once
#include <boost/any.hpp>
#include <unordered_map>

#include "streaming_logging.h"

namespace ray {
namespace streaming {
enum class ConfigEnum : uint32_t {
  QUEUE_ID_VECTOR = 0,
  MIN = QUEUE_ID_VECTOR,
  MAX = QUEUE_ID_VECTOR
};
}
}  // namespace ray

namespace std {
template <>
struct hash<::ray::streaming::ConfigEnum> {
  size_t operator()(const ::ray::streaming::ConfigEnum &config_enum_key) const {
    return static_cast<uint32_t>(config_enum_key);
  }
};

template <>
struct hash<const ::ray::streaming::ConfigEnum> {
  size_t operator()(const ::ray::streaming::ConfigEnum &config_enum_key) const {
    return static_cast<uint32_t>(config_enum_key);
  }
};
}  // namespace std

namespace ray {
namespace streaming {

class Config {
 public:
  template <typename ValueType>
  inline void Set(ConfigEnum key, const ValueType &any) {
    config_map_.emplace(key, any);
  }

  template <typename ValueType>
  inline void Set(ConfigEnum key, ValueType &&any) {
    config_map_.emplace(key, any);
  }

  template <typename ValueType>
  inline boost::any &GetOrDefault(ConfigEnum key, ValueType &&any) {
    auto item = config_map_.find(key);
    if (item != config_map_.end()) {
      return item->second;
    }
    Set(key, any);
    return any;
  }

  boost::any &Get(ConfigEnum key) const;
  boost::any Get(ConfigEnum key, boost::any default_value) const;

  inline uint32_t GetInt32(ConfigEnum key) { return boost::any_cast<uint32_t>(Get(key)); }

  inline uint64_t GetInt64(ConfigEnum key) { return boost::any_cast<uint64_t>(Get(key)); }

  inline double GetDouble(ConfigEnum key) { return boost::any_cast<double>(Get(key)); }

  inline bool GetBool(ConfigEnum key) { return boost::any_cast<bool>(Get(key)); }

  inline std::string GetString(ConfigEnum key) {
    return boost::any_cast<std::string>(Get(key));
  }

  virtual ~Config() = default;

 protected:
  mutable std::unordered_map<ConfigEnum, boost::any> config_map_;
};

}  // namespace streaming
}  // namespace ray
