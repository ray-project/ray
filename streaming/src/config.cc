#include "config.h"
namespace ray {
namespace streaming {

boost::any &Config::Get(ConfigEnum key) const {
  auto item = config_map_.find(key);
  STREAMING_CHECK(item != config_map_.end());
  return item->second;
}

}  // namespace streaming
}  // namespace ray
