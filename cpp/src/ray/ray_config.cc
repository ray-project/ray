
#include <ray/api/ray_config.h>

namespace ray {
namespace api {

std::shared_ptr<RayConfig> RayConfig::config_ = nullptr;

std::shared_ptr<RayConfig> RayConfig::GetInstance() {
  if (config_ == nullptr) {
    config_ = std::make_shared<RayConfig>();
  }
  return config_;
}
}  // namespace api
}  // namespace ray