
#include <ray/api/ray_config.h>

namespace ray {
namespace api {

std::shared_ptr<RayConfig> RayConfig::config_ = nullptr;

std::shared_ptr<RayConfig> RayConfig::GetInstance(std::string address, bool local_mode) {
  if (config_ == nullptr) {
    config_ = std::make_shared<RayConfig>();
    config_->redis_ip = address;
    if (!local_mode) {
      config_->run_mode = RunMode::SINGLE_PROCESS;
    }
  }
  return config_;
}
}  // namespace api
}  // namespace ray