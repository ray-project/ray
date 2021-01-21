
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

std::shared_ptr<RayConfig> RayConfig::GetInstance(std::string address, bool local_mode,
                                                  int min_workers, int max_workers) {
  if (config_ == nullptr) {
    config_ = std::make_shared<RayConfig>();
    config_->redis_ip = address;
    if (local_mode) {
      config_->run_mode = RunMode::SINGLE_PROCESS;
    }
  }
  return config_;
}
}  // namespace api
}  // namespace ray