#include "ray/common/ray_config.h"

RayConfig &RayConfig::instance() {
  static RayConfig config;
  return config;
}