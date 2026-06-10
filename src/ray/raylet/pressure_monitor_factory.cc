// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "ray/raylet/pressure_monitor_factory.h"

#include <cstdlib>
#include <memory>
#include <string>

#include "ray/common/cgroup/memory_pressure_reader.h"
#include "ray/common/ray_config.h"
#include "ray/raylet/ratio_hysteresis_pressure_monitor.h"
#include "ray/util/logging.h"

namespace ray {
namespace raylet {

std::unique_ptr<MemoryPressureSignalMonitor> CreatePressureMonitor() {
  RatioHysteresisPressureMonitor::Config cfg;

  const char *pod_env = std::getenv("POD_NAME");
  if (pod_env == nullptr || *pod_env == '\0') {
    RAY_LOG(WARNING)
        << "memory_pressure_monitor_enabled=true but POD_NAME env is unset; "
           "pressure signals will lack pod_name and may be hard to correlate "
           "with K8s pod events. Set POD_NAME via Downward API in non-KubeRay "
           "deployments.";
  }
  cfg.pod_name = pod_env == nullptr ? "" : pod_env;
  cfg.pressure_ratio = RayConfig::instance().memory_pressure_ratio();
  cfg.release_hysteresis = RayConfig::instance().memory_pressure_release_hysteresis();
  cfg.consecutive_hits = RayConfig::instance().memory_pressure_consecutive_hits();
  cfg.release_consecutive_hits =
      RayConfig::instance().memory_pressure_release_consecutive_hits();
  cfg.persistent_warning_interval_ms =
      RayConfig::instance().memory_pressure_persistent_warning_interval_ms();
  cfg.poll_interval_ms = RayConfig::instance().memory_pressure_poll_interval_ms();

  // RAY_IPPR_TEST_CGROUP_ROOT unset/empty -> production path (no-argument constructor
  // detects the real /sys/fs/cgroup); non-empty -> integration-test path (single-argument
  // constructor points at a fake cgroup tree, with the constructor detecting v2/v1
  // internally). Read the environment variable directly rather than via RAY_CONFIG, to
  // avoid keeping a test-only injection channel in the production config table; goes
  // through std::getenv like POD_NAME above.
  const char *cgroup_root_env = std::getenv("RAY_IPPR_TEST_CGROUP_ROOT");
  const std::string cgroup_root =
      cgroup_root_env == nullptr ? "" : cgroup_root_env;
  auto reader = cgroup_root.empty()
                    ? std::make_unique<FileMemoryPressureReader>()
                    : std::make_unique<FileMemoryPressureReader>(cgroup_root);

  RAY_LOG(INFO) << "CreatePressureMonitor: interval=" << cfg.poll_interval_ms
                << "ms pod=" << cfg.pod_name << " ratio=" << cfg.pressure_ratio;
  return std::make_unique<RatioHysteresisPressureMonitor>(std::move(reader),
                                                          std::move(cfg));
}

}  // namespace raylet
}  // namespace ray
