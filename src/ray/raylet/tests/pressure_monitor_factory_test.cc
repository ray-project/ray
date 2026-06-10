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

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/raylet/ratio_hysteresis_pressure_monitor.h"

namespace ray {
namespace raylet {

namespace {

// Set every field to a non-default value; otherwise the assertions cannot distinguish
// "read from env" from "hardcoded default". poll_interval_ms must be 0: the monitor
// returned by CreatePressureMonitor immediately starts its internal self-driven thread,
// and 0 disables that thread (see Config::poll_interval_ms), avoiding tests reading the
// real /sys/fs/cgroup.
constexpr char kNonDefaultConfig[] = R"({
  "memory_pressure_poll_interval_ms": 0,
  "memory_pressure_ratio": 0.77,
  "memory_pressure_release_hysteresis": 0.05,
  "memory_pressure_consecutive_hits": 5,
  "memory_pressure_release_consecutive_hits": 7,
  "memory_pressure_persistent_warning_interval_ms": 12345
})";

}  // namespace

// Scenario "factory returns the default impl" + "config reads centralized in the
// factory": CreatePressureMonitor returns a RatioHysteresisPressureMonitor through the
// interface, and each field of its Config is taken verbatim from the corresponding
// RAY_memory_pressure_* -- feed non-default values and assert pass-through, proving the
// config was read rather than hardcoded.
TEST(PressureMonitorFactoryTest, ReadsConfigFromRayConfigAndReturnsDefaultImpl) {
  RayConfig::instance().initialize(kNonDefaultConfig);
  ::setenv("POD_NAME", "factory-test-pod", /*overwrite=*/1);

  std::unique_ptr<MemoryPressureSignalMonitor> monitor = CreatePressureMonitor();
  ASSERT_NE(monitor, nullptr);

  auto *impl = dynamic_cast<RatioHysteresisPressureMonitor *>(monitor.get());
  ASSERT_NE(impl, nullptr)
      << "factory MUST return the sole implementation RatioHysteresisPressureMonitor";

  const RatioHysteresisPressureMonitor::Config &cfg = impl->GetConfigForTest();
  EXPECT_EQ(cfg.pod_name, "factory-test-pod");
  EXPECT_DOUBLE_EQ(cfg.pressure_ratio, 0.77);
  EXPECT_DOUBLE_EQ(cfg.release_hysteresis, 0.05);
  EXPECT_EQ(cfg.consecutive_hits, 5);
  EXPECT_EQ(cfg.release_consecutive_hits, 7);
  EXPECT_EQ(cfg.persistent_warning_interval_ms, 12345);
  EXPECT_EQ(cfg.poll_interval_ms, 0u);

  ::unsetenv("POD_NAME");
}

// POD_NAME unset: the factory MUST still return successfully, with pod_name falling back
// to an empty string (warn only, no crash).
TEST(PressureMonitorFactoryTest, MissingPodNameYieldsEmptyAndStillConstructs) {
  RayConfig::instance().initialize(kNonDefaultConfig);
  ::unsetenv("POD_NAME");

  std::unique_ptr<MemoryPressureSignalMonitor> monitor = CreatePressureMonitor();
  ASSERT_NE(monitor, nullptr);

  auto *impl = dynamic_cast<RatioHysteresisPressureMonitor *>(monitor.get());
  ASSERT_NE(impl, nullptr);
  EXPECT_EQ(impl->GetConfigForTest().pod_name, "");
}

}  // namespace raylet
}  // namespace ray
