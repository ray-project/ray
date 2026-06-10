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
#pragma once

#include <memory>

#include "ray/raylet/memory_pressure_signal_monitor.h"

namespace ray {
namespace raylet {

// Centrally reads the RAY_memory_pressure_* environment variables and POD_NAME, assembles
// a Config, and constructs and returns the default implementation
// RatioHysteresisPressureMonitor. A free function rather than a factory class -- there is
// currently only one decision algorithm, and introducing strategy dispatch
// (RAY_memory_pressure_strategy) would be premature abstraction (YAGNI). When a second
// decision algorithm (e.g. PSI) is actually needed, upgrade this function into a factory
// with branching.
//
// The caller (NodeManager) only invokes this function when
// RAY_memory_pressure_monitor_enabled=true; the function itself does not check that
// switch.
std::unique_ptr<MemoryPressureSignalMonitor> CreatePressureMonitor();

}  // namespace raylet
}  // namespace ray
