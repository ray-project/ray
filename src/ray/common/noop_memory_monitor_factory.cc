// Copyright 2026 The Ray Authors.
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

#include <memory>

#include "ray/common/memory_monitor_factory.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/noop_memory_monitor.h"

namespace ray {

std::unique_ptr<MemoryMonitorInterface> MemoryMonitorFactory::Create(
    KillWorkersCallback _) {
  return std::make_unique<NoopMemoryMonitor>();
}

}  // namespace ray
