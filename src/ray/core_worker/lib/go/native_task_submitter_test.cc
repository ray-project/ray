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

// Unit tests for the default CPU=1 behavior applied by the Go task submission
// bridge. The default is added so that an empty resource map matches Python's
// behavior of requesting one CPU, which keeps autoscaler compatibility (it
// triggers pending lease reporting).

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "ray/core_worker/lib/go/task_submitter_ops.h"

namespace ray {
namespace go {
namespace {

// ParseResources itself still returns an empty map for empty input; the
// default CPU=1 is added by EnsureDefaultCPU, called from BuildTaskOptions and
// BuildActorOptions. This keeps ParseResources backward compatible.
TEST(ParseResourcesDefaultCPUTest, ParseResourcesEmptyStillReturnsEmpty) {
  std::string emptyResources = "";
  auto result = TaskSubmitterOperations::ParseResources(emptyResources);
  EXPECT_TRUE(result.empty());
}

TEST(ParseResourcesDefaultCPUTest, ParseResourcesValidString) {
  std::string resources = "CPU:2.0,GPU:1.0,memory:1073741824.0";
  auto result = TaskSubmitterOperations::ParseResources(resources);

  EXPECT_EQ(result.size(), 3);
  EXPECT_NEAR(result["CPU"], 2.0, 0.001);
  EXPECT_NEAR(result["GPU"], 1.0, 0.001);
  EXPECT_NEAR(result["memory"], 1073741824.0, 0.001);
}

// An empty resource map (what the C ABI layer sees when the Go caller specifies
// no resources) gets the default CPU=1.
TEST(ParseResourcesDefaultCPUTest, DefaultCPUAddedForEmptyResources) {
  std::unordered_map<std::string, double> resources;
  TaskSubmitterOperations::EnsureDefaultCPU(resources);

  EXPECT_EQ(resources.size(), 1);
  EXPECT_NEAR(resources["CPU"], 1.0, 0.001);
}

// Explicitly specified resources are preserved: the default is only added when
// the map is empty, so a GPU-only task does not silently also request a CPU.
TEST(ParseResourcesDefaultCPUTest, NoDefaultCPUWhenResourcesSpecified) {
  auto resources = TaskSubmitterOperations::ParseResources("GPU:0.5,memory:536870912.0");
  TaskSubmitterOperations::EnsureDefaultCPU(resources);

  EXPECT_EQ(resources.size(), 2);
  EXPECT_NEAR(resources["GPU"], 0.5, 0.001);
  EXPECT_NEAR(resources["memory"], 536870912.0, 0.001);
  EXPECT_EQ(resources.count("CPU"), 0);
}

// An explicit CPU:0 makes the map non-empty, so the default is not applied and
// the user's override (e.g. for an IO-bound task) is honored.
TEST(ParseResourcesDefaultCPUTest, ExplicitCPUZeroIsHonored) {
  auto resources = TaskSubmitterOperations::ParseResources("CPU:0.0,memory:1073741824.0");
  TaskSubmitterOperations::EnsureDefaultCPU(resources);

  EXPECT_EQ(resources.size(), 2);
  EXPECT_NEAR(resources["CPU"], 0.0, 0.001);
  EXPECT_NEAR(resources["memory"], 1073741824.0, 0.001);
}

}  // namespace
}  // namespace go
}  // namespace ray
