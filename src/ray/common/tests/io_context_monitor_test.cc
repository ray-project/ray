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

#include "ray/common/asio/io_context_monitor.h"

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
#include "ray/observability/fake_metric.h"

namespace ray {
namespace {

class IOContextMonitorTest : public ::testing::Test {
 protected:
  using TimePoint = std::chrono::steady_clock::time_point;

  void SetUp() override { fake_now_ = TimePoint(std::chrono::seconds(1000)); }

  void AdvanceTime(std::chrono::milliseconds duration) { fake_now_ += duration; }

  SteadyClock FakeClock() {
    return [this]() { return fake_now_; };
  }

  IOContextMonitor MakeMonitor(
      std::string name,
      std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
      std::chrono::milliseconds deadline = std::chrono::milliseconds{5000}) {
    return IOContextMonitor(std::move(name), std::move(io_contexts),
                            lag_gauge_, deadline_counter_,
                            deadline, FakeClock());
  }

  double GetLag(const std::string &ctx_name) {
    auto values = lag_gauge_.GetTagToValue();
    for (const auto &[tags, value] : values) {
      auto it = tags.find("Name");
      if (it != tags.end() && it->second == ctx_name) {
        return value;
      }
    }
    return -1;
  }

  double GetDeadlineExceeded(const std::string &ctx_name) {
    auto values = deadline_counter_.GetTagToValue();
    for (const auto &[tags, value] : values) {
      auto it = tags.find("Name");
      if (it != tags.end() && it->second == ctx_name) {
        return value;
      }
    }
    return 0;
  }

  TimePoint fake_now_;
  observability::FakeGauge lag_gauge_;
  observability::FakeCounter deadline_counter_;
};

TEST_F(IOContextMonitorTest, ProbeSucceeds) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}});

  monitor.Tick();
  ctx.poll();
  EXPECT_TRUE(monitor.Tick());
  EXPECT_GE(GetLag("ctx"), 0);
}

TEST_F(IOContextMonitorTest, DetectsStuckIOContext) {
  instrumented_io_context stuck_ctx;
  auto monitor = MakeMonitor("test", {{"stuck", &stuck_ctx}},
                             std::chrono::milliseconds(100));

  EXPECT_TRUE(monitor.Tick());
  AdvanceTime(std::chrono::milliseconds(200));
  EXPECT_FALSE(monitor.Tick());
  EXPECT_EQ(GetDeadlineExceeded("stuck"), 1);

  // Additional ticks should not increment the counter again (already unhealthy).
  AdvanceTime(std::chrono::milliseconds(200));
  monitor.Tick();
  EXPECT_EQ(GetDeadlineExceeded("stuck"), 1);
}

TEST_F(IOContextMonitorTest, HealthyWithinDeadline) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}},
                             std::chrono::milliseconds(100));

  monitor.Tick();
  AdvanceTime(std::chrono::milliseconds(50));
  EXPECT_TRUE(monitor.Tick());
}

TEST_F(IOContextMonitorTest, LagNotRecordedWhileOutstanding) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}});

  // First tick posts probe. No lag recorded yet.
  monitor.Tick();
  EXPECT_EQ(GetLag("ctx"), -1);

  // Tick again without draining — probe outstanding. Lag should still not be recorded.
  monitor.Tick();
  EXPECT_EQ(GetLag("ctx"), -1);

  // Drain and tick — now lag is recorded.
  ctx.poll();
  monitor.Tick();
  EXPECT_GE(GetLag("ctx"), 0);
}

TEST_F(IOContextMonitorTest, MultipleIOContexts) {
  instrumented_io_context healthy_ctx;
  instrumented_io_context stuck_ctx;

  auto monitor = MakeMonitor("test",
                             {{"healthy", &healthy_ctx}, {"stuck", &stuck_ctx}},
                             std::chrono::milliseconds(100));

  monitor.Tick();
  healthy_ctx.poll();
  AdvanceTime(std::chrono::milliseconds(200));

  EXPECT_FALSE(monitor.Tick());
  EXPECT_GE(GetLag("healthy"), 0);
  EXPECT_EQ(GetDeadlineExceeded("stuck"), 1);
}

TEST_F(IOContextMonitorTest, LateCompletionDoesNotRestoreHealth) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}},
                             std::chrono::milliseconds(100));

  // Post probe, go past deadline without draining.
  monitor.Tick();
  AdvanceTime(std::chrono::milliseconds(200));
  EXPECT_FALSE(monitor.Tick());

  // Drain — the outstanding probe completes, but it was past deadline.
  ctx.poll();
  // Tick sees the late completion. Should NOT restore health.
  EXPECT_FALSE(monitor.Tick());

  // The new probe completes within deadline → now healthy.
  ctx.restart();
  ctx.poll();
  EXPECT_TRUE(monitor.Tick());
}

TEST_F(IOContextMonitorTest, DoesNotAccumulateProbesOnStuckContext) {
  instrumented_io_context stuck_ctx;
  auto monitor = MakeMonitor("test", {{"stuck", &stuck_ctx}},
                             std::chrono::milliseconds(100));

  // Tick multiple times without draining.
  monitor.Tick();
  AdvanceTime(std::chrono::milliseconds(200));
  monitor.Tick();
  AdvanceTime(std::chrono::milliseconds(200));
  monitor.Tick();

  // Only one probe callback should have been posted (not 3).
  auto handlers_run = stuck_ctx.poll();
  EXPECT_EQ(handlers_run, 1);
}

}  // namespace
}  // namespace ray
