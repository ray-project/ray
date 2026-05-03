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

#include "ray/common/asio/io_context_monitor.h"

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
#include "ray/observability/fake_metric.h"

namespace ray {
namespace {

class IOContextMonitorTest : public ::testing::Test {
 protected:
  IOContextMonitor MakeMonitor(
      std::string name,
      std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
      absl::Duration deadline = absl::Seconds(5)) {
    return IOContextMonitor(std::move(name),
                            std::move(io_contexts),
                            lag_gauge_,
                            deadline_counter_,
                            deadline,
                            clock_);
  }

  double GetLag(const std::string &ctx_name) {
    for (const auto &[tags, value] : lag_gauge_.GetTagToValue()) {
      auto it = tags.find("Name");
      if (it != tags.end() && it->second == ctx_name) {
        return value;
      }
    }
    return -1;
  }

  double GetDeadlineExceeded(const std::string &ctx_name) {
    for (const auto &[tags, value] : deadline_counter_.GetTagToValue()) {
      auto it = tags.find("Name");
      if (it != tags.end() && it->second == ctx_name) {
        return value;
      }
    }
    return 0;
  }

  std::shared_ptr<FakeClock> clock_ = std::make_shared<FakeClock>();
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
  auto monitor = MakeMonitor("test", {{"stuck", &stuck_ctx}}, absl::Milliseconds(100));

  EXPECT_TRUE(monitor.Tick());
  clock_->AdvanceTime(absl::Milliseconds(200));
  EXPECT_FALSE(monitor.Tick());
  EXPECT_EQ(GetDeadlineExceeded("stuck"), 1);

  // Additional ticks should not increment the counter again (already unhealthy).
  clock_->AdvanceTime(absl::Milliseconds(200));
  monitor.Tick();
  EXPECT_EQ(GetDeadlineExceeded("stuck"), 1);
}

TEST_F(IOContextMonitorTest, HealthyWithinDeadline) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}}, absl::Milliseconds(100));

  monitor.Tick();
  clock_->AdvanceTime(absl::Milliseconds(50));
  EXPECT_TRUE(monitor.Tick());
}

TEST_F(IOContextMonitorTest, LagNotRecordedWhileOutstanding) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}});

  monitor.Tick();
  EXPECT_EQ(GetLag("ctx"), -1);

  monitor.Tick();
  EXPECT_EQ(GetLag("ctx"), -1);

  ctx.poll();
  monitor.Tick();
  EXPECT_GE(GetLag("ctx"), 0);
}

TEST_F(IOContextMonitorTest, MultipleIOContexts) {
  instrumented_io_context healthy_ctx;
  instrumented_io_context stuck_ctx;

  auto monitor = MakeMonitor("test",
                             {{"healthy", &healthy_ctx}, {"stuck", &stuck_ctx}},
                             absl::Milliseconds(100));

  monitor.Tick();
  healthy_ctx.poll();
  clock_->AdvanceTime(absl::Milliseconds(200));

  EXPECT_FALSE(monitor.Tick());
  EXPECT_GE(GetLag("healthy"), 0);
  EXPECT_EQ(GetDeadlineExceeded("stuck"), 1);

  // Unblock the stuck context.
  stuck_ctx.poll();
  // Tick sees the late completion — still unhealthy, but posts a fresh probe.
  EXPECT_FALSE(monitor.Tick());

  // The fresh probe completes within deadline → back to healthy.
  stuck_ctx.restart();
  stuck_ctx.poll();
  EXPECT_TRUE(monitor.Tick());
}

TEST_F(IOContextMonitorTest, CompletionPastDeadlineMarksUnhealthy) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}}, absl::Milliseconds(100));

  // Tick 1 posts the probe at t=0.
  EXPECT_TRUE(monitor.Tick());

  // Advance past the deadline before the probe runs, so when it completes its
  // lag exceeds the deadline.
  clock_->AdvanceTime(absl::Milliseconds(200));
  ctx.poll();

  // Tick 2 observes a completed probe whose lag (200ms) is past the deadline.
  EXPECT_FALSE(monitor.Tick());
  EXPECT_EQ(GetDeadlineExceeded("ctx"), 1);
  EXPECT_GE(GetLag("ctx"), 200);
}

TEST_F(IOContextMonitorTest, LateCompletionDoesNotRestoreHealth) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}}, absl::Milliseconds(100));

  monitor.Tick();
  clock_->AdvanceTime(absl::Milliseconds(200));
  EXPECT_FALSE(monitor.Tick());

  ctx.poll();
  EXPECT_FALSE(monitor.Tick());

  ctx.restart();
  ctx.poll();
  EXPECT_TRUE(monitor.Tick());
}

TEST_F(IOContextMonitorTest, DoesNotAccumulateProbesOnStuckContext) {
  instrumented_io_context stuck_ctx;
  auto monitor = MakeMonitor("test", {{"stuck", &stuck_ctx}}, absl::Milliseconds(100));

  monitor.Tick();
  clock_->AdvanceTime(absl::Milliseconds(200));
  monitor.Tick();
  clock_->AdvanceTime(absl::Milliseconds(200));
  monitor.Tick();

  auto handlers_run = stuck_ctx.poll();
  EXPECT_EQ(handlers_run, 1);
}

TEST(IOContextMonitorThreadTest, CallbackAndShutdown) {
  InstrumentedIOContextWithThread ctx("test_ctx");
  observability::FakeGauge lag_gauge;
  observability::FakeCounter deadline_counter;

  auto monitor = std::make_unique<IOContextMonitor>(
      "test",
      std::vector<std::pair<std::string, instrumented_io_context *>>{
          {"test_ctx", &ctx.GetIoService()}},
      lag_gauge,
      deadline_counter,
      absl::Seconds(5));

  std::atomic<int> callback_count{0};
  IOContextMonitorThread thread(
      std::move(monitor), absl::Milliseconds(50), [&callback_count](bool healthy) {
        callback_count.fetch_add(1);
      });

  thread.Start();
  while (callback_count.load() < 2) {
    absl::SleepFor(absl::Milliseconds(1));
  }
  thread.Stop();

  ctx.Stop();
}

}  // namespace
}  // namespace ray
