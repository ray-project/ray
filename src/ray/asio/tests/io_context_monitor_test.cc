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

#include "ray/asio/io_context_monitor.h"

#include "gtest/gtest.h"
#include "ray/asio/asio_util.h"
#include "ray/observability/fake_metric.h"

namespace ray {
namespace {

class IOContextMonitorTest : public ::testing::Test {
 protected:
  IOContextMonitor MakeMonitor(
      std::string name,
      std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
      absl::Duration deadline = absl::Seconds(5),
      absl::Duration latency_window = absl::Seconds(30)) {
    return IOContextMonitor(std::move(name),
                            std::move(io_contexts),
                            latency_gauge_,
                            unhealthy_counter_,
                            deadline,
                            latency_window,
                            clock_);
  }

  double GetLatency(const std::string &ctx_name) {
    for (const auto &[tags, value] : latency_gauge_.GetTagToValue()) {
      auto it = tags.find("Name");
      if (it != tags.end() && it->second == ctx_name) {
        return value;
      }
    }
    return -1;
  }

  // Accumulated count of deadline misses for the given io_context. Returns 0 if the
  // context has never missed the deadline (the counter was never recorded to).
  double GetUnhealthyCount(const std::string &ctx_name) {
    for (const auto &[tags, value] : unhealthy_counter_.GetTagToValue()) {
      auto it = tags.find("Name");
      if (it != tags.end() && it->second == ctx_name) {
        return value;
      }
    }
    return 0;
  }

  std::shared_ptr<FakeClock> clock_ = std::make_shared<FakeClock>();
  observability::FakeGauge latency_gauge_;
  observability::FakeCounter unhealthy_counter_;
};

TEST_F(IOContextMonitorTest, ProbeSucceeds) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}});

  monitor.Tick();
  ctx.poll();
  EXPECT_TRUE(monitor.Tick());
  EXPECT_GE(GetLatency("ctx"), 0);
}

TEST_F(IOContextMonitorTest, DetectsStuckIOContext) {
  instrumented_io_context stuck_ctx;
  auto monitor = MakeMonitor("test", {{"stuck", &stuck_ctx}}, absl::Milliseconds(100));

  EXPECT_TRUE(monitor.Tick());
  EXPECT_EQ(GetUnhealthyCount("stuck"), 0);
  clock_->AdvanceTime(absl::Milliseconds(200));
  EXPECT_FALSE(monitor.Tick());
  EXPECT_EQ(GetUnhealthyCount("stuck"), 1);

  // The same outstanding probe should not be counted again on subsequent ticks.
  clock_->AdvanceTime(absl::Milliseconds(200));
  monitor.Tick();
  EXPECT_EQ(GetUnhealthyCount("stuck"), 1);
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
  EXPECT_EQ(GetLatency("ctx"), -1);

  monitor.Tick();
  EXPECT_EQ(GetLatency("ctx"), -1);

  ctx.poll();
  monitor.Tick();
  EXPECT_GE(GetLatency("ctx"), 0);
}

TEST_F(IOContextMonitorTest, ExportsWindowedMaxLatency) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}});

  // First probe takes 100ms; the windowed max is exported.
  monitor.Tick();  // Post the probe at t0.
  clock_->AdvanceTime(absl::Milliseconds(100));
  ctx.poll();      // Probe completes 100ms later.
  monitor.Tick();  // Observe completion: record max=100, post a fresh probe.
  EXPECT_EQ(GetLatency("ctx"), 100);

  // Second probe takes only 20ms. Since it is within the (default 30s) window, the
  // windowed max stays at 100 and the exported value does not drop to the latest
  // point-in-time latency.
  clock_->AdvanceTime(absl::Milliseconds(20));
  ctx.restart();
  ctx.poll();
  monitor.Tick();
  EXPECT_EQ(GetLatency("ctx"), 100);
}

TEST_F(IOContextMonitorTest, WindowedMaxDropsAfterEviction) {
  instrumented_io_context ctx;
  // Use a short window so the first probe's latency is evicted quickly.
  auto monitor =
      MakeMonitor("test", {{"ctx", &ctx}}, absl::Seconds(5), absl::Milliseconds(30));

  // First probe takes 100ms; recorded at t0+100ms.
  monitor.Tick();
  clock_->AdvanceTime(absl::Milliseconds(100));
  ctx.poll();
  monitor.Tick();
  EXPECT_EQ(GetLatency("ctx"), 100);

  // Second probe takes 50ms, recorded at t0+150ms. The first sample (at t0+100ms) is
  // now older than the 30ms window and is evicted, so the exported max drops to the
  // surviving sample's latency (50ms).
  clock_->AdvanceTime(absl::Milliseconds(50));
  ctx.restart();
  ctx.poll();
  monitor.Tick();
  EXPECT_EQ(GetLatency("ctx"), 50);
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
  EXPECT_GE(GetLatency("healthy"), 0);
  EXPECT_EQ(GetUnhealthyCount("healthy"), 0);
  EXPECT_EQ(GetUnhealthyCount("stuck"), 1);

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
  EXPECT_EQ(GetUnhealthyCount("ctx"), 1);
  EXPECT_GE(GetLatency("ctx"), 200);
}

TEST_F(IOContextMonitorTest, UnhealthyCountAccumulatesAcrossProbesAndDoesNotDip) {
  instrumented_io_context ctx;
  auto monitor = MakeMonitor("test", {{"ctx", &ctx}}, absl::Milliseconds(100));

  // Each cycle: a probe is posted, then completes past the deadline, so the next
  // Tick counts exactly one deadline miss and posts a fresh probe.
  auto miss_once = [&] {
    clock_->AdvanceTime(absl::Milliseconds(200));
    ctx.restart();
    ctx.poll();  // Complete the outstanding probe (late).
    EXPECT_FALSE(monitor.Tick());
  };

  // Tick 1 posts the first probe; nothing has missed yet.
  EXPECT_TRUE(monitor.Tick());
  EXPECT_EQ(GetUnhealthyCount("ctx"), 0);

  // Three consecutive probes each miss the deadline; the counter increments by 1
  // each time rather than re-counting the same probe.
  miss_once();
  EXPECT_EQ(GetUnhealthyCount("ctx"), 1);
  miss_once();
  EXPECT_EQ(GetUnhealthyCount("ctx"), 2);
  miss_once();
  EXPECT_EQ(GetUnhealthyCount("ctx"), 3);

  // The next probe completes within the deadline, so the io_context recovers. The
  // unhealthy counter must not dip when health is restored.
  clock_->AdvanceTime(absl::Milliseconds(50));
  ctx.restart();
  ctx.poll();
  EXPECT_TRUE(monitor.Tick());
  EXPECT_EQ(GetUnhealthyCount("ctx"), 3);
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
  observability::FakeGauge latency_gauge;
  observability::FakeCounter unhealthy_counter;

  auto monitor = std::make_unique<IOContextMonitor>(
      "test",
      std::vector<std::pair<std::string, instrumented_io_context *>>{
          {"test_ctx", &ctx.GetIoService()}},
      latency_gauge,
      unhealthy_counter,
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
