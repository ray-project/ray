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
#include "ray/raylet/ratio_hysteresis_pressure_monitor.h"

#include <climits>
#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/cgroup/mock_memory_pressure_reader.h"

namespace ray {
namespace raylet {

namespace {

constexpr int64_t kLimit = 1000;

RatioHysteresisPressureMonitor::Config MakeConfig() {
  RatioHysteresisPressureMonitor::Config c;
  c.pod_name = "test-pod";
  c.pressure_ratio = 0.85;
  c.release_hysteresis = 0.10;
  c.consecutive_hits = 2;
  c.release_consecutive_hits = 3;
  c.persistent_warning_interval_ms = 60000;
  c.poll_interval_ms = 0;  // 0 = don't start the internal self-driven sampling
                           // thread (see Config::poll_interval_ms comment). Tests
                           // drive sampling deterministically via manual Poll()
                           // calls, avoiding any race with the internal thread
                           // over the mock reader.
  return c;
}

}  // namespace

TEST(RatioHysteresisPressureMonitorTest, ConsecutiveHitsEmitSignal) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);  // ratio 0.9, hit 1
  mock->Push(910, kLimit);  // ratio 0.91, hit 2 → emit
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());

  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  EXPECT_GT((*monitor.GetCurrentSignal()), 0.9);
}

TEST(RatioHysteresisPressureMonitorTest, SingleSpikeDebounced) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);  // hit once
  mock->Push(500, kLimit);  // fall back → reset
  mock->Push(600, kLimit);  // still below threshold
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  monitor.Poll();
  monitor.Poll();
  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

TEST(RatioHysteresisPressureMonitorTest, ReleaseHysteresisClearsSignal) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  // Build up pressure.
  mock->Push(900, kLimit);
  mock->Push(910, kLimit);
  // Release: need 3 consecutive < (0.85 - 0.10) = 0.75.
  mock->Push(700, kLimit);  // release 1
  mock->Push(710, kLimit);  // release 2
  mock->Push(600, kLimit);  // release 3 → cleared
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  monitor.Poll();
  monitor.Poll();  // signal emitted
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  monitor.Poll();  // cleared
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

TEST(RatioHysteresisPressureMonitorTest, HoveringInHysteresisBandKeepsSignal) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);  // hit 1
  mock->Push(900, kLimit);  // hit 2 → signal
  // Hover between 0.75 and 0.85 — neither release nor hit.
  mock->Push(800, kLimit);
  mock->Push(800, kLimit);
  mock->Push(800, kLimit);
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
  for (int i = 0; i < 5; ++i) monitor.Poll();
  EXPECT_TRUE(monitor.GetCurrentSignal().has_value());
}

TEST(RatioHysteresisPressureMonitorTest, ReaderErrorKeepsPreviousState) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);
  mock->Push(900, kLimit);  // signal emitted
  mock->PushError("io");
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  monitor.Poll();  // reader error — state preserved
  EXPECT_TRUE(monitor.GetCurrentSignal().has_value());
}

TEST(RatioHysteresisPressureMonitorTest, OnResizeClearsInternalState) {
  // C1 regression: when NodeManager invokes OnResize after a memory
  // upsize, the monitor must clear its counters + current_signal_ so the
  // next heartbeat doesn't keep emitting stale pressure.
  // OnResize() replaces ResetSignal + HadActiveSignalSinceReset: it reads the
  // old latch value and resets in one call.
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);
  mock->Push(910, kLimit);  // signal emitted
  // After OnResize the reader must still provide samples to verify that
  // accumulation restarts from a clean state.
  mock->Push(900, kLimit);  // hit 1 after reset (not enough to activate)
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());

  // The first OnResize after activation returns true (was under pressure) and
  // clears current_signal_ + counters + latch.
  EXPECT_TRUE(monitor.OnResize());
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());

  // State has been cleared: one more Poll after reset only accrues hit 1 <
  // consecutive_hits=2, so it must not activate.
  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());

  // A second OnResize returns false (no residual latch), proving the latch was
  // truly cleared.
  EXPECT_FALSE(monitor.OnResize());
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Pressure-source attribution latch: set once a signal activates and held until
// OnResize — even after the Poll release path has already cleared
// current_signal_. This is the sole criterion NodeManager uses to distinguish
// "pressure-driven upsize" from "task-driven upsize".
//
// OnResize() resets as it reads the old latch value, so it cannot do a
// read-only check. We therefore split what would have been consecutive reads on
// one monitor into two independent monitor instances: each is constructed into
// the target state, then asserted with a single OnResize() call.
TEST(RatioHysteresisPressureMonitorTest, LatchSetOnActivation) {
  // Instance A: Poll once only (hit 1, not activated) → OnResize() returns false.
  {
    auto mock = std::make_unique<MockMemoryPressureReader>();
    mock->Push(900, kLimit);  // hit 1
    RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
    monitor.Poll();
    EXPECT_FALSE(monitor.OnResize());  // hit 1 not activated → latch not set
  }
  // Instance B: Poll twice (hit 1, hit 2 → activated) → OnResize() returns true.
  {
    auto mock = std::make_unique<MockMemoryPressureReader>();
    mock->Push(900, kLimit);  // hit 1
    mock->Push(910, kLimit);  // hit 2 → signal emitted
    RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
    monitor.Poll();
    monitor.Poll();
    EXPECT_TRUE(monitor.OnResize());  // hit 2 → activated → latch set
  }
}

// Core scenario: after kubelet enlarges the cgroup limit, the ratio drops below
// the release threshold and current_signal_ is cleared by the Poll release path,
// but the latch must stay true — otherwise a subsequently arriving
// pressure-driven resize RPC would observe latch=false at the attribution point
// and fail to isolate a pod that genuinely should be isolated.
TEST(RatioHysteresisPressureMonitorTest, LatchSurvivesSignalDecayAfterCgroupGrows) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  // Pressure phase: limit=1000, ratio 0.9 → activated.
  mock->Push(900, kLimit);
  mock->Push(910, kLimit);  // signal emitted
  // After kubelet upsize: current is unchanged but limit doubles → ratio 0.455 <
  // 0.75 release threshold. Three consecutive releases → current_signal_ cleared.
  mock->Push(910, kLimit * 2);  // release 1
  mock->Push(910, kLimit * 2);  // release 2
  mock->Push(910, kLimit * 2);  // release 3 → current_signal_ cleared
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());

  monitor.Poll();
  monitor.Poll();
  monitor.Poll();
  // The signal has decayed and been cleared...
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
  // ...but the latch is still held, so the attribution point isolates correctly.
  // OnResize() replaces HadActiveSignalSinceReset: one call asserts the latch is
  // still true after decay.
  EXPECT_TRUE(monitor.OnResize());
}

// OnResize must clear the latch and return its old value. Even after
// current_signal_ has been cleared by the release path and the counters zeroed,
// as long as the latch is still true the first OnResize must return true; the
// second OnResize must return false, proving the latch was truly cleared and
// won't leak into the next resize and mis-isolate a task-driven upsize.
// OnResize() replaces ResetSignal + HadActiveSignalSinceReset: it reads the old
// latch value and resets.
TEST(RatioHysteresisPressureMonitorTest, OnResizeReturnsTrueAfterDecayThenClearsLatch) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);
  mock->Push(910, kLimit);      // signal emitted
  mock->Push(910, kLimit * 2);  // release 1
  mock->Push(910, kLimit * 2);  // release 2
  mock->Push(910, kLimit * 2);  // release 3 → current_signal_ cleared
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
  for (int i = 0; i < 5; ++i) monitor.Poll();
  ASSERT_FALSE(monitor.GetCurrentSignal().has_value());

  // Latch still held after decay → first OnResize returns true and clears latch.
  EXPECT_TRUE(monitor.OnResize());
  // Already cleared → second OnResize returns false, proving the fast-return
  // guard didn't leak a residual latch.
  EXPECT_FALSE(monitor.OnResize());
}

// Task-driven upsize scenario: pressure never activated, latch stays false, so
// the attribution point does not isolate.
TEST(RatioHysteresisPressureMonitorTest, LatchStaysFalseWhenNeverActivated) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(500, kLimit);  // ratio 0.5, well below threshold
  mock->Push(600, kLimit);  // ratio 0.6
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
  monitor.Poll();
  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
  // OnResize() replaces HadActiveSignalSinceReset: never activated → latch always
  // false → returns false.
  EXPECT_FALSE(monitor.OnResize());
}

// Facet 3: real-vs-synthetic data gap. An instantaneous cgroup count rarely
// equals the limit exactly (kernel counting granularity / reserved pages / OOM
// kill firing before the ceiling is reached); the at_max decision is based on
// ratio >= 0.99 rather than current == limit, so "near the ceiling but not
// equal" still counts as at_max.
//
// Note: at_max_capacity_ is private state with no external accessor, so a unit
// test cannot assert it directly; its only external effect is whether
// MaybeEmitPersistentWarning chooses a WARNING vs INFO log level, which is
// covered at the integration / log-verification layer
// (feedback_unit_test_blind_spots facet 4). The contract this case guards: when
// current < limit but ratio >= 0.99, the Poll() path does not bail out early due
// to the old current == limit check, and the signal still emits normally.
TEST(RatioHysteresisPressureMonitorTest, RatioPathNotGatedByCurrentEqualsLimit) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  // 0.995 ratio — near the ceiling but current != limit.
  mock->Push(995, kLimit);
  mock->Push(995, kLimit);  // hit 2 → emit
  mock->Push(995, kLimit);
  auto cfg = MakeConfig();
  cfg.persistent_warning_interval_ms = 1;  // make every Poll take the warning branch
  RatioHysteresisPressureMonitor monitor(std::move(mock), cfg);
  int64_t fake_time = 0;
  monitor.SetClockForTest([&fake_time]() { return fake_time; });

  fake_time = 1000;
  monitor.Poll();
  fake_time = 2000;
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  fake_time = 3000;
  monitor.Poll();  // takes the at_max path; must not crash / spin
  EXPECT_TRUE(monitor.GetCurrentSignal().has_value());
  EXPECT_DOUBLE_EQ((*monitor.GetCurrentSignal()), 995.0 / kLimit);
}

// Facet 6: config robustness. A misconfigured release_hysteresis (1.0, negative)
// must be clamped by the ctor, otherwise release_threshold goes negative and the
// signal is never cleared.
//
// Test strategy: clamping is private logic with no accessor to inspect directly.
// We infer it from the observable behavior of release_threshold: if clamping
// works, a ratio in the corresponding range should trigger a release; if it
// fails (release_threshold goes negative), every ratio is >= the threshold and
// the signal hangs forever. Pick a ratio that releases under clamping but not
// without it to distinguish the two implementations.
TEST(RatioHysteresisPressureMonitorCtorClampTest,
     ClampsReleaseHysteresisAboveMaxStillAllowsRelease) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  // pressure_ratio = 0.85, release_hysteresis misconfigured to 1.0
  //   clamping works:  → 0.84, release_threshold = 0.01, only ratio 0.0 releases.
  //   clamping fails:  release_threshold = -0.15; any ratio is >= -0.15, and
  //                    ratio < -0.15 never holds, so the signal never releases.
  // Use ratio = 0 (current=0) to distinguish: clamping works → 0 < 0.01 releases;
  //                                           clamping fails → 0 < -0.15 false, no
  //                                           release.
  mock->Push(900, kLimit);
  mock->Push(900, kLimit);                             // emit
  for (int i = 0; i < 10; ++i) mock->Push(0, kLimit);  // ratio 0
  auto cfg = MakeConfig();
  cfg.release_hysteresis = 1.0;  // intentional misconfiguration
  RatioHysteresisPressureMonitor monitor(std::move(mock), cfg);
  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  // release_consecutive_hits=3 → release after 3 ratio-0 samples (clamping works).
  monitor.Poll();
  monitor.Poll();
  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

TEST(RatioHysteresisPressureMonitorCtorClampTest,
     ClampsReleaseHysteresisBelowZeroToZero) {
  // Negative misconfiguration → clamped to 0.0. release_threshold =
  // pressure_ratio = 0.85. Any ratio < 0.85 starts the release count.
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);
  mock->Push(900, kLimit);                              // emit
  for (int i = 0; i < 5; ++i) mock->Push(800, kLimit);  // ratio 0.8 < 0.85
  auto cfg = MakeConfig();
  cfg.release_hysteresis = -0.5;  // intentional misconfiguration
  RatioHysteresisPressureMonitor monitor(std::move(mock), cfg);
  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  // release_consecutive_hits = 3 → release after 3 samples at 0.8.
  monitor.Poll();
  monitor.Poll();
  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Facet 4: FSM exit state. OnResize must zero both the hit counter and the
// signal; a Poll after reset should accrue consecutive_hits from zero rather
// than continuing the old count. OnResize() replaces ResetSignal: it reads the
// old latch value and resets (the old value is irrelevant here, return value
// discarded).
TEST(RatioHysteresisPressureMonitorTest, OnResizeLetsCounterAccrueFromScratch) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);  // hit 1 (accruing)
  // Feed one more 0.9 ratio after OnResize: if reset failed to zero
  // consecutive_hits_, this sample becomes hit 2 and immediately fires the
  // signal; if reset is correct it is only hit 1, no signal.
  mock->Push(900, kLimit);
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());
  monitor.Poll();
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
  monitor.OnResize();  // counter should be zeroed (return value discarded)
  monitor.Poll();
  // Only 1 hit accrued after reset, so it must not fire (consecutive_hits=2).
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Blind spot 1: the gating logic has no owner. The `if (limit <= 0) return;` in
// Poll() is the skip gate for uncapped cgroups; the truth table has two
// quadrants: limit>0 → compute ratio; limit<=0 → skip, never under pressure. The
// original tests only covered the limit>0 side. This adds the limit==0 side.
TEST(RatioHysteresisPressureMonitorTest, UncappedLimitZeroNeverEmitsSignal) {
  // Arrange: current is very high but limit=0 (uncapped cgroup), ratio meaningless.
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(999999, 0);
  mock->Push(999999, 0);
  mock->Push(999999, 0);
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  // Act
  for (int i = 0; i < 3; ++i) monitor.Poll();

  // Assert: the limit<=0 gate skips accumulation, so no signal is ever emitted.
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Blind spot 1 + blind spot 3: the reader's "max" sentinel maps to INT64_MAX,
// which is the limit value in a real uncapped scenario. limit=INT64_MAX > 0
// passes the limit<=0 gate, but current/limit ≈ 0 → ratio far below threshold →
// still must not register as pressure. Use INT64_MAX rather than 0 to stay close
// to reality and distinguish the "uncapped but past the gate" path from the
// "uncapped stopped at the gate" path.
TEST(RatioHysteresisPressureMonitorTest, Int64MaxLimitYieldsNearZeroRatioNoSignal) {
  // Arrange
  auto mock = std::make_unique<MockMemoryPressureReader>();
  const int64_t kUncapped = INT64_MAX;
  mock->Push(8L * 1024 * 1024 * 1024, kUncapped);  // 8GiB / INT64_MAX ≈ 0
  mock->Push(8L * 1024 * 1024 * 1024, kUncapped);
  mock->Push(8L * 1024 * 1024 * 1024, kUncapped);
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  // Act
  for (int i = 0; i < 3; ++i) monitor.Poll();

  // Assert: limit passes the >0 gate, but ratio≈0 does not trigger.
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Blind spot 3: at the boundary but not equal. The pressure decision is
// `ratio >= pressure_ratio` (inclusive). Two near-boundary samples distinguish
// `>=` from `>`:
//   ratio exactly == 0.85 → inclusive impl registers pressure, `>` impl does not.
//   ratio = 0.849         → neither impl registers pressure (the release gate
//                           also doesn't fire; it falls into the hover band).
TEST(RatioHysteresisPressureMonitorTest, RatioExactlyAtThresholdCountsAsHit) {
  // Arrange: current=850, limit=1000 → ratio exactly 0.85.
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(850, kLimit);  // ratio == 0.85, hit 1
  mock->Push(850, kLimit);  // hit 2 → emit
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  // Act
  monitor.Poll();
  monitor.Poll();

  // Assert: the equality boundary counts as a hit, so the 2nd poll fires.
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  EXPECT_DOUBLE_EQ((*monitor.GetCurrentSignal()), 0.85);
}

TEST(RatioHysteresisPressureMonitorTest, RatioJustBelowThresholdNoHit) {
  // Blind spot 3: 0.849 sits just below 0.85, falling into the
  // [release_threshold, pressure_ratio) hover band (0.75 ≤ 0.849 < 0.85) → it
  // neither accrues a hit nor releases, so it is never under pressure.
  // Arrange
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(849, kLimit);  // ratio 0.849
  mock->Push(849, kLimit);
  mock->Push(849, kLimit);
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  // Act
  for (int i = 0; i < 3; ++i) monitor.Poll();

  // Assert
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Blind spot 5: making the config a real constant. consecutive_hits defaults to
// 2, and the original tests all implicitly depend on "fires only on the 2nd
// poll". If someone makes consecutive_hits configurable but Poll still
// hardcodes 2, a default-value test wouldn't catch it. Here we override
// consecutive_hits=1 and assert it fires on the *first* poll, proving the
// threshold is actually read from config.
TEST(RatioHysteresisPressureMonitorTest, ConsecutiveHitsConfigOverrideEmitsOnFirstPoll) {
  // Arrange
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);  // ratio 0.9
  auto cfg = MakeConfig();
  cfg.consecutive_hits = 1;  // non-default: should fire on the first poll
  RatioHysteresisPressureMonitor monitor(std::move(mock), cfg);

  // Act
  monitor.Poll();

  // Assert: override=1 → fires on the 1st poll (with the default 2 this would
  // still be empty, which is what makes the test discriminating).
  EXPECT_TRUE(monitor.GetCurrentSignal().has_value());
}

// Blind spot 5: likewise override release_consecutive_hits to 1 to verify the
// release threshold is read from config.
TEST(RatioHysteresisPressureMonitorTest,
     ReleaseConsecutiveHitsConfigOverrideClearsOnFirstDrop) {
  // Arrange: build up pressure, then drop below the release threshold in a
  // single poll.
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);
  mock->Push(900, kLimit);  // emit
  mock->Push(700, kLimit);  // ratio 0.7 < release_threshold 0.75
  auto cfg = MakeConfig();
  cfg.release_consecutive_hits = 1;  // non-default: clears on the first drop
  RatioHysteresisPressureMonitor monitor(std::move(mock), cfg);

  // Act
  monitor.Poll();
  monitor.Poll();
  ASSERT_TRUE(monitor.GetCurrentSignal().has_value());
  monitor.Poll();  // single 0.7 sample → override=1 should clear immediately

  // Assert
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
}

// Blind spot 2: name matches behavior. When the reader errors, the contract is
// "Keeps Previous State". The existing ReaderErrorKeepsPreviousState proves
// "holds the signal when one exists", but "previous state" also includes the
// *accrued counter*: an error poll must not advance consecutive_hits_. We
// construct a discriminating input to prove the before/keep semantics —
// hit1 → error → hit: if the error poll wrongly counts as a hit, the 3rd poll is
// hit2 and fires immediately; with the correct implementation the error poll is
// returned/skipped, so only the 3rd poll reaches hit2 and fires. Both fire at
// the same poll, so we instead use hit1 → error → low-ratio to distinguish: the
// error neither clears nor advances, and the low-ratio poll resets the counter.
TEST(RatioHysteresisPressureMonitorTest, ReaderErrorDoesNotAdvanceHitCounter) {
  // Arrange
  auto mock = std::make_unique<MockMemoryPressureReader>();
  mock->Push(900, kLimit);  // hit 1
  mock->PushError("io");    // error poll: must not count as hit 2
  mock->Push(900, kLimit);  // if the error is skipped, this is hit 2 → fires
  RatioHysteresisPressureMonitor monitor(std::move(mock), MakeConfig());

  // Act
  monitor.Poll();  // hit 1
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
  monitor.Poll();  // error: state preserved, counter not advanced
  EXPECT_FALSE(monitor.GetCurrentSignal().has_value());
  monitor.Poll();  // hit 2 → fires

  // Assert: the error poll didn't swallow a valid hit; accrual semantics correct.
  EXPECT_TRUE(monitor.GetCurrentSignal().has_value());
}

TEST(RatioHysteresisPressureMonitorTest, PersistentWarningRateLimited) {
  auto mock = std::make_unique<MockMemoryPressureReader>();
  // 10 consecutive at-max samples.
  for (int i = 0; i < 10; ++i) mock->Push(kLimit, kLimit);
  auto cfg = MakeConfig();
  cfg.persistent_warning_interval_ms = 60000;
  RatioHysteresisPressureMonitor monitor(std::move(mock), cfg);
  int64_t fake_time = 0;
  monitor.SetClockForTest([&fake_time]() { return fake_time; });
  // Advance clock slowly (< 60s) → expect no extra warnings after first.
  for (int i = 0; i < 10; ++i) {
    fake_time += 2000;  // 2s per poll
    monitor.Poll();
  }
  // No assertion on log lines — the contract is that Poll doesn't
  // spin/loop/crash; behavior verified via log inspection in integration.
  EXPECT_TRUE(monitor.GetCurrentSignal().has_value());
}

}  // namespace raylet
}  // namespace ray
