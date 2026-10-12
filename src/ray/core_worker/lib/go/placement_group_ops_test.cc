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

// Unit tests for placement_group_ops.cc - Placement group management layer.
// These tests use a GoogleMock ICoreWorkerOps (MockPGOps) to verify that
// PlacementGroupOperations forwards arguments to the seam and that the C
// boundary functions (ray_runtime_*) parse/encode correctly, without requiring
// a real CoreWorker.

#include "ray/core_worker/lib/go/placement_group_ops.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/go/core_worker_provider.h"

namespace ray {
namespace go {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

// ============================================================================
// Mock Placement Group Operations for Testing
// ============================================================================

/**
 * @brief GoogleMock fake of ICoreWorkerOps.
 *
 * Implements the full ICoreWorkerOps surface (SubmitTask / CreateActor /
 * SubmitActorTask / KillActor plus the three placement group methods) so that
 * the PlacementGroupOperations business layer can be tested against a
 * controllable double via PlacementGroupOperations::SetCoreWorkerOps.
 */
class MockPGOps : public ICoreWorkerOps {
 public:
  MOCK_METHOD(std::vector<ray::rpc::ObjectReference>,
              SubmitTask,
              (const ray::core::RayFunction &function,
               const std::vector<std::unique_ptr<ray::TaskArg>> &args,
               const ray::core::TaskOptions &task_options,
               int max_retries,
               bool retry_exceptions,
               const ray::rpc::SchedulingStrategy &scheduling_strategy,
               const std::string &debugger_breakpoint,
               const std::string &serialized_retry_exception_allowlist,
               const std::string &call_site,
               const ray::TaskID current_task_id),
              (override));

  MOCK_METHOD(ray::Status,
              CreateActor,
              (const ray::core::RayFunction &function,
               const std::vector<std::unique_ptr<ray::TaskArg>> &args,
               const ray::core::ActorCreationOptions &actor_creation_options,
               const std::string &extension_data,
               const std::string &call_site,
               ray::ActorID *actor_id),
              (override));

  MOCK_METHOD(ray::Status,
              SubmitActorTask,
              (const ray::ActorID &actor_id,
               const ray::core::RayFunction &function,
               const std::vector<std::unique_ptr<ray::TaskArg>> &args,
               const ray::core::TaskOptions &task_options,
               int max_retries,
               bool retry_exceptions,
               const std::string &serialized_retry_exception_allowlist,
               const std::string &call_site,
               std::vector<ray::rpc::ObjectReference> &task_returns,
               const ray::TaskID current_task_id),
              (override));

  MOCK_METHOD(ray::Status,
              KillActor,
              (const ray::ActorID &actor_id, bool force_kill, bool no_restart),
              (override));

  MOCK_METHOD(ray::Status,
              CreatePlacementGroup,
              (const ray::core::PlacementGroupCreationOptions &options,
               PlacementGroupID *placement_group_id),
              (override));

  MOCK_METHOD(ray::Status,
              RemovePlacementGroup,
              (const PlacementGroupID &placement_group_id),
              (override));

  MOCK_METHOD(ray::Status,
              WaitPlacementGroupReady,
              (const PlacementGroupID &placement_group_id, int64_t timeout_seconds),
              (override));
};

// ============================================================================
// Test Fixtures
// ============================================================================

class PlacementGroupOpsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_ = std::make_shared<MockPGOps>();
    PlacementGroupOperations::SetCoreWorkerOps(mock_);
  }

  void TearDown() override {
    // Restore the production CoreWorkerAdapter so tests never leak the mock
    // into other test binaries or subsequent tests.
    PlacementGroupOperations::SetCoreWorkerOps(nullptr);
    mock_.reset();
  }

  std::shared_ptr<MockPGOps> mock_;
};

namespace {

// A fixed, valid PlacementGroupID (18 bytes = 36 hex chars).
std::string TestPGHex() { return "0102030405060708090a0b0c0d0e0f101112"; }

}  // namespace

// ============================================================================
// C Boundary: CreatePlacementGroup Tests
// ============================================================================

TEST_F(PlacementGroupOpsTest, CreatePlacementGroupForwardsOptions) {
  // The C boundary must parse the bundles JSON, build a
  // PlacementGroupCreationOptions with the given name/strategy and forward it
  // to the seam, then encode the returned id as a hex string.
  EXPECT_CALL(*mock_, CreatePlacementGroup(_, _))
      .WillOnce(Invoke([](const ray::core::PlacementGroupCreationOptions &options,
                          PlacementGroupID *placement_group_id) -> ray::Status {
        EXPECT_EQ(options.name_, "pg-1");
        EXPECT_EQ(options.strategy_, ray::rpc::PACK);
        EXPECT_FALSE(options.is_detached_);
        EXPECT_EQ(options.bundles_.size(), 1u);
        if (options.bundles_.size() == 1u) {
          EXPECT_NEAR(options.bundles_[0].at("CPU"), 1.0, 1e-6);
          EXPECT_NEAR(options.bundles_[0].at("memory"), 128.0, 1e-6);
        }
        *placement_group_id = PlacementGroupID::FromHex(TestPGHex());
        return ray::Status::OK();
      }));

  char *error = nullptr;
  char *pg_id_hex = nullptr;
  int ret = ray_runtime_create_placement_group(
      /*name=*/"pg-1",
      /*bundles_json=*/R"([{"CPU":1,"memory":128}])",
      /*strategy=*/0,
      &pg_id_hex,
      &error);

  EXPECT_EQ(ret, 1);
  EXPECT_EQ(error, nullptr);
  ASSERT_NE(pg_id_hex, nullptr);
  EXPECT_EQ(std::string(pg_id_hex), TestPGHex());
  free(pg_id_hex);
  free(error);
}

TEST_F(PlacementGroupOpsTest, CreatePlacementGroupRejectsMalformedBundlesJson) {
  // Malformed bundles JSON must fail at the C boundary without reaching the
  // seam, returning 0 with an error string.
  EXPECT_CALL(*mock_, CreatePlacementGroup(_, _)).Times(0);

  char *error = nullptr;
  char *pg_id_hex = nullptr;
  int ret = ray_runtime_create_placement_group(
      /*name=*/"pg-1",
      /*bundles_json=*/"not-json",
      /*strategy=*/0,
      &pg_id_hex,
      &error);

  EXPECT_EQ(ret, 0);
  EXPECT_NE(error, nullptr);
  EXPECT_EQ(pg_id_hex, nullptr);
  free(pg_id_hex);
  free(error);
}

// ============================================================================
// C Boundary: RemovePlacementGroup Tests
// ============================================================================

TEST_F(PlacementGroupOpsTest, RemovePlacementGroupForwardsId) {
  // The C boundary must decode the hex id and forward it to the seam.
  EXPECT_CALL(*mock_, RemovePlacementGroup(PlacementGroupID::FromHex(TestPGHex())))
      .WillOnce(Return(ray::Status::OK()));

  char *error = nullptr;
  int ret = ray_runtime_remove_placement_group(TestPGHex().c_str(), &error);

  EXPECT_EQ(ret, 1);
  EXPECT_EQ(error, nullptr);
  free(error);
}

TEST_F(PlacementGroupOpsTest, RemovePlacementGroupRejectsInvalidHex) {
  // FromHex returns Nil for malformed input (it does not throw); the boundary
  // must reject a nil id and never forward it to the seam.
  EXPECT_CALL(*mock_, RemovePlacementGroup(_)).Times(0);

  char *error = nullptr;
  int ret = ray_runtime_remove_placement_group("zzz", &error);

  EXPECT_EQ(ret, 0);
  EXPECT_NE(error, nullptr);
  EXPECT_NE(std::string(error).find("invalid placement group id"), std::string::npos);
  free(error);
}

TEST_F(PlacementGroupOpsTest, RemovePlacementGroupForwardsErrorFromSeam) {
  // A non-OK status from the seam must surface as a boundary error.
  EXPECT_CALL(*mock_, RemovePlacementGroup(PlacementGroupID::FromHex(TestPGHex())))
      .WillOnce(Return(ray::Status::Invalid("boom")));

  char *error = nullptr;
  int ret = ray_runtime_remove_placement_group(TestPGHex().c_str(), &error);

  EXPECT_EQ(ret, 0);
  EXPECT_NE(error, nullptr);
  EXPECT_NE(std::string(error).find("boom"), std::string::npos);
  free(error);
}

// ============================================================================
// C Boundary: WaitPlacementGroupReady Tests
// ============================================================================

TEST_F(PlacementGroupOpsTest, WaitPlacementGroupReadyForwardsIdAndTimeout) {
  EXPECT_CALL(*mock_, WaitPlacementGroupReady(PlacementGroupID::FromHex(TestPGHex()), 5))
      .WillOnce(Return(ray::Status::OK()));

  int ready = 0;
  char *error = nullptr;
  int ret = ray_runtime_wait_placement_group_ready(
      TestPGHex().c_str(), /*timeout_seconds=*/5, &ready, &error);

  EXPECT_EQ(ret, 1);
  EXPECT_EQ(ready, 1);
  EXPECT_EQ(error, nullptr);
  free(error);
}

TEST_F(PlacementGroupOpsTest, WaitPlacementGroupReadyTimeoutReportsNotReady) {
  // A timed-out wait is not an error at the boundary: the call succeeds but
  // the ready flag is cleared.
  EXPECT_CALL(*mock_, WaitPlacementGroupReady(_, _))
      .WillOnce(Return(ray::Status::TimedOut("not ready")));

  int ready = 1;
  char *error = nullptr;
  int ret = ray_runtime_wait_placement_group_ready(
      TestPGHex().c_str(), /*timeout_seconds=*/1, &ready, &error);

  EXPECT_EQ(ret, 1);
  EXPECT_EQ(ready, 0);
  EXPECT_EQ(error, nullptr);
  free(error);
}

TEST_F(PlacementGroupOpsTest, WaitPlacementGroupReadyRejectsInvalidHex) {
  // A malformed hex id must be rejected at the boundary, not forwarded to the
  // seam as a nil id.
  EXPECT_CALL(*mock_, WaitPlacementGroupReady(_, _)).Times(0);

  int ready = 1;
  char *error = nullptr;
  int ret = ray_runtime_wait_placement_group_ready(
      "zzz", /*timeout_seconds=*/1, &ready, &error);

  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ready, 0);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST_F(PlacementGroupOpsTest, WaitPlacementGroupReadyNotFoundReportsError) {
  // Only TimedOut is folded into the not-ready flag; a NotFound (or any other
  // non-OK) status must surface as a boundary error.
  EXPECT_CALL(*mock_, WaitPlacementGroupReady(_, _))
      .WillOnce(Return(ray::Status::NotFound("no such pg")));

  int ready = 1;
  char *error = nullptr;
  int ret = ray_runtime_wait_placement_group_ready(
      TestPGHex().c_str(), /*timeout_seconds=*/1, &ready, &error);

  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ready, 0);
  EXPECT_NE(error, nullptr);
  EXPECT_NE(std::string(error).find("no such pg"), std::string::npos);
  free(error);
}

// ============================================================================
// ParseBundlesJson Tests
// ============================================================================

TEST_F(PlacementGroupOpsTest, ParseBundlesJsonRejectsMalformedJson) {
  std::vector<std::unordered_map<std::string, double>> bundles;
  EXPECT_FALSE(PlacementGroupOperations::ParseBundlesJson("not-json", &bundles));
  EXPECT_TRUE(PlacementGroupOperations::ParseBundlesJson("[]", &bundles));
  EXPECT_EQ(bundles.size(), 0u);
}

TEST_F(PlacementGroupOpsTest, ParseBundlesJsonParsesResourceMaps) {
  std::vector<std::unordered_map<std::string, double>> bundles;
  EXPECT_TRUE(PlacementGroupOperations::ParseBundlesJson(
      R"([{"CPU":1,"memory":128},{"GPU":0.5}])", &bundles));
  ASSERT_EQ(bundles.size(), 2u);
  EXPECT_NEAR(bundles[0]["CPU"], 1.0, 1e-6);
  EXPECT_NEAR(bundles[0]["memory"], 128.0, 1e-6);
  EXPECT_NEAR(bundles[1]["GPU"], 0.5, 1e-6);
}

}  // namespace go
}  // namespace ray
