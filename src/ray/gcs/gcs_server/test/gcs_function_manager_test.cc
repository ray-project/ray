// Copyright 2021 The Ray Authors.
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
// clang-format off
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_function_manager.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
// clang-format on
using namespace ::testing;  // NOLINT
using namespace ray::gcs;   // NOLINT
using namespace ray;        // NOLINT

class GCSFunctionManagerTest : public Test {
 public:
  void SetUp() override {
    kv = std::make_unique<FakeInternalKVInterface>();
    function_manager = std::make_unique<GCSFunctionManager>(*kv, io_context);
  }
  std::unique_ptr<GCSFunctionManager> function_manager;
  std::unique_ptr<FakeInternalKVInterface> kv;
  instrumented_io_context io_context;

  // Helper method to check if a key exists in the fake KV store
  bool HasKey(const std::string &ns, const std::string &key) {
    std::string full_key = ns + key;
    return kv->kv_store_.find(full_key) != kv->kv_store_.end();
  }
};

TEST_F(GCSFunctionManagerTest, TestFunctionManagerGC) {
  JobID job_id = BaseID<JobID>::FromRandom();
  std::string job_id_hex = job_id.Hex();

  // Pre-populate KV store with function/actor data for this job
  kv->kv_store_["funRemoteFunction:" + job_id_hex + ":key1"] = "value1";
  kv->kv_store_["funActorClass:" + job_id_hex + ":key1"] = "value1";
  kv->kv_store_["funFunctionsToRun:" + job_id_hex + ":key1"] = "value1";

  function_manager->AddJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_TRUE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_TRUE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));

  function_manager->AddJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager->AddJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager->RemoveJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager->RemoveJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager->RemoveJobReference(job_id);
  // Now all keys should be deleted (job finished)
  EXPECT_FALSE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));
}
