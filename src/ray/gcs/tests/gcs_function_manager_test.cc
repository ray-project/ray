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

#include "ray/gcs/gcs_function_manager.h"

#include <gtest/gtest.h>

#include <memory>

#include "mock/ray/gcs/gcs_kv_manager.h"

namespace ray {

class GCSFunctionManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
    fake_kv_ = std::make_unique<gcs::FakeInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GCSFunctionManager>(*fake_kv_, io_context_);
  }

 protected:
  std::unique_ptr<gcs::GCSFunctionManager> function_manager_;
  std::unique_ptr<gcs::FakeInternalKVInterface> fake_kv_;
  instrumented_io_context io_context_;

  // Helper method to check if a key exists in the fake KV store
  bool HasKey(const std::string &ns, const std::string &key) {
    std::string full_key = ns + key;
    return fake_kv_->kv_store_.find(full_key) != fake_kv_->kv_store_.end();
  }
};

TEST_F(GCSFunctionManagerTest, TestFunctionManagerGC) {
  JobID job_id = JobID::FromInt(1);
  std::string job_id_hex = job_id.Hex();

  // Pre-populate KV store with function/actor data for this job
  fake_kv_->kv_store_["funRemoteFunction:" + job_id_hex + ":key1"] = "value1";
  fake_kv_->kv_store_["funActorClass:" + job_id_hex + ":key1"] = "value1";
  fake_kv_->kv_store_["funFunctionsToRun:" + job_id_hex + ":key1"] = "value1";

  function_manager_->AddJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_TRUE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_TRUE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));

  function_manager_->AddJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager_->AddJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager_->RemoveJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager_->RemoveJobReference(job_id);
  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));

  function_manager_->RemoveJobReference(job_id);
  // Now all keys should be deleted (job finished)
  EXPECT_FALSE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));
}

TEST_F(GCSFunctionManagerTest, TestRemoveJobReferenceIsIdempotent) {
  JobID job_id = JobID::FromInt(2);
  std::string job_id_hex = job_id.Hex();

  // Pre-populate KV store with function/actor data for this job
  fake_kv_->kv_store_["funRemoteFunction:" + job_id_hex + ":key1"] = "value1";
  fake_kv_->kv_store_["funActorClass:" + job_id_hex + ":key1"] = "value1";
  fake_kv_->kv_store_["funFunctionsToRun:" + job_id_hex + ":key1"] = "value1";

  // Add a job reference (counter becomes 1)
  function_manager_->AddJobReference(job_id);

  // Keys should still exist (job not finished)
  EXPECT_TRUE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_TRUE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_TRUE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));

  // First RemoveJobReference call - should succeed and trigger cleanup
  function_manager_->RemoveJobReference(job_id);

  // Keys should now be deleted (job finished)
  EXPECT_FALSE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));

  // Network retry scenario: Subsequent calls should be handled gracefully.
  // This simulates when raylet retries MarkJobFinished due to network failures
  function_manager_->RemoveJobReference(job_id);
  function_manager_->RemoveJobReference(job_id);
  function_manager_->RemoveJobReference(job_id);

  // Keys should still be deleted (idempotent behavior - no crashes or state changes)
  EXPECT_FALSE(HasKey("fun", "RemoteFunction:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "ActorClass:" + job_id_hex + ":key1"));
  EXPECT_FALSE(HasKey("fun", "FunctionsToRun:" + job_id_hex + ":key1"));
}

}  // namespace ray
