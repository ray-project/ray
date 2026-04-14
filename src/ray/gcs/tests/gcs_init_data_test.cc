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

#include "ray/gcs/gcs_init_data.h"

#include <gtest/gtest.h>

#include <memory>

#include "ray/common/asio/asio_util.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {
namespace gcs {

class GcsInitDataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    io_context_ =
        std::make_unique<InstrumentedIOContextWithThread>("GcsInitDataTest");
    store_client_ = std::make_shared<InMemoryStoreClient>();
    gcs_table_storage_ = std::make_unique<GcsTableStorage>(store_client_);
  }

  void TearDown() override { io_context_.reset(); }

  // Helper to synchronously put a job into the table.
  void PutJob(const JobID &job_id, const rpc::JobTableData &data) {
    std::promise<void> p;
    gcs_table_storage_->JobTable().Put(
        job_id, data, {[&p](ray::Status) { p.set_value(); }, io_context_->GetIoService()});
    p.get_future().get();
  }

  // Helper to synchronously put a node into the table.
  void PutNode(const NodeID &node_id, const rpc::GcsNodeInfo &data) {
    std::promise<void> p;
    gcs_table_storage_->NodeTable().Put(
        node_id, data, {[&p](ray::Status) { p.set_value(); }, io_context_->GetIoService()});
    p.get_future().get();
  }

  // Helper to synchronously put a placement group into the table.
  void PutPlacementGroup(const PlacementGroupID &pg_id,
                         const rpc::PlacementGroupTableData &data) {
    std::promise<void> p;
    gcs_table_storage_->PlacementGroupTable().Put(
        pg_id, data, {[&p](ray::Status) { p.set_value(); }, io_context_->GetIoService()});
    p.get_future().get();
  }

  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
  std::shared_ptr<InMemoryStoreClient> store_client_;
  std::unique_ptr<GcsTableStorage> gcs_table_storage_;
};

TEST_F(GcsInitDataTest, TestAsyncLoadEmpty) {
  GcsInitData init_data(*gcs_table_storage_);
  std::promise<void> p;
  init_data.AsyncLoad(
      {[&p]() { p.set_value(); }, io_context_->GetIoService()});
  p.get_future().get();

  ASSERT_TRUE(init_data.Jobs().empty());
  ASSERT_TRUE(init_data.Nodes().empty());
  ASSERT_TRUE(init_data.Actors().empty());
  ASSERT_TRUE(init_data.ActorTaskSpecs().empty());
  ASSERT_TRUE(init_data.PlacementGroups().empty());
}

TEST_F(GcsInitDataTest, TestAsyncLoadWithJobs) {
  // Pre-populate 3 jobs.
  for (int i = 0; i < 3; i++) {
    auto job_id = JobID::FromInt(i + 1);
    rpc::JobTableData job_data;
    job_data.set_job_id(job_id.Binary());
    job_data.set_is_dead(false);
    PutJob(job_id, job_data);
  }

  GcsInitData init_data(*gcs_table_storage_);
  std::promise<void> p;
  init_data.AsyncLoad(
      {[&p]() { p.set_value(); }, io_context_->GetIoService()});
  p.get_future().get();

  ASSERT_EQ(init_data.Jobs().size(), 3);
  // Verify a specific job is present.
  auto job_id_1 = JobID::FromInt(1);
  ASSERT_TRUE(init_data.Jobs().contains(job_id_1));
}

TEST_F(GcsInitDataTest, TestAsyncLoadWithNodes) {
  // Pre-populate 2 nodes.
  for (int i = 0; i < 2; i++) {
    auto node_id = NodeID::FromRandom();
    rpc::GcsNodeInfo node_info;
    node_info.set_node_id(node_id.Binary());
    node_info.set_node_manager_address("127.0.0.1");
    node_info.set_state(rpc::GcsNodeInfo::ALIVE);
    PutNode(node_id, node_info);
  }

  GcsInitData init_data(*gcs_table_storage_);
  std::promise<void> p;
  init_data.AsyncLoad(
      {[&p]() { p.set_value(); }, io_context_->GetIoService()});
  p.get_future().get();

  ASSERT_EQ(init_data.Nodes().size(), 2);
}

TEST_F(GcsInitDataTest, TestAsyncLoadWithPlacementGroups) {
  auto pg_id = PlacementGroupID::Of(JobID::FromInt(99));
  rpc::PlacementGroupTableData pg_data;
  pg_data.set_placement_group_id(pg_id.Binary());
  pg_data.set_state(rpc::PlacementGroupTableData::PENDING);
  PutPlacementGroup(pg_id, pg_data);

  GcsInitData init_data(*gcs_table_storage_);
  std::promise<void> p;
  init_data.AsyncLoad(
      {[&p]() { p.set_value(); }, io_context_->GetIoService()});
  p.get_future().get();

  ASSERT_EQ(init_data.PlacementGroups().size(), 1);
  ASSERT_TRUE(init_data.PlacementGroups().contains(pg_id));
}

TEST_F(GcsInitDataTest, TestAsyncLoadAllTables) {
  // Populate all table types.
  auto job_id = JobID::FromInt(1);
  rpc::JobTableData job_data;
  job_data.set_job_id(job_id.Binary());
  PutJob(job_id, job_data);

  auto node_id = NodeID::FromRandom();
  rpc::GcsNodeInfo node_info;
  node_info.set_node_id(node_id.Binary());
  PutNode(node_id, node_info);

  auto pg_id = PlacementGroupID::Of(JobID::FromInt(99));
  rpc::PlacementGroupTableData pg_data;
  pg_data.set_placement_group_id(pg_id.Binary());
  PutPlacementGroup(pg_id, pg_data);

  GcsInitData init_data(*gcs_table_storage_);
  std::promise<void> p;
  init_data.AsyncLoad(
      {[&p]() { p.set_value(); }, io_context_->GetIoService()});
  p.get_future().get();

  ASSERT_EQ(init_data.Jobs().size(), 1);
  ASSERT_EQ(init_data.Nodes().size(), 1);
  ASSERT_EQ(init_data.PlacementGroups().size(), 1);
  // Actors and ActorTaskSpecs are empty since we didn't populate them.
  ASSERT_TRUE(init_data.Actors().empty());
  ASSERT_TRUE(init_data.ActorTaskSpecs().empty());
}

}  // namespace gcs
}  // namespace ray
