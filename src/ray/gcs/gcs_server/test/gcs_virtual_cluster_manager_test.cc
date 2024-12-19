// Copyright 2017 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

// clang-format off
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/pubsub/subscriber.h"

// clang-format on

namespace ray {
namespace gcs {
class GcsVirtualClusterManagerTest : public ::testing::Test {
 public:
  GcsVirtualClusterManagerTest() {
    gcs_publisher_ = std::make_unique<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_unique<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_virtual_cluster_manager_ = std::make_unique<gcs::GcsVirtualClusterManager>(
        *gcs_table_storage_, *gcs_publisher_);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<gcs::GcsVirtualClusterManager> gcs_virtual_cluster_manager_;
};

class PrimaryClusterTest : public ::testing::Test {};

TEST_F(PrimaryClusterTest, NodeAddAndRemove) {
  auto primary_cluster = std::make_shared<ray::gcs::PrimaryCluster>(
      [](auto data, auto callback) { return Status::OK(); });

  size_t node_count = 200;
  size_t template_count = 10;
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>>
      template_id_to_nodes;
  for (size_t i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    auto template_id = std::to_string(i % template_count);
    node->set_node_type_name(template_id);
    primary_cluster->OnNodeAdd(*node);
    template_id_to_nodes[template_id].emplace(NodeID::FromBinary(node->node_id()), node);
  }

  const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
  EXPECT_EQ(visiable_node_instances.size(), template_count);
  for (auto &[template_id, job_node_instances] : visiable_node_instances) {
    EXPECT_EQ(job_node_instances.size(), 1);
    EXPECT_EQ(job_node_instances.begin()->first, ray::gcs::kEmptyJobClusterId);
    EXPECT_EQ(job_node_instances.begin()->second.size(), node_count / template_count);
  }

  size_t each_template_keeps_node_count = 8;
  ASSERT_TRUE(node_count / template_count > each_template_keeps_node_count);
  for (auto &[template_id, nodes] : template_id_to_nodes) {
    auto iter = nodes.begin();
    for (; iter != nodes.end();) {
      if (nodes.size() == each_template_keeps_node_count) {
        break;
      }
      auto current_iter = iter++;
      primary_cluster->OnNodeDead(*current_iter->second);
      nodes.erase(current_iter);
    }
  }
  for (auto &[template_id, job_node_instances] : visiable_node_instances) {
    EXPECT_EQ(job_node_instances.size(), 1);
    EXPECT_EQ(job_node_instances.begin()->first, ray::gcs::kEmptyJobClusterId);
    EXPECT_EQ(job_node_instances.begin()->second.size(), node_count / template_count);

    size_t alive_count = 0;
    for (const auto &[id, node_instance] : job_node_instances.begin()->second) {
      if (!node_instance->is_dead()) {
        alive_count++;
      }
    }
    EXPECT_EQ(alive_count, each_template_keeps_node_count);
  }

  each_template_keeps_node_count = 0;
  for (auto &[template_id, nodes] : template_id_to_nodes) {
    auto iter = nodes.begin();
    for (; iter != nodes.end();) {
      if (nodes.size() == each_template_keeps_node_count) {
        break;
      }
      auto current_iter = iter++;
      primary_cluster->OnNodeDead(*current_iter->second);
      nodes.erase(current_iter);
    }
  }
  EXPECT_EQ(visiable_node_instances.size(), template_count);
  for (auto &[template_id, job_node_instances] : visiable_node_instances) {
    EXPECT_EQ(job_node_instances.size(), 1);
    EXPECT_EQ(job_node_instances.begin()->first, ray::gcs::kEmptyJobClusterId);
    EXPECT_EQ(job_node_instances.begin()->second.size(), node_count / template_count);

    size_t alive_count = 0;
    for (const auto &[id, node_instance] : job_node_instances.begin()->second) {
      if (!node_instance->is_dead()) {
        alive_count++;
      }
    }
    EXPECT_EQ(alive_count, each_template_keeps_node_count);
  }
}

TEST_F(PrimaryClusterTest, CreateOrUpdateVirtualCluster) {
  auto primary_cluster =
      std::make_shared<ray::gcs::PrimaryCluster>([](auto data, auto callback) {
        callback(Status::OK(), data);
        return Status::OK();
      });

  size_t node_count = 200;
  size_t template_count = 10;
  for (size_t i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    auto template_id = std::to_string(i % template_count);
    node->set_node_type_name(template_id);
    primary_cluster->OnNodeAdd(*node);
  }

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  {
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id("virtual_cluster_id_0");
    request.set_mode(rpc::AllocationMode::EXCLUSIVE);
    request.set_revision(0);
    request.mutable_replica_sets()->insert({template_id_0, 5});
    request.mutable_replica_sets()->insert({template_id_1, 10});
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  {
    // Check the logical cluster virtual_cluster_id_0 visible node instances.
    auto logical_cluster = primary_cluster->GetLogicalCluster("virtual_cluster_id_0");
    ASSERT_NE(logical_cluster, nullptr);
    const auto &visiable_node_instances = logical_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(), 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              10);

    // Check that the revision changed.
    EXPECT_NE(logical_cluster->GetRevision(), 0);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 remains template_count - 5 nodes, template_id_1 has
    // template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template - 10);

    // Check that the revision unchanged.
    EXPECT_NE(primary_cluster->GetRevision(), 0);
  }

  {
    // Create virtual_cluster_id_1 and check that the status is ok.
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id("virtual_cluster_id_1");
    request.set_mode(rpc::AllocationMode::EXCLUSIVE);
    request.set_revision(0);
    request.mutable_replica_sets()->insert({template_id_0, node_count_per_template - 5});
    request.mutable_replica_sets()->insert({template_id_1, node_count_per_template - 10});
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  {
    // Check the logical cluster virtual_cluster_id_1 visible node instances.
    auto logical_cluster = primary_cluster->GetLogicalCluster("virtual_cluster_id_1");
    ASSERT_NE(logical_cluster, nullptr);
    const auto &visiable_node_instances = logical_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template - 10);

    // Check that the revision changed.
    EXPECT_NE(logical_cluster->GetRevision(), 0);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 remains template_count - 5 nodes, template_id_1 has
    // template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count - 2);
    EXPECT_FALSE(visiable_node_instances.contains(template_id_0));
    EXPECT_FALSE(visiable_node_instances.contains(template_id_1));

    // Check that the revision unchanged.
    EXPECT_NE(primary_cluster->GetRevision(), 0);
  }

  {
    // Create virtual_cluster_id_2 and check that the status is succeed.
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id("virtual_cluster_id_2");
    request.set_mode(rpc::AllocationMode::EXCLUSIVE);
    request.set_revision(0);
    request.mutable_replica_sets()->insert({template_id_0, 0});
    request.mutable_replica_sets()->insert({template_id_1, 0});
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());

    auto logical_cluster = primary_cluster->GetLogicalCluster("virtual_cluster_id_2");
    ASSERT_NE(logical_cluster, nullptr);
    ASSERT_EQ(logical_cluster->GetVisibleNodeInstances().size(), 2);
    ASSERT_EQ(logical_cluster->GetVisibleNodeInstances().at(template_id_0).size(), 0);
    ASSERT_EQ(logical_cluster->GetVisibleNodeInstances().at(template_id_1).size(), 0);

    ASSERT_EQ(logical_cluster->GetReplicaSets().size(), 2);
    ASSERT_EQ(logical_cluster->GetReplicaSets().at(template_id_0), 0);
    ASSERT_EQ(logical_cluster->GetReplicaSets().at(template_id_1), 0);
  }

  {
    // Create virtual_cluster_id_3 and check that the status is failed.
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id("virtual_cluster_id_3");
    request.set_mode(rpc::AllocationMode::EXCLUSIVE);
    request.set_revision(0);
    request.mutable_replica_sets()->insert({template_id_0, 1});
    request.mutable_replica_sets()->insert({template_id_1, 0});
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(primary_cluster->GetLogicalCluster("virtual_cluster_id_3"), nullptr);
  }
}

TEST_F(PrimaryClusterTest, CreateJobCluster) {
  auto primary_cluster =
      std::make_shared<ray::gcs::PrimaryCluster>([](auto data, auto callback) {
        callback(Status::OK(), data);
        return Status::OK();
      });

  size_t node_count = 200;
  size_t template_count = 10;
  for (size_t i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    auto template_id = std::to_string(i % template_count);
    node->set_node_type_name(template_id);
    primary_cluster->OnNodeAdd(*node);
  }

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  std::string job_id_0 = "job_0";

  {
    // Create job_cluster_id_0 and check that the status is ok.
    auto status = primary_cluster->CreateJobCluster(
        job_id_0,
        {{template_id_0, 5}, {template_id_1, 10}},
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  auto job_cluster_0 = primary_cluster->GetJobCluster(job_id_0);
  ASSERT_NE(job_cluster_0, nullptr);
  auto job_cluster_id_0 = job_cluster_0->GetID();
  ASSERT_EQ(job_cluster_id_0, kPrimaryClusterID + "##" + job_id_0);
  {
    // Check the job cluster job_cluster_id_0 visible node instances.
    const auto &visiable_node_instances = job_cluster_0->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(), 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              10);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that job_cluster_id_0 in template_id_0 has 5 nodes, kEmptyJobClusterId in
    // template_id_0 has template_count - 5 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_0).size(), 5);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template - 5);

    // Check that job_cluster_id_0 in template_id_1 has 10 nodes, kEmptyJobClusterId in
    // template_id_1 has template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_0).size(), 10);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template - 10);
  }

  std::string job_id_1 = "job_1";
  {
    // Create job_cluster_id_1 and check that the status is ok.
    auto status = primary_cluster->CreateJobCluster(
        job_id_1,
        {{template_id_0, node_count_per_template - 5},
         {template_id_1, node_count_per_template - 10}},
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  auto job_cluster_1 = primary_cluster->GetJobCluster(job_id_1);
  ASSERT_NE(job_cluster_1, nullptr);
  auto job_cluster_id_1 = job_cluster_1->GetID();
  {
    // Check the job cluster job_cluster_id_1 visible node instances.
    const auto &visiable_node_instances = job_cluster_1->GetVisibleNodeInstances();
    // Check that template_id_0 has node_count_per_template - 5 nodes, template_id_1 has
    // node_count_per_template - 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template - 10);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that job_cluster_id_0 in template_id_0 has 5 nodes,
    // job_cluster_id_1 in template_id_0 has template_count - 5 nodes, kEmptyJobClusterId
    // does not exist in template_id_0.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_0).size(), 5);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_1).size(),
              node_count_per_template - 5);
    ASSERT_FALSE(visiable_node_instances.at(template_id_0).contains(kEmptyJobClusterId));

    // Check that job_cluster_id_0 in template_id_1 has 10 nodes,
    // job_cluster_id_1 in template_id_0 has template_count - 10 nodes, kEmptyJobClusterId
    // does not exist in template_id_1.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_0).size(), 10);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_1).size(),
              node_count_per_template - 10);
    ASSERT_FALSE(visiable_node_instances.at(template_id_0).contains(kEmptyJobClusterId));
  }

  {
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id("virtual_cluster_id_0");
    request.set_mode(rpc::AllocationMode::EXCLUSIVE);
    request.set_revision(0);
    request.mutable_replica_sets()->insert({template_id_0, 2});
    request.mutable_replica_sets()->insert({template_id_1, 2});
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());

    std::string job_id_1 = "job_1";
    auto logical_cluster = primary_cluster->GetLogicalCluster(virtual_cluster_id_0);
    ExclusiveCluster *exclusive_cluster =
        dynamic_cast<ExclusiveCluster *>(logical_cluster.get());
    // Create job_cluster_id_1 and check that the status is ok.
    auto status = exclusive_cluster->CreateJobCluster(
        job_id_1,
        {{template_id_0, 1}, {template_id_1, 1}},
        [this, job_id_1](const Status &status,
                         std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_EQ(data->id(), std::string("virtual_cluster_id_0") + "##" + job_id_1);
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }
}

TEST_F(PrimaryClusterTest, RemoveJobCluster) {
  auto primary_cluster =
      std::make_shared<ray::gcs::PrimaryCluster>([](auto data, auto callback) {
        callback(Status::OK(), data);
        return Status::OK();
      });

  size_t node_count = 200;
  size_t template_count = 10;
  for (size_t i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    auto template_id = std::to_string(i % template_count);
    node->set_node_type_name(template_id);
    primary_cluster->OnNodeAdd(*node);
  }

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  std::string job_id_0 = "job_0";

  {
    // Create job_cluster_id_0 and check that the status is ok.
    auto status = primary_cluster->CreateJobCluster(
        job_id_0,
        {{template_id_0, 5}, {template_id_1, 10}},
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  auto job_cluster_0 = primary_cluster->GetJobCluster(job_id_0);
  ASSERT_NE(job_cluster_0, nullptr);
  auto job_cluster_id_0 = job_cluster_0->GetID();
  {
    // Check the job cluster job_cluster_id_0 visible node instances.
    const auto &visiable_node_instances = job_cluster_0->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(), 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              10);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that job_cluster_id_0 in template_id_0 has 5 nodes, kEmptyJobClusterId in
    // template_id_0 has template_count - 5 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_0).size(), 5);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template - 5);

    // Check that job_cluster_id_0 in template_id_1 has 10 nodes, kEmptyJobClusterId in
    // template_id_1 has template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_0).size(), 10);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template - 10);
  }

  {
    auto status = primary_cluster->RemoveJobCluster(
        job_id_0,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(data->is_removed());
        });
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(primary_cluster->GetJobCluster(job_id_0), nullptr);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template);

    // Check that template_id_1 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template);
  }

  {
    // Remove the job cluster that does not exist.
    auto status = primary_cluster->RemoveJobCluster(
        "job_1",
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_FALSE(true);
        });
    ASSERT_TRUE(status.IsNotFound());
  }
}

TEST_F(PrimaryClusterTest, RemoveLogicalCluster) {
  auto primary_cluster =
      std::make_shared<ray::gcs::PrimaryCluster>([](auto data, auto callback) {
        callback(Status::OK(), data);
        return Status::OK();
      });

  size_t node_count = 200;
  size_t template_count = 10;
  for (size_t i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    auto template_id = std::to_string(i % template_count);
    node->set_node_type_name(template_id);
    primary_cluster->OnNodeAdd(*node);
  }

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  std::string virtual_cluster_id_0 = "virtual_cluster_id_0";

  {
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id(virtual_cluster_id_0);
    request.set_mode(rpc::AllocationMode::EXCLUSIVE);
    request.set_revision(0);
    request.mutable_replica_sets()->insert({template_id_0, 5});
    request.mutable_replica_sets()->insert({template_id_1, 10});
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  {
    // Check the logical cluster virtual_cluster_id_0 visible node instances.
    auto logical_cluster = primary_cluster->GetLogicalCluster(virtual_cluster_id_0);
    ASSERT_NE(logical_cluster, nullptr);
    const auto &visiable_node_instances = logical_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(), 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              10);

    // Check that the revision changed.
    EXPECT_NE(logical_cluster->GetRevision(), 0);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 remains template_count - 5 nodes, template_id_1 has
    // template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template - 10);

    // Check that the revision unchanged.
    EXPECT_NE(primary_cluster->GetRevision(), 0);
  }

  {
    auto status = primary_cluster->RemoveLogicalCluster(
        virtual_cluster_id_0,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(data->is_removed());
        });
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(primary_cluster->GetLogicalCluster(virtual_cluster_id_0), nullptr);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kEmptyJobClusterId).size(),
              node_count_per_template);

    // Check that template_id_1 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kEmptyJobClusterId).size(),
              node_count_per_template);
  }

  {
    // Remove the logical cluster that does not exist.
    auto status = primary_cluster->RemoveLogicalCluster(
        virtual_cluster_id_0,
        [this](const Status &status, std::shared_ptr<rpc::VirtualClusterTableData> data) {
          ASSERT_FALSE(true);
        });
    ASSERT_TRUE(status.IsNotFound());
  }
}

}  // namespace gcs
}  // namespace ray
