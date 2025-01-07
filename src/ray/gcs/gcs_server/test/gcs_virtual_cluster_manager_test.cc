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
  GcsVirtualClusterManagerTest() : cluster_resource_manager_(io_service_) {
    gcs_publisher_ = std::make_unique<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_unique<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_virtual_cluster_manager_ = std::make_unique<gcs::GcsVirtualClusterManager>(
        *gcs_table_storage_, *gcs_publisher_, cluster_resource_manager_);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<gcs::GcsVirtualClusterManager> gcs_virtual_cluster_manager_;
  ClusterResourceManager cluster_resource_manager_;
};

class MockGcsInitData : public GcsInitData {
 public:
  using GcsInitData::GcsInitData;

  void SetNodes(
      const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> &nodes) {
    for (const auto &[node_id, node] : nodes) {
      node_table_data_[node_id] = *node;
    }
  }

  void SetVirtualClusters(
      const absl::flat_hash_map<std::string,
                                std::shared_ptr<rpc::VirtualClusterTableData>>
          &virtual_clusters) {
    for (const auto &[virtual_cluster_id, virtual_cluster] : virtual_clusters) {
      virtual_cluster_table_data_[VirtualClusterID::FromBinary(virtual_cluster_id)] =
          *virtual_cluster;
    }
  }
};

bool operator==(const ReplicaInstances &lhs, const ReplicaInstances &rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (const auto &[template_id, job_node_instances] : lhs) {
    if (!rhs.contains(template_id)) {
      return false;
    }
    const auto &rhs_job_node_instances = rhs.at(template_id);
    if (job_node_instances.size() != rhs_job_node_instances.size()) {
      return false;
    }
    for (const auto &[job_cluster_id, node_instances] : job_node_instances) {
      if (!rhs_job_node_instances.contains(job_cluster_id)) {
        return false;
      }
      const auto &rhs_node_instances = rhs_job_node_instances.at(job_cluster_id);
      if (node_instances.size() != rhs_node_instances.size()) {
        return false;
      }
      for (const auto &[node_instance_id, node_instance] : node_instances) {
        if (!rhs_node_instances.contains(node_instance_id)) {
          return false;
        }
        const auto &rhs_node_instance = rhs_node_instances.at(node_instance_id);
        if (node_instance->hostname() != rhs_node_instance->hostname() ||
            node_instance->template_id() != rhs_node_instance->template_id() ||
            node_instance->is_dead() != rhs_node_instance->is_dead()) {
          return false;
        }
      }
    }
  }
  return true;
}

// lhs <= rhs
bool operator<=(const ReplicaInstances &lhs, const ReplicaInstances &rhs) {
  if (lhs.size() > rhs.size()) {
    return false;
  }
  for (const auto &[template_id, job_node_instances] : lhs) {
    if (!rhs.contains(template_id)) {
      return false;
    }
    const auto &rhs_job_node_instances = rhs.at(template_id);
    if (job_node_instances.size() > rhs_job_node_instances.size()) {
      return false;
    }
    for (const auto &[job_cluster_id, node_instances] : job_node_instances) {
      if (!rhs_job_node_instances.contains(job_cluster_id)) {
        return false;
      }
      const auto &rhs_node_instances = rhs_job_node_instances.at(job_cluster_id);
      if (node_instances.size() > rhs_node_instances.size()) {
        return false;
      }
      for (const auto &[node_instance_id, node_instance] : node_instances) {
        if (!rhs_node_instances.contains(node_instance_id)) {
          return false;
        }
        const auto &rhs_node_instance = rhs_node_instances.at(node_instance_id);
        if (node_instance->hostname() != rhs_node_instance->hostname() ||
            node_instance->template_id() != rhs_node_instance->template_id() ||
            node_instance->is_dead() != rhs_node_instance->is_dead()) {
          return false;
        }
      }
    }
  }
  return true;
}

bool operator==(const ReplicaSets &lhs, const ReplicaSets &rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (const auto &[template_id, count] : lhs) {
    if (!rhs.contains(template_id) || rhs.at(template_id) != count) {
      return false;
    }
  }
  return true;
}

bool operator<=(const ReplicaSets &lhs, const ReplicaSets &rhs) {
  if (lhs.size() > rhs.size()) {
    return false;
  }
  for (const auto &[template_id, count] : lhs) {
    if (!rhs.contains(template_id) || rhs.at(template_id) < count) {
      return false;
    }
  }
  return true;
}

class VirtualClusterTest : public ::testing::Test {
 public:
  VirtualClusterTest() : cluster_resource_manager_(io_service_) {
    async_data_flusher_ = [this](auto data, auto callback) {
      virtual_clusters_data_[data->id()] = data;
      if (callback != nullptr) {
        callback(Status::OK(), data, nullptr);
      }
      return Status::OK();
    };
  }

  std::shared_ptr<ray::gcs::PrimaryCluster> InitPrimaryCluster(
      size_t node_count,
      size_t template_count,
      absl::flat_hash_map<std::string,
                          absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>>
          *template_id_to_nodes = nullptr) {
    auto primary_cluster = std::make_shared<ray::gcs::PrimaryCluster>(
        async_data_flusher_, cluster_resource_manager_);

    for (size_t i = 0; i < node_count; ++i) {
      auto node = Mocker::GenNodeInfo();
      auto template_id = std::to_string(i % template_count);
      node->set_node_type_name(template_id);
      primary_cluster->OnNodeAdd(*node);
      if (template_id_to_nodes != nullptr) {
        (*template_id_to_nodes)[template_id].emplace(NodeID::FromBinary(node->node_id()),
                                                     node);
      }
      nodes_.emplace(NodeID::FromBinary(node->node_id()), node);
    }
    return primary_cluster;
  }

  Status CreateVirtualCluster(
      std::shared_ptr<ray::gcs::PrimaryCluster> primary_cluster,
      const std::string &virtual_cluster_id,
      const absl::flat_hash_map<std::string, size_t> &replica_sets,
      bool divisible = true) {
    rpc::CreateOrUpdateVirtualClusterRequest request;
    request.set_virtual_cluster_id(virtual_cluster_id);
    request.set_divisible(true);
    request.set_revision(0);
    request.mutable_replica_sets()->insert(replica_sets.begin(), replica_sets.end());
    auto status = primary_cluster->CreateOrUpdateVirtualCluster(
        request,
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
        });
    return status;
  }

  instrumented_io_context io_service_;
  ClusterResourceManager cluster_resource_manager_;
  AsyncClusterDataFlusher async_data_flusher_;
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> nodes_;
  absl::flat_hash_map<std::string, std::shared_ptr<rpc::VirtualClusterTableData>>
      virtual_clusters_data_;
};

class PrimaryClusterTest : public VirtualClusterTest {
 public:
  using VirtualClusterTest::VirtualClusterTest;
};

TEST_F(PrimaryClusterTest, NodeAddAndRemove) {
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>>
      template_id_to_nodes;
  size_t node_count = 200;
  size_t template_count = 10;
  auto primary_cluster =
      InitPrimaryCluster(node_count, template_count, &template_id_to_nodes);

  const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
  EXPECT_EQ(visiable_node_instances.size(), template_count);
  for (auto &[template_id, job_node_instances] : visiable_node_instances) {
    EXPECT_EQ(job_node_instances.size(), 1);
    EXPECT_EQ(job_node_instances.begin()->first, ray::gcs::kUndividedClusterId);
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
    EXPECT_EQ(job_node_instances.begin()->first, ray::gcs::kUndividedClusterId);
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
    EXPECT_EQ(job_node_instances.begin()->first, ray::gcs::kUndividedClusterId);
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
  size_t node_count = 200;
  size_t template_count = 10;
  auto primary_cluster = InitPrimaryCluster(node_count, template_count);

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  std::string virtual_cluster_id_0 = "virtual_cluster_id_0";
  ASSERT_TRUE(CreateVirtualCluster(primary_cluster,
                                   virtual_cluster_id_0,
                                   {{template_id_0, 5}, {template_id_1, 10}})
                  .ok());

  {
    // Check the logical cluster virtual_cluster_id_0 visible node instances.
    auto logical_cluster = primary_cluster->GetLogicalCluster("virtual_cluster_id_0");
    ASSERT_NE(logical_cluster, nullptr);
    const auto &visiable_node_instances = logical_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
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
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template - 10);

    // Check that the revision unchanged.
    EXPECT_NE(primary_cluster->GetRevision(), 0);
  }

  // Create virtual_cluster_id_1 and check that the status is ok.
  std::string virtual_cluster_id_1 = "virtual_cluster_id_1";
  ASSERT_TRUE(CreateVirtualCluster(primary_cluster,
                                   virtual_cluster_id_1,
                                   {{template_id_0, node_count_per_template - 5},
                                    {template_id_1, node_count_per_template - 10}})
                  .ok());

  {
    // Check the logical cluster virtual_cluster_id_1 visible node instances.
    auto logical_cluster = primary_cluster->GetLogicalCluster("virtual_cluster_id_1");
    ASSERT_NE(logical_cluster, nullptr);
    const auto &visiable_node_instances = logical_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
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

  // Create virtual_cluster_id_2 and check that the status is succeed.
  std::string virtual_cluster_id_2 = "virtual_cluster_id_2";
  ASSERT_TRUE(CreateVirtualCluster(primary_cluster,
                                   virtual_cluster_id_2,
                                   {{template_id_0, 0}, {template_id_1, 0}})
                  .ok());

  {
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
    std::string virtual_cluster_id_3 = "virtual_cluster_id_3";
    ASSERT_FALSE(CreateVirtualCluster(primary_cluster,
                                      virtual_cluster_id_3,
                                      {{template_id_0, 5}, {template_id_1, 10}})
                     .ok());
    ASSERT_EQ(primary_cluster->GetLogicalCluster(virtual_cluster_id_3), nullptr);
  }
}

TEST_F(PrimaryClusterTest, CreateJobCluster) {
  size_t node_count = 200;
  size_t template_count = 10;
  auto primary_cluster = InitPrimaryCluster(node_count, template_count);

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  std::string job_cluster_id_0 = primary_cluster->BuildJobClusterID("job_0");
  {
    // Create job_cluster_id_0 and check that the status is ok.
    auto status = primary_cluster->CreateJobCluster(
        job_cluster_id_0,
        {{template_id_0, 5}, {template_id_1, 10}},
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  auto job_cluster_0 = primary_cluster->GetJobCluster(job_cluster_id_0);
  ASSERT_NE(job_cluster_0, nullptr);
  {
    // Check the job cluster job_cluster_id_0 visible node instances.
    const auto &visiable_node_instances = job_cluster_0->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              10);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that job_cluster_id_0 in template_id_0 has 5 nodes, kUndividedClusterId in
    // template_id_0 has template_count - 5 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_0).size(), 5);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template - 5);

    // Check that job_cluster_id_0 in template_id_1 has 10 nodes, kUndividedClusterId in
    // template_id_1 has template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_0).size(), 10);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template - 10);
  }

  std::string job_cluster_id_1 = primary_cluster->BuildJobClusterID("job_1");
  {
    // Create job_cluster_id_1 and check that the status is ok.
    auto status = primary_cluster->CreateJobCluster(
        job_cluster_id_1,
        {{template_id_0, node_count_per_template - 5},
         {template_id_1, node_count_per_template - 10}},
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  auto job_cluster_1 = primary_cluster->GetJobCluster(job_cluster_id_1);
  ASSERT_NE(job_cluster_1, nullptr);
  {
    // Check the job cluster job_cluster_id_1 visible node instances.
    const auto &visiable_node_instances = job_cluster_1->GetVisibleNodeInstances();
    // Check that template_id_0 has node_count_per_template - 5 nodes, template_id_1 has
    // node_count_per_template - 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template - 10);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that job_cluster_id_0 in template_id_0 has 5 nodes,
    // job_cluster_id_1 in template_id_0 has template_count - 5 nodes, kUndividedClusterId
    // does not exist in template_id_0.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_0).size(), 5);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_1).size(),
              node_count_per_template - 5);
    ASSERT_FALSE(visiable_node_instances.at(template_id_0).contains(kUndividedClusterId));

    // Check that job_cluster_id_0 in template_id_1 has 10 nodes,
    // job_cluster_id_1 in template_id_0 has template_count - 10 nodes,
    // kUndividedClusterId does not exist in template_id_1.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_0).size(), 10);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_1).size(),
              node_count_per_template - 10);
    ASSERT_FALSE(visiable_node_instances.at(template_id_0).contains(kUndividedClusterId));
  }
}

TEST_F(PrimaryClusterTest, RemoveJobCluster) {
  size_t node_count = 200;
  size_t template_count = 10;
  auto primary_cluster = InitPrimaryCluster(node_count, template_count);

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  std::string job_cluster_id_0 = primary_cluster->BuildJobClusterID("job_0");
  {
    // Create job_cluster_id_0 and check that the status is ok.
    auto status = primary_cluster->CreateJobCluster(
        job_cluster_id_0,
        {{template_id_0, 5}, {template_id_1, 10}},
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
        });
    ASSERT_TRUE(status.ok());
  }

  auto job_cluster_0 = primary_cluster->GetJobCluster(job_cluster_id_0);
  ASSERT_NE(job_cluster_0, nullptr);
  {
    // Check the job cluster job_cluster_id_0 visible node instances.
    const auto &visiable_node_instances = job_cluster_0->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              10);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that job_cluster_id_0 in template_id_0 has 5 nodes, kUndividedClusterId in
    // template_id_0 has template_count - 5 nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(job_cluster_id_0).size(), 5);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template - 5);

    // Check that job_cluster_id_0 in template_id_1 has 10 nodes, kUndividedClusterId in
    // template_id_1 has template_count - 10 nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(job_cluster_id_0).size(), 10);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template - 10);
  }

  {
    auto status = primary_cluster->RemoveJobCluster(
        job_cluster_id_0,
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(data->is_removed());
        });
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(primary_cluster->GetJobCluster(job_cluster_id_0), nullptr);
  }

  {
    // Check the primary cluster visible node instances.
    const auto &visiable_node_instances = primary_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.size(), template_count);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template);

    // Check that template_id_1 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template);
  }

  {
    std::string job_cluster_id_1 = primary_cluster->BuildJobClusterID("job_1");
    // Remove the job cluster that does not exist.
    auto status = primary_cluster->RemoveJobCluster(
        job_cluster_id_1,
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) { ASSERT_FALSE(true); });
    ASSERT_TRUE(status.IsNotFound());
  }
}

TEST_F(PrimaryClusterTest, RemoveVirtualCluster) {
  size_t node_count = 200;
  size_t template_count = 10;
  auto primary_cluster = InitPrimaryCluster(node_count, template_count);

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";
  size_t node_count_per_template = node_count / template_count;

  // Create virtual_cluster_id_0 and check that the status is succeed.
  std::string virtual_cluster_id_0 = "virtual_cluster_id_0";
  ASSERT_TRUE(CreateVirtualCluster(primary_cluster,
                                   virtual_cluster_id_0,
                                   {{template_id_0, 5}, {template_id_1, 10}})
                  .ok());

  {
    // Check the logical cluster virtual_cluster_id_0 visible node instances.
    auto logical_cluster = primary_cluster->GetLogicalCluster(virtual_cluster_id_0);
    ASSERT_NE(logical_cluster, nullptr);
    const auto &visiable_node_instances = logical_cluster->GetVisibleNodeInstances();
    // Check that template_id_0 has 5 nodes, template_id_1 has 10 nodes.
    EXPECT_EQ(visiable_node_instances.size(), 2);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
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
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template - 5);

    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template - 10);

    // Check that the revision unchanged.
    EXPECT_NE(primary_cluster->GetRevision(), 0);
  }

  {
    auto status = primary_cluster->RemoveVirtualCluster(
        virtual_cluster_id_0,
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) {
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
    EXPECT_EQ(visiable_node_instances.at(template_id_0).at(kUndividedClusterId).size(),
              node_count_per_template);

    // Check that template_id_1 has node_count_per_template nodes.
    EXPECT_EQ(visiable_node_instances.at(template_id_1).size(), 1);
    EXPECT_EQ(visiable_node_instances.at(template_id_1).at(kUndividedClusterId).size(),
              node_count_per_template);
  }

  {
    // Remove the logical cluster that does not exist.
    auto status = primary_cluster->RemoveVirtualCluster(
        virtual_cluster_id_0,
        [this](const Status &status,
               std::shared_ptr<rpc::VirtualClusterTableData> data,
               const ReplicaSets *replica_sets_to_recommend) { ASSERT_FALSE(true); });
    ASSERT_TRUE(status.IsNotFound());
  }
}

TEST_F(PrimaryClusterTest, GetVirtualClusters) {
  size_t node_count = 200;
  size_t template_count = 10;
  auto primary_cluster = InitPrimaryCluster(node_count, template_count);

  std::string template_id_0 = "0";
  std::string template_id_1 = "1";

  std::string virtual_cluster_id_0 = "virtual_cluster_id_0";
  std::string virtual_cluster_id_1 = "virtual_cluster_id_1";

  std::string job_cluster_id_0 = primary_cluster->BuildJobClusterID("job_0");
  ASSERT_TRUE(CreateVirtualCluster(primary_cluster,
                                   virtual_cluster_id_0,
                                   {{template_id_0, 5}, {template_id_1, 5}})
                  .ok());

  ASSERT_TRUE(CreateVirtualCluster(primary_cluster,
                                   virtual_cluster_id_1,
                                   {{template_id_0, 5}, {template_id_1, 5}})
                  .ok());

  ASSERT_TRUE(primary_cluster
                  ->CreateJobCluster(job_cluster_id_0,
                                     {{template_id_0, 10}, {template_id_1, 10}},
                                     [this](const Status &status,
                                            auto data,
                                            const auto *replica_sets_to_recommend) {
                                       ASSERT_TRUE(status.ok());
                                     })
                  .ok());

  auto virtual_cluster_0 = std::dynamic_pointer_cast<DivisibleCluster>(
      primary_cluster->GetLogicalCluster(virtual_cluster_id_0));
  ASSERT_TRUE(virtual_cluster_0 != nullptr);

  std::string job_cluster_id_1 = virtual_cluster_0->BuildJobClusterID("job_1");
  ASSERT_TRUE(virtual_cluster_0
                  ->CreateJobCluster(job_cluster_id_1,
                                     {{template_id_0, 2}, {template_id_1, 2}},
                                     [this](const Status &status,
                                            auto data,
                                            const auto *replica_sets_to_recommend) {
                                       ASSERT_TRUE(status.ok());
                                     })
                  .ok());

  {
    rpc::GetVirtualClustersRequest request;
    request.set_virtual_cluster_id(virtual_cluster_id_0);
    request.set_include_job_clusters(false);
    absl::flat_hash_map<std::string, std::shared_ptr<rpc::VirtualClusterTableData>>
        virtual_clusters_data_map;
    primary_cluster->ForeachVirtualClustersData(
        request, [this, &virtual_clusters_data_map](auto data) {
          virtual_clusters_data_map.emplace(data->id(), data);
        });
    ASSERT_EQ(virtual_clusters_data_map.size(), 1);
    ASSERT_TRUE(virtual_clusters_data_map.contains(virtual_cluster_id_0));

    virtual_clusters_data_map.clear();
    request.set_include_job_clusters(true);
    primary_cluster->ForeachVirtualClustersData(
        request, [this, &virtual_clusters_data_map](auto data) {
          virtual_clusters_data_map.emplace(data->id(), data);
        });
    ASSERT_EQ(virtual_clusters_data_map.size(), 2);
    ASSERT_TRUE(virtual_clusters_data_map.contains(virtual_cluster_id_0));

    auto job_cluster = virtual_cluster_0->GetJobCluster(job_cluster_id_1);
    ASSERT_TRUE(job_cluster != nullptr);
    ASSERT_TRUE(virtual_clusters_data_map.contains(job_cluster->GetID()));

    virtual_clusters_data_map.clear();
    request.set_include_job_clusters(true);
    request.set_only_include_indivisible_clusters(true);
    primary_cluster->ForeachVirtualClustersData(
        request, [this, &virtual_clusters_data_map](auto data) {
          virtual_clusters_data_map.emplace(data->id(), data);
        });
    ASSERT_EQ(virtual_clusters_data_map.size(), 1);
    ASSERT_FALSE(virtual_clusters_data_map.contains(virtual_cluster_id_0));
    ASSERT_TRUE(virtual_clusters_data_map.contains(job_cluster->GetID()));
  }
}

// ┌───────────────────────────────────────────────────┐
// │ ┌───────────────────┐ ┌─────────┐                 │
// │ │ virtual_cluster_1 │ │         │    Exclusive    │
// │ │     Exclusive     │ │         │                 │
// │ │ ┌─────────┐       │ │  job_0  │                 │
// │ │ │  job_1  │       │ │         │                 │
// │ │ │         │       │ │         │                 │
// │ │ └─────────┘       │ │         │                 │
// │ └───────────────────┘ │         │                 │
// │ ┌───────────────────┐ │         │                 │
// │ │ virtual_cluster_2 │ │         │                 │
// │ │       Mixed       │ │         │                 │
// │ │                   │ │         │                 │
// │ │                   │ │         │                 │
// │ └───────────────────┘ └─────────┘                 │
// └───────────────────────────────────────────────────┘
class FailoverTest : public PrimaryClusterTest {
 public:
  using PrimaryClusterTest::PrimaryClusterTest;

  std::shared_ptr<ray::gcs::PrimaryCluster> InitVirtualClusters(size_t node_count,
                                                                size_t template_count) {
    auto primary_cluster = InitPrimaryCluster(node_count, template_count);

    RAY_CHECK_OK(CreateVirtualCluster(primary_cluster,
                                      virtual_cluster_id_1_,
                                      {{template_id_0_, 5}, {template_id_1_, 5}}));

    RAY_CHECK_OK(CreateVirtualCluster(primary_cluster,
                                      virtual_cluster_id_2_,
                                      {{template_id_0_, 5}, {template_id_1_, 5}},
                                      /*divisible=*/false));

    job_cluster_id_0_ = primary_cluster->BuildJobClusterID("job_0");
    RAY_CHECK_OK(primary_cluster->CreateJobCluster(
        job_cluster_id_0_,
        {{template_id_0_, 4}, {template_id_1_, 4}},
        [this](const Status &status, auto data, const auto *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
        }));

    auto virtual_cluster_1 = std::dynamic_pointer_cast<DivisibleCluster>(
        primary_cluster->GetLogicalCluster(virtual_cluster_id_1_));
    RAY_CHECK(virtual_cluster_1 != nullptr);

    job_cluster_id_1_ = virtual_cluster_1->BuildJobClusterID("job_1");
    RAY_CHECK_OK(virtual_cluster_1->CreateJobCluster(
        job_cluster_id_1_,
        {{template_id_0_, 4}, {template_id_1_, 4}},
        [this](const Status &status, auto data, const auto *replica_sets_to_recommend) {
          ASSERT_TRUE(status.ok());
        }));
    primary_cluster_ = primary_cluster;
    return primary_cluster;
  }

  NodeID SelectFirstNode(const std::string &virtual_cluster_id,
                         const std::string &template_id) {
    if (auto virtual_cluster = primary_cluster_->GetVirtualCluster(virtual_cluster_id)) {
      ReplicaSets replica_sets{{template_id, 1}};
      ReplicaInstances replica_instances;
      if (virtual_cluster->LookupUndividedNodeInstances(
              replica_sets, replica_instances, nullptr)) {
        return NodeID::FromHex(
            replica_instances.at(template_id).at(kUndividedClusterId).begin()->first);
      }
    }
    return NodeID::Nil();
  }

  std::string template_id_0_ = "0";
  std::string template_id_1_ = "1";
  std::string job_cluster_id_0_;
  std::string job_cluster_id_1_;
  std::string virtual_cluster_id_1_ = "virtual_cluster_id_1";
  std::string virtual_cluster_id_2_ = "virtual_cluster_id_2";
  std::shared_ptr<ray::gcs::PrimaryCluster> primary_cluster_;
};

TEST_F(FailoverTest, FailoverNormal) {
  size_t node_count = 40;
  size_t template_count = 2;
  auto primary_cluster = InitVirtualClusters(node_count, template_count);
  ASSERT_EQ(virtual_clusters_data_.size(), 4);

  // Mock a gcs_init_data.
  instrumented_io_context io_service;
  gcs::InMemoryGcsTableStorage gcs_table_storage(io_service);
  MockGcsInitData gcs_init_data(gcs_table_storage);
  gcs_init_data.SetNodes(nodes_);
  gcs_init_data.SetVirtualClusters(virtual_clusters_data_);

  // Failover to a new primary cluster.
  auto new_primary_cluster = std::make_shared<PrimaryCluster>(
      [this](auto data, auto callback) {
        callback(Status::OK(), data, nullptr);
        return Status::OK();
      },
      cluster_resource_manager_);
  new_primary_cluster->Initialize(gcs_init_data);

  // Check the visible node instances and replica sets of the primary cluster are the
  // same.
  ASSERT_TRUE(new_primary_cluster->GetVisibleNodeInstances() ==
              primary_cluster->GetVisibleNodeInstances());
  ASSERT_TRUE(new_primary_cluster->GetReplicaSets() == primary_cluster->GetReplicaSets());

  // Check the visible node instances and replica sets of the virtual clusters are the
  // same.
  primary_cluster->ForeachVirtualCluster(
      [this, new_primary_cluster](const auto &virtual_cluster) {
        auto new_virtual_cluster =
            new_primary_cluster->GetVirtualCluster(virtual_cluster->GetID());
        ASSERT_TRUE(new_virtual_cluster != nullptr);
        ASSERT_TRUE(new_virtual_cluster->GetVisibleNodeInstances() ==
                    virtual_cluster->GetVisibleNodeInstances());
        ASSERT_TRUE(new_virtual_cluster->GetReplicaSets() ==
                    virtual_cluster->GetReplicaSets());
      });
}

TEST_F(FailoverTest, FailoverWithDeadNodes) {
  size_t node_count = 40;
  size_t template_count = 2;
  auto primary_cluster = InitVirtualClusters(node_count, template_count);
  ASSERT_EQ(virtual_clusters_data_.size(), 4);

  std::vector<NodeID> dead_node_ids;
  // Mock template_id_1_'s nodes dead in job_cluster_1 and job_cluster_2.
  dead_node_ids.emplace_back(SelectFirstNode(job_cluster_id_0_, template_id_0_));
  dead_node_ids.emplace_back(SelectFirstNode(job_cluster_id_1_, template_id_0_));
  // Mock template_id_1_'s nodes dead in virtual_cluster_1 and virtual_cluster_2.
  dead_node_ids.emplace_back(SelectFirstNode(virtual_cluster_id_1_, template_id_1_));
  dead_node_ids.emplace_back(SelectFirstNode(virtual_cluster_id_2_, template_id_1_));
  // Erase the dead nodes.
  for (const auto &dead_node_id : dead_node_ids) {
    const auto &dead_node = nodes_.at(dead_node_id);
    primary_cluster->OnNodeDead(*dead_node);
    nodes_.erase(dead_node_id);
  }

  auto default_data_flusher = [this](auto data, auto callback) {
    callback(Status::OK(), data, nullptr);
    return Status::OK();
  };

  {
    // Mock a gcs_init_data.
    instrumented_io_context io_service;
    gcs::InMemoryGcsTableStorage gcs_table_storage(io_service);
    MockGcsInitData gcs_init_data(gcs_table_storage);
    gcs_init_data.SetNodes(nodes_);
    gcs_init_data.SetVirtualClusters(virtual_clusters_data_);

    // Failover to a new primary cluster.
    auto new_primary_cluster =
        std::make_shared<PrimaryCluster>(default_data_flusher, cluster_resource_manager_);
    new_primary_cluster->Initialize(gcs_init_data);

    // Check the visible node instances and replica sets of the primary cluster are the
    // same.
    ASSERT_TRUE(new_primary_cluster->GetVisibleNodeInstances() ==
                primary_cluster->GetVisibleNodeInstances());
    ASSERT_TRUE(new_primary_cluster->GetReplicaSets() ==
                primary_cluster->GetReplicaSets());

    // Check the visible node instances and replica sets of the virtual clusters are the
    // same.
    primary_cluster->ForeachVirtualCluster(
        [this, new_primary_cluster](const auto &logical_cluster) {
          auto new_virtual_cluster =
              new_primary_cluster->GetVirtualCluster(logical_cluster->GetID());
          ASSERT_TRUE(new_virtual_cluster != nullptr);
          ASSERT_TRUE(new_virtual_cluster->GetVisibleNodeInstances() ==
                      logical_cluster->GetVisibleNodeInstances());
          ASSERT_TRUE(new_virtual_cluster->GetReplicaSets() ==
                      logical_cluster->GetReplicaSets());
        });
  }

  {
    auto virtual_cluster_1 = std::dynamic_pointer_cast<DivisibleCluster>(
        primary_cluster->GetVirtualCluster(virtual_cluster_id_1_));
    ASSERT_TRUE(virtual_cluster_1 != nullptr);

    // Assume that the dead nodes in job cluster 1 is replaced by a new alive one from
    // primary cluster.
    auto node_instance_replenish_callback = [primary_cluster](auto node_instance) {
      return primary_cluster->ReplenishUndividedNodeInstance(std::move(node_instance));
    };
    ASSERT_TRUE(
        virtual_cluster_1->ReplenishNodeInstances(node_instance_replenish_callback));
    async_data_flusher_(virtual_cluster_1->ToProto(), nullptr);

    // Mock a gcs_init_data.
    instrumented_io_context io_service;
    gcs::InMemoryGcsTableStorage gcs_table_storage(io_service);
    MockGcsInitData gcs_init_data(gcs_table_storage);
    gcs_init_data.SetNodes(nodes_);
    gcs_init_data.SetVirtualClusters(virtual_clusters_data_);

    // Failover to a new primary cluster.
    auto new_primary_cluster =
        std::make_shared<PrimaryCluster>(default_data_flusher, cluster_resource_manager_);
    new_primary_cluster->Initialize(gcs_init_data);

    // Check the visible node instances and replica sets of the new primary cluster are
    // less than the old primary cluster.
    ASSERT_TRUE(new_primary_cluster->GetVisibleNodeInstances() <=
                primary_cluster->GetVisibleNodeInstances());
    ASSERT_TRUE(new_primary_cluster->GetReplicaSets() <=
                primary_cluster->GetReplicaSets());

    // Check the visible node instances and replica sets of the virtual clusters are the
    // same.
    primary_cluster->ForeachVirtualCluster(
        [this, new_primary_cluster](const auto &virtual_cluster) {
          auto new_virtual_cluster =
              new_primary_cluster->GetVirtualCluster(virtual_cluster->GetID());
          ASSERT_TRUE(new_virtual_cluster != nullptr);
          ASSERT_TRUE(new_virtual_cluster->GetVisibleNodeInstances() ==
                      virtual_cluster->GetVisibleNodeInstances());
          ASSERT_TRUE(new_virtual_cluster->GetReplicaSets() ==
                      virtual_cluster->GetReplicaSets());
        });
  }
}

TEST_F(FailoverTest, OnlyFlushJobClusters) {
  size_t node_count = 40;
  size_t template_count = 2;
  auto primary_cluster = InitVirtualClusters(node_count, template_count);
  ASSERT_EQ(virtual_clusters_data_.size(), 4);

  std::vector<NodeID> dead_node_ids;
  // Mock template_id_1_'s nodes dead in job_cluster_1 and job_cluster_2.
  dead_node_ids.emplace_back(SelectFirstNode(job_cluster_id_0_, template_id_0_));
  dead_node_ids.emplace_back(SelectFirstNode(job_cluster_id_1_, template_id_0_));
  // Mock template_id_1_'s nodes dead in virtual_cluster_1 and virtual_cluster_2.
  dead_node_ids.emplace_back(SelectFirstNode(virtual_cluster_id_1_, template_id_1_));
  dead_node_ids.emplace_back(SelectFirstNode(virtual_cluster_id_2_, template_id_1_));
  // Erase the dead nodes.
  for (const auto &dead_node_id : dead_node_ids) {
    const auto &dead_node = nodes_.at(dead_node_id);
    primary_cluster->OnNodeDead(*dead_node);
    nodes_.erase(dead_node_id);
  }

  auto default_data_flusher = [this](auto data, auto callback) {
    callback(Status::OK(), data, nullptr);
    return Status::OK();
  };

  {
    auto virtual_cluster_1 = std::dynamic_pointer_cast<DivisibleCluster>(
        primary_cluster->GetVirtualCluster(virtual_cluster_id_1_));
    ASSERT_TRUE(virtual_cluster_1 != nullptr);

    // Assume that the dead nodes in job cluster 1 is replaced by a new alive one from
    // primary cluster.
    auto node_instance_replenish_callback = [primary_cluster](auto node_instance) {
      return primary_cluster->ReplenishUndividedNodeInstance(std::move(node_instance));
    };
    ASSERT_TRUE(
        virtual_cluster_1->ReplenishNodeInstances(node_instance_replenish_callback));
    // async_data_flusher_(virtual_cluster_1->ToProto(), nullptr);

    // Mock a gcs_init_data.
    instrumented_io_context io_service;
    gcs::InMemoryGcsTableStorage gcs_table_storage(io_service);
    MockGcsInitData gcs_init_data(gcs_table_storage);
    gcs_init_data.SetNodes(nodes_);
    gcs_init_data.SetVirtualClusters(virtual_clusters_data_);

    RAY_LOG(INFO) << "Initialize";
    // Failover to a new primary cluster.
    auto new_primary_cluster =
        std::make_shared<PrimaryCluster>(default_data_flusher, cluster_resource_manager_);
    new_primary_cluster->Initialize(gcs_init_data);

    auto new_job_cluster_1 = new_primary_cluster->GetVirtualCluster(job_cluster_id_1_);
    ASSERT_TRUE(new_job_cluster_1 != nullptr);
    // The dead node dead_node_ids[1] in job cluster 1 is replenished and the job cluster
    // 1 is flushed.
    ASSERT_FALSE(new_job_cluster_1->ContainsNodeInstance(dead_node_ids[1].Hex()));

    auto new_virtual_cluster_1 =
        new_primary_cluster->GetVirtualCluster(virtual_cluster_id_1_);
    ASSERT_TRUE(new_virtual_cluster_1 != nullptr);
    // The dead node dead_node_ids[1] is repaired in virtual cluster 1 when fo.
    ASSERT_FALSE(new_virtual_cluster_1->ContainsNodeInstance(dead_node_ids[1].Hex()));
    // The dead node dead_node_ids[2] is still in the virtual cluster 1 as it is not
    // flushed.
    ASSERT_TRUE(new_virtual_cluster_1->ContainsNodeInstance(dead_node_ids[2].Hex()));

    // Check all the node instances in job cluster 1 are in the new virtual cluster 1.
    new_job_cluster_1->ForeachNodeInstance(
        [new_virtual_cluster_1](const auto &node_instance) {
          ASSERT_TRUE(new_virtual_cluster_1->ContainsNodeInstance(
              node_instance->node_instance_id()));
        });
  }
}

}  // namespace gcs
}  // namespace ray
