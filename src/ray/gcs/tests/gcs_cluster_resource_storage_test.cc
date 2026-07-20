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

#include "ray/gcs/gcs_cluster_resource_storage.h"

#include <gtest/gtest.h>

#include <memory>

#include "ray/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {

class GcsClusterResourceStorageTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_table_storage_ = std::make_shared<gcs::GcsTableStorage>(
        std::make_unique<gcs::InMemoryStoreClient>());
    cluster_resource_storage_ = std::make_unique<gcs::ClusterResourceStorage>(
        gcs_table_storage_.get(), io_service_);
  }

  void TearDown() override { io_service_.stop(); }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<gcs::ClusterResourceStorage> cluster_resource_storage_;
};

TEST_F(GcsClusterResourceStorageTest, TestBasicStorage) {
  auto node_resources_table = gcs_table_storage_.get()->NodeResourcesTable();
  auto node_id = NodeID::FromRandom();
  auto resource_data = rpc::ResourcesData();
  resource_data.set_node_id(node_id.Binary());

  auto on_start =
      [this, node_resources_table, node_id, resource_data](
          const absl::flat_hash_map<NodeID, rpc::ResourcesData> &&result) mutable {
        ASSERT_EQ(0, result.size());
        cluster_resource_storage_.get()->Put(node_id, resource_data);

        auto on_put =
            [this, node_resources_table, node_id](
                const absl::flat_hash_map<NodeID, rpc::ResourcesData> &&result) mutable {
              ASSERT_EQ(1, result.size());
              auto it = result.find(node_id);
              ASSERT_NE(it, result.end());

              cluster_resource_storage_.get()->Delete(node_id);

              auto on_del = [node_resources_table](
                                const absl::flat_hash_map<NodeID, rpc::ResourcesData>
                                    &&result) mutable { ASSERT_EQ(0, result.size()); };

              node_resources_table.GetAll({std::move(on_del), io_service_});
              io_service_.run_one();
            };

        node_resources_table.GetAll({std::move(on_put), io_service_});
        io_service_.run_one();
      };

  node_resources_table.GetAll({std::move(on_start), io_service_});
  io_service_.run_one();
}

}  // namespace ray
