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

#include "gtest/gtest.h"

#include "ray/gcs/gcs_server/gcs_node_resource_manager.h"

namespace ray {

class GcsNodeResourceManagerTest : public ::testing::Test {};

TEST_F(GcsNodeResourceManagerTest, TestApi) {
  gcs::GcsNodeResourceManager node_resource_manager;

  // Test UpdateNodeResources.
  ClientID node_id = ClientID::FromRandom();
  gcs::NodeInfoAccessor::ResourceMap resources;
  std::string attr1("attr1");
  double resource_capacity = 6.0;
  auto resource_table_data = std::make_shared<rpc::ResourceTableData>();
  resource_table_data->set_resource_capacity(resource_capacity);
  resources[attr1] = resource_table_data;
  node_resource_manager.UpdateNodeResources(node_id, resources);

  // Test GetNodeResources.
  std::vector<std::string> resource_names;
  resource_names.push_back(attr1);
  auto get_result = node_resource_manager.GetNodeResources(node_id, resource_names);
  ASSERT_EQ(get_result->size(), 1);

  // Test DeleteNodeResources.
  node_resource_manager.DeleteNodeResources(node_id, resource_names);
  ASSERT_TRUE(node_resource_manager.GetNodeResources(node_id, resource_names)->empty());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
