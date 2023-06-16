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

#include "ray/raylet/scheduling/node_labels_index.h"

#include "gtest/gtest.h"
#include "ray/common/id.h"

using json = nlohmann::json;
namespace ray {

scheduling::NodeID node_id_1(NodeID::FromRandom().Binary());
scheduling::NodeID node_id_2(NodeID::FromRandom().Binary());

TEST(NodeLabelsIndexTest, TestDebugString) {
  NodeLabelsIndex node_labels_index;
  ASSERT_EQ(node_labels_index.DebugString(), "{\"label_to_nodes\":null}");

  node_labels_index.AddLabels(node_id_1, {{"node_ip", "1.1.1.1"}, {"gpu_type", "A100"}});
  auto json_object = json::parse(node_labels_index.DebugString());
  ASSERT_EQ(json_object.at("label_to_nodes").at("gpu_type").at("A100")[0],
            node_id_1.ToInt());
}

TEST(NodeLabelsIndexTest, TestBasic) {
  NodeLabelsIndex node_labels_index;
  auto empty_debug_str = node_labels_index.DebugString();
  ASSERT_TRUE(node_labels_index.GetNodesByKey("node_ip").empty());
  ASSERT_TRUE(
      node_labels_index.GetNodesByKeyAndValue("node_ip", {"1.1.1.1", "2.2.2.2"}).empty());

  // Add node_id_1
  node_labels_index.AddLabels(node_id_1, {{"node_ip", "1.1.1.1"}, {"gpu_type", "A100"}});
  auto result = node_labels_index.GetNodesByKey("node_ip");
  ASSERT_EQ(result, absl::flat_hash_set<scheduling::NodeID>{node_id_1});
  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"1.1.1.1"});
  ASSERT_EQ(result, absl::flat_hash_set<scheduling::NodeID>{node_id_1});

  // Add node_id_2
  node_labels_index.AddLabels(node_id_2, {{"node_ip", "2.2.2.2"}, {"gpu_type", "A100"}});
  result = node_labels_index.GetNodesByKey("node_ip");
  ASSERT_EQ(result, (absl::flat_hash_set<scheduling::NodeID>{node_id_1, node_id_2}));

  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"1.1.1.1"});
  ASSERT_EQ(result, absl::flat_hash_set<scheduling::NodeID>{node_id_1});
  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"1.1.1.1", "2.2.2.2"});
  ASSERT_EQ(result, (absl::flat_hash_set<scheduling::NodeID>{node_id_1, node_id_2}));
  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"2.2.2.2", "3.3.3.3"});
  ASSERT_EQ(result, absl::flat_hash_set<scheduling::NodeID>{node_id_2});

  result = node_labels_index.GetNodesByKeyAndValue("gpu_type", {"A100"});
  ASSERT_EQ(result, (absl::flat_hash_set<scheduling::NodeID>{node_id_1, node_id_2}));

  // Don't exist key
  result = node_labels_index.GetNodesByKey("other_key");
  ASSERT_TRUE(result.empty());
  result = node_labels_index.GetNodesByKeyAndValue("other_key", {"1.1.1.1", "2.2.2.2"});
  ASSERT_TRUE(result.empty());

  // Remove node_id_1
  node_labels_index.RemoveLabels(node_id_1);
  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"1.1.1.1"});
  ASSERT_TRUE(result.empty());
  result = node_labels_index.GetNodesByKeyAndValue("gpu_type", {"A100"});
  ASSERT_EQ(result, absl::flat_hash_set<scheduling::NodeID>{node_id_2});
  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"2.2.2.2"});
  ASSERT_EQ(result, absl::flat_hash_set<scheduling::NodeID>{node_id_2});

  // Remove node_id_2
  node_labels_index.RemoveLabels(node_id_2);
  result = node_labels_index.GetNodesByKeyAndValue("node_ip", {"2.2.2.2"});
  ASSERT_TRUE(result.empty());
  result = node_labels_index.GetNodesByKeyAndValue("gpu_type", {"A100"});
  ASSERT_TRUE(result.empty());

  ASSERT_EQ(node_labels_index.DebugString(), empty_debug_str);
}
}  // namespace ray
