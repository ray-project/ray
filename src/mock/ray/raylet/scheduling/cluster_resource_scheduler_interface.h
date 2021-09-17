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

namespace ray {

class MockClusterResourceSchedulerInterface : public ClusterResourceSchedulerInterface {
 public:
  MOCK_METHOD(bool, RemoveNode, (const std::string &node_id_string), (override));
  MOCK_METHOD(bool, UpdateNode,
              (const std::string &node_id_string,
               const rpc::ResourcesData &resource_data),
              (override));
  MOCK_METHOD(void, UpdateResourceCapacity,
              (const std::string &node_id_string, const std::string &resource_name,
               double resource_total),
              (override));
  MOCK_METHOD(void, DeleteResource,
              (const std::string &node_id_string, const std::string &resource_name),
              (override));
  MOCK_METHOD(void, UpdateLastResourceUsage,
              (const std::shared_ptr<SchedulingResources> gcs_resources), (override));
  MOCK_METHOD(void, FillResourceUsage, (rpc::ResourcesData & data), (override));
  MOCK_METHOD(double, GetLocalAvailableCpus, (), (const, override));
  MOCK_METHOD(ray::gcs::NodeResourceInfoAccessor::ResourceMap, GetResourceTotals, (),
              (const, override));
  MOCK_METHOD(std::string, GetLocalResourceViewString, (), (const, override));
};

}  // namespace ray
