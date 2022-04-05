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
namespace gcs {

class Mockpair_hash : public pair_hash {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsPlacementGroupSchedulerInterface
    : public GcsPlacementGroupSchedulerInterface {
 public:
  MOCK_METHOD(void,
              ScheduleUnplacedBundles,
              (std::shared_ptr<GcsPlacementGroup> placement_group,
               PGSchedulingFailureCallback failure_callback,
               PGSchedulingSuccessfulCallback success_callback),
              (override));
  MOCK_METHOD((absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>),
              GetBundlesOnNode,
              (const NodeID &node_id),
              (override));
  MOCK_METHOD(void,
              DestroyPlacementGroupBundleResourcesIfExists,
              (const PlacementGroupID &placement_group_id),
              (override));
  MOCK_METHOD(void,
              MarkScheduleCancelled,
              (const PlacementGroupID &placement_group_id),
              (override));
  MOCK_METHOD(
      void,
      ReleaseUnusedBundles,
      ((const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles)),
      (override));
  MOCK_METHOD(void,
              Initialize,
              ((const absl::flat_hash_map<
                  PlacementGroupID,
                  std::vector<std::shared_ptr<BundleSpecification>>> &group_to_bundles)),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockLeaseStatusTracker : public LeaseStatusTracker {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockBundleLocationIndex : public BundleLocationIndex {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsPlacementGroupScheduler : public GcsPlacementGroupScheduler {
 public:
  MOCK_METHOD(void,
              ScheduleUnplacedBundles,
              (std::shared_ptr<GcsPlacementGroup> placement_group,
               PGSchedulingFailureCallback failure_handler,
               PGSchedulingSuccessfulCallback success_handler),
              (override));
  MOCK_METHOD(void,
              DestroyPlacementGroupBundleResourcesIfExists,
              (const PlacementGroupID &placement_group_id),
              (override));
  MOCK_METHOD(void,
              MarkScheduleCancelled,
              (const PlacementGroupID &placement_group_id),
              (override));
  MOCK_METHOD((absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>),
              GetBundlesOnNode,
              (const NodeID &node_id),
              (override));
  MOCK_METHOD(
      void,
      ReleaseUnusedBundles,
      ((const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles)),
      (override));
};

}  // namespace gcs
}  // namespace ray
