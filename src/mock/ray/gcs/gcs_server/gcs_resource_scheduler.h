// Copyright  The Ray Authors.
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

class MockNodeScorer : public NodeScorer {
 public:
  MOCK_METHOD(double,
              Score,
              (const ResourceSet &required_resources,
               const SchedulingResources &node_resources),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockLeastResourceScorer : public LeastResourceScorer {
 public:
  MOCK_METHOD(double,
              Score,
              (const ResourceSet &required_resources,
               const SchedulingResources &node_resources),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsResourceScheduler : public GcsResourceScheduler {
 public:
};

}  // namespace gcs
}  // namespace ray
