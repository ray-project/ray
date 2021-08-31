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
namespace raylet {

class Mockpair_hash : public pair_hash {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockBundleTransactionState : public BundleTransactionState {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockPlacementGroupResourceManager : public PlacementGroupResourceManager {
 public:
  MOCK_METHOD(bool, PrepareBundle, (const BundleSpecification &bundle_spec), (override));
  MOCK_METHOD(void, CommitBundle, (const BundleSpecification &bundle_spec), (override));
  MOCK_METHOD(void, ReturnBundle, (const BundleSpecification &bundle_spec), (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockNewPlacementGroupResourceManager : public NewPlacementGroupResourceManager {
 public:
};

}  // namespace raylet
}  // namespace ray
