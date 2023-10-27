// Copyright 2023 The Ray Authors.
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

class MockWork : public Work {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockLocalTaskManager : public LocalTaskManager {
 public:
  MOCK_METHOD(void, PopulateResourceUsage, (rpc::ResourcesData & data), (const));
};

}  // namespace raylet
}  // namespace ray
