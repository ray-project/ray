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

class MockTaskDependencyManagerInterface : public TaskDependencyManagerInterface {
 public:
  MOCK_METHOD(bool,
              RequestTaskDependencies,
              (const TaskID &task_id,
               const std::vector<rpc::ObjectReference> &required_objects),
              (override));
  MOCK_METHOD(void, RemoveTaskDependencies, (const TaskID &task_id), (override));
  MOCK_METHOD(bool, TaskDependenciesBlocked, (const TaskID &task_id), (const, override));
  MOCK_METHOD(bool, CheckObjectLocal, (const ObjectID &object_id), (const, override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockDependencyManager : public DependencyManager {
 public:
};

}  // namespace raylet
}  // namespace ray
