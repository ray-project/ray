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

class MockInternalKVInterface : public ray::gcs::InternalKVInterface {
 public:
  MockInternalKVInterface() {}

  MOCK_METHOD(void,
              Get,
              (const std::string &ns,
               const std::string &key,
               std::function<void(std::optional<std::string>)> callback),
              (override));
  MOCK_METHOD(void,
              Put,
              (const std::string &ns,
               const std::string &key,
               const std::string &value,
               bool overwrite,
               std::function<void(bool)> callback),
              (override));
  MOCK_METHOD(void,
              Del,
              (const std::string &ns,
               const std::string &key,
               bool del_by_prefix,
               std::function<void(int64_t)> callback),
              (override));
  MOCK_METHOD(void,
              Exists,
              (const std::string &ns,
               const std::string &key,
               std::function<void(bool)> callback),
              (override));
  MOCK_METHOD(void,
              Keys,
              (const std::string &ns,
               const std::string &prefix,
               std::function<void(std::vector<std::string>)> callback),
              (override));
  MOCK_METHOD(instrumented_io_context &, GetEventLoop, (), (override));
};

}  // namespace gcs
}  // namespace ray
