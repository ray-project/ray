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

class MockInMemoryStoreClient : public InMemoryStoreClient {
 public:
  MOCK_METHOD(void,
              AsyncPut,
              (const std::string &table_name,
               const std::string &key,
               std::string data,
               bool overwrite,
               Postable<void(bool)> callback),
              (override));

  MOCK_METHOD(void,
              AsyncGet,
              (const std::string &table_name,
               const std::string &key,
               ToPostable<OptionalItemCallback<std::string>> callback),
              (override));

  MOCK_METHOD(void,
              AsyncGetAll,
              (const std::string &table_name,
               Postable<void(absl::flat_hash_map<std::string, std::string>)> callback),
              (override));

  MOCK_METHOD(void,
              AsyncMultiGet,
              (const std::string &table_name,
               const std::vector<std::string> &keys,
               Postable<void(absl::flat_hash_map<std::string, std::string>)> callback),
              (override));

  MOCK_METHOD(void,
              AsyncDelete,
              (const std::string &table_name,
               const std::string &key,
               Postable<void(bool)> callback),
              (override));

  MOCK_METHOD(void,
              AsyncBatchDelete,
              (const std::string &table_name,
               const std::vector<std::string> &keys,
               Postable<void(int64_t)> callback),
              (override));

  MOCK_METHOD(void,
              AsyncGetKeys,
              (const std::string &table_name,
               const std::string &prefix,
               Postable<void(std::vector<std::string>)> callback),
              (override));

  MOCK_METHOD(void,
              AsyncExists,
              (const std::string &table_name,
               const std::string &key,
               Postable<void(bool)> callback),
              (override));

  MOCK_METHOD(void, AsyncGetNextJobID, (Postable<void(int)> callback), (override));
};

}  // namespace gcs
}  // namespace ray
