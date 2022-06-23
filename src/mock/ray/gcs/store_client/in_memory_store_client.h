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
  MOCK_METHOD(Status,
              AsyncPut,
              (const std::string &table_name,
               const std::string &key,
               const std::string &data,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncPutWithIndex,
              (const std::string &table_name,
               const std::string &key,
               const std::string &index_key,
               const std::string &data,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncGet,
              (const std::string &table_name,
               const std::string &key,
               const OptionalItemCallback<std::string> &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncGetByIndex,
              (const std::string &table_name,
               const std::string &index_key,
               (const MapCallback<std::string, std::string> &callback)),
              (override));
  MOCK_METHOD(Status,
              AsyncGetAll,
              (const std::string &table_name,
               (const MapCallback<std::string, std::string> &callback)),
              (override));
  MOCK_METHOD(Status,
              AsyncDelete,
              (const std::string &table_name,
               const std::string &key,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncDeleteWithIndex,
              (const std::string &table_name,
               const std::string &key,
               const std::string &index_key,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncBatchDelete,
              (const std::string &table_name,
               const std::vector<std::string> &keys,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncBatchDeleteWithIndex,
              (const std::string &table_name,
               const std::vector<std::string> &keys,
               const std::vector<std::string> &index_keys,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncDeleteByIndex,
              (const std::string &table_name,
               const std::string &index_key,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(int, GetNextJobID, (), (override));
};

}  // namespace gcs
}  // namespace ray
