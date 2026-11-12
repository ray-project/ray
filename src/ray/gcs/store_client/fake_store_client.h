// Copyright The Ray Authors.
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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {
namespace gcs {

// Hand-written fake for the StoreClient interface. Each async method records its
// call arguments in a public vector and stores the last callback it received (in
// a unique_ptr, since Postable is not default-constructible) so tests can drive
// completions manually via `std::move(*fake.last_async_put_callback).Post(...)`.
class FakeStoreClient : public StoreClient {
 public:
  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override {
    async_put_calls.push_back({table_name, key});
    last_async_put_callback = std::make_unique<Postable<void(bool)>>(std::move(callback));
  }

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<rpc::OptionalItemCallback<std::string>> callback) override {
    async_get_calls.push_back({table_name, key});
    last_async_get_callback =
        std::make_unique<ToPostable<rpc::OptionalItemCallback<std::string>>>(
            std::move(callback));
  }

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override {
    async_get_all_calls.push_back(table_name);
    last_async_get_all_callback =
        std::make_unique<Postable<void(absl::flat_hash_map<std::string, std::string>)>>(
            std::move(callback));
  }

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override {
    async_multi_get_calls.push_back(table_name);
    last_async_multi_get_callback =
        std::make_unique<Postable<void(absl::flat_hash_map<std::string, std::string>)>>(
            std::move(callback));
  }

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override {
    async_delete_calls.push_back({table_name, key});
    last_async_delete_callback =
        std::make_unique<Postable<void(bool)>>(std::move(callback));
  }

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override {
    async_batch_delete_calls.push_back(table_name);
    last_async_batch_delete_callback =
        std::make_unique<Postable<void(int64_t)>>(std::move(callback));
  }

  void AsyncGetNextJobID(Postable<void(int)> callback) override {
    async_get_next_job_id_calls++;
    last_async_get_next_job_id_callback =
        std::make_unique<Postable<void(int)>>(std::move(callback));
  }

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override {
    async_get_keys_calls.push_back({table_name, prefix});
    last_async_get_keys_callback =
        std::make_unique<Postable<void(std::vector<std::string>)>>(std::move(callback));
  }

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override {
    async_exists_calls.push_back({table_name, key});
    last_async_exists_callback =
        std::make_unique<Postable<void(bool)>>(std::move(callback));
  }

  // Recorded calls. For methods with (table_name, key)-ish args a pair is stored.
  std::vector<std::pair<std::string, std::string>> async_put_calls;
  std::vector<std::pair<std::string, std::string>> async_get_calls;
  std::vector<std::string> async_get_all_calls;
  std::vector<std::string> async_multi_get_calls;
  std::vector<std::pair<std::string, std::string>> async_delete_calls;
  std::vector<std::string> async_batch_delete_calls;
  int async_get_next_job_id_calls = 0;
  std::vector<std::pair<std::string, std::string>> async_get_keys_calls;
  std::vector<std::pair<std::string, std::string>> async_exists_calls;

  // Last callback received per method (nullptr until the method is invoked).
  std::unique_ptr<Postable<void(bool)>> last_async_put_callback;
  std::unique_ptr<ToPostable<rpc::OptionalItemCallback<std::string>>>
      last_async_get_callback;
  std::unique_ptr<Postable<void(absl::flat_hash_map<std::string, std::string>)>>
      last_async_get_all_callback;
  std::unique_ptr<Postable<void(absl::flat_hash_map<std::string, std::string>)>>
      last_async_multi_get_callback;
  std::unique_ptr<Postable<void(bool)>> last_async_delete_callback;
  std::unique_ptr<Postable<void(int64_t)>> last_async_batch_delete_callback;
  std::unique_ptr<Postable<void(int)>> last_async_get_next_job_id_callback;
  std::unique_ptr<Postable<void(std::vector<std::string>)>> last_async_get_keys_callback;
  std::unique_ptr<Postable<void(bool)>> last_async_exists_callback;
};

}  // namespace gcs
}  // namespace ray
