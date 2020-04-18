// Copyright 2017 The Ray Authors.
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

#ifndef RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H

#include <memory>
#include <unordered_set>
#include "absl/synchronization/mutex.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

class InMemoryTable {
  std::unordered_map<std::string, std::string> records GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

/// \class InMemoryStoreClient
///
/// This class is thread safe.
template <typename Key, typename Data, typename IndexKey>
class InMemoryStoreClient : public StoreClient<Key, Data, IndexKey> {
 public:
  InMemoryStoreClient() {
    // Run the event loop.
    // Using boost::asio::io_context::work to avoid ending the event loop when
    // there are no events to handle.
    boost::asio::io_context::work worker(main_service_);
    main_service_.run();
  }

  virtual ~InMemoryStoreClient() {
    // Stop the event loop.
    main_service_.stop();
  }

  Status AsyncPut(const std::string &table_name, const Key &key, const Data &data,
                  const StatusCallback &callback) override;

  Status AsyncPutWithIndex(const std::string &table_name, const Key &key,
                           const IndexKey &index_key, const Data &data,
                           const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name, const Key &key,
                  const OptionalItemCallback<Data> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const SegmentedCallback<std::pair<Key, Data>> &callback) override;

  Status AsyncDelete(const std::string &table_name, const Key &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name, const IndexKey &index_key,
                            const StatusCallback &callback) override;

 private:
  std::mutex mutex_;
  const std::unordered_map<std::string, std::shared_ptr<InMemoryTable>> tables_;
  boost::asio::io_context main_service_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H
