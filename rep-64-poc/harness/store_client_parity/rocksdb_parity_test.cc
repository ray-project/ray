// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

// Phase 6 StoreClientTestBase parity (REP-64 POC).
//
// Subclasses StoreClientTestBase with RocksDbStoreClient. The base
// fixture exercises every StoreClient method (Put/Get/Delete/GetAll/
// MultiGet/GetKeys/BatchDelete/Exists) the same way
// `in_memory_store_client_test.cc` and `redis_store_client_test.cc`
// already do for their backends. If RocksDB really fits the StoreClient
// interface, this test passes unmodified.

#include <filesystem>
#include <random>
#include <string>

#include "gtest/gtest.h"
#include "ray/gcs/store_client/rocksdb_store_client.h"
#include "ray/gcs/store_client/tests/store_client_test_base.h"

namespace fs = std::filesystem;

namespace ray {
namespace gcs {

namespace {

fs::path UniqueDbPath() {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  auto p = fs::temp_directory_path() /
           ("rep64-phase6-parity-" + std::to_string(rng()));
  fs::create_directories(p);
  return p;
}

}  // namespace

class RocksDbStoreClientParityTest : public StoreClientTestBase {
 public:
  void InitStoreClient() override {
    db_path_ = UniqueDbPath();
    store_client_ = std::make_shared<RocksDbStoreClient>(
        *(io_service_pool_->Get()), db_path_.string(),
        /*expected_cluster_id=*/"");
  }

  void TearDown() override {
    StoreClientTestBase::TearDown();
    store_client_.reset();
    fs::remove_all(db_path_);
  }

 private:
  fs::path db_path_;
};

TEST_F(RocksDbStoreClientParityTest, AsyncPutAndAsyncGetTest) {
  TestAsyncPutAndAsyncGet();
}

TEST_F(RocksDbStoreClientParityTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
