// Copyright 2026 The Ray Authors.
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

// Phase 2 smoke test for the RocksDB Bazel integration (REP-64 POC).
//
// This is the smallest possible exercise of the new dependency: open a
// RocksDB at a temp path, put one key, read it back, verify the value.
// Exists to prove that:
//
//   * librocksdb.a links into a Ray-built binary
//   * basic Open / Put / Get work end-to-end
//   * the build is clean under --config=asan-clang
//
// It is *not* a StoreClient implementation. The real RocksDbStoreClient
// arrives in Phase 3.

#include <filesystem>
#include <random>
#include <string>

#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace fs = std::filesystem;

namespace {

fs::path UniqueTempDir() {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  auto p =
      fs::temp_directory_path() / ("rep64-poc-rocksdb-smoke-" + std::to_string(rng()));
  fs::create_directories(p);
  return p;
}

}  // namespace

TEST(RocksDbSmoke, OpenPutGet) {
  const fs::path db_path = UniqueTempDir();

  rocksdb::Options options;
  options.create_if_missing = true;
  // Match the durability contract Ray's GCS will rely on:
  // every write is fsynced before ack.
  rocksdb::WriteOptions wo;
  wo.sync = true;

  rocksdb::DB *db = nullptr;
  ASSERT_TRUE(rocksdb::DB::Open(options, db_path.string(), &db).ok());
  ASSERT_NE(db, nullptr);

  const std::string key = "rep64";
  const std::string value = "hello-from-rocksdb";

  ASSERT_TRUE(db->Put(wo, key, value).ok());

  std::string read_back;
  ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), key, &read_back).ok());
  EXPECT_EQ(read_back, value);

  delete db;
  fs::remove_all(db_path);
}
