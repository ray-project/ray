// Copyright 2025 The Ray Authors.
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

#include "ray/core_worker/rdt_memory_stats.h"

#include <atomic>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace ray {
namespace core {

TEST(RdtMemoryStatsTest, SetReplaceDelete) {
  RdtMemoryStats stats;
  ObjectID object_id = ObjectID::FromRandom();

  stats.Set(object_id, {{"cpu", 4000}}, 1, 0);
  rpc::CoreWorkerStats s;
  stats.AddToStats(&s, -1);
  ASSERT_EQ(s.rdt_objects_total(), 1);
  ASSERT_EQ(s.rdt_objects_size(), 1);
  EXPECT_EQ(s.rdt_objects(0).object_id(), object_id.Binary());
  EXPECT_EQ(s.rdt_objects(0).num_copies(), 1);
  EXPECT_EQ(s.rdt_objects(0).num_errors(), 0);
  EXPECT_EQ(s.rdt_objects(0).tensor_bytes().at("cpu"), 4000);

  // Replace the snapshot for the same object id.
  stats.Set(object_id, {{"cuda:0", 8000}}, 2, 1);
  rpc::CoreWorkerStats s2;
  stats.AddToStats(&s2, -1);
  ASSERT_EQ(s2.rdt_objects_total(), 1);
  ASSERT_EQ(s2.rdt_objects_size(), 1);
  EXPECT_EQ(s2.rdt_objects(0).num_copies(), 2);
  EXPECT_EQ(s2.rdt_objects(0).num_errors(), 1);
  EXPECT_EQ(s2.rdt_objects(0).tensor_bytes().at("cuda:0"), 8000);
  EXPECT_EQ(s2.rdt_objects(0).tensor_bytes().count("cpu"), 0);

  // Erase when there are no copies and no errors.
  stats.Set(object_id, {}, 0, 0);
  rpc::CoreWorkerStats s3;
  stats.AddToStats(&s3, -1);
  EXPECT_EQ(s3.rdt_objects_total(), 0);
  EXPECT_EQ(s3.rdt_objects_size(), 0);
}

TEST(RdtMemoryStatsTest, IndependentObjectIds) {
  RdtMemoryStats stats;
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();

  stats.Set(id1, {{"cpu", 100}}, 1, 0);
  stats.Set(id2, {{"cpu", 200}}, 1, 0);

  rpc::CoreWorkerStats s;
  stats.AddToStats(&s, -1);
  ASSERT_EQ(s.rdt_objects_total(), 2);
  ASSERT_EQ(s.rdt_objects_size(), 2);

  bool saw_id1 = false;
  bool saw_id2 = false;
  for (const auto &info : s.rdt_objects()) {
    if (info.object_id() == id1.Binary()) {
      saw_id1 = true;
      EXPECT_EQ(info.tensor_bytes().at("cpu"), 100);
    } else if (info.object_id() == id2.Binary()) {
      saw_id2 = true;
      EXPECT_EQ(info.tensor_bytes().at("cpu"), 200);
    }
  }
  EXPECT_TRUE(saw_id1);
  EXPECT_TRUE(saw_id2);
}

TEST(RdtMemoryStatsTest, ZeroAndUnknownSize) {
  RdtMemoryStats stats;
  ObjectID object_id = ObjectID::FromRandom();

  // -1 means unknown/incomplete and must be preserved; 0 is a valid empty size.
  stats.Set(object_id, {{"cuda:0", -1}, {"cpu", 0}}, 1, 0);

  rpc::CoreWorkerStats s;
  stats.AddToStats(&s, -1);
  ASSERT_EQ(s.rdt_objects_size(), 1);
  const auto &bytes = s.rdt_objects(0).tensor_bytes();
  ASSERT_EQ(bytes.at("cuda:0"), -1);
  ASSERT_EQ(bytes.at("cpu"), 0);
}

TEST(RdtMemoryStatsTest, ErrorsOnlyRecord) {
  RdtMemoryStats stats;
  ObjectID object_id = ObjectID::FromRandom();

  // num_copies == 0 but num_errors > 0: the record must be retained.
  stats.Set(object_id, {}, 0, 2);

  rpc::CoreWorkerStats s;
  stats.AddToStats(&s, -1);
  ASSERT_EQ(s.rdt_objects_total(), 1);
  ASSERT_EQ(s.rdt_objects_size(), 1);
  EXPECT_EQ(s.rdt_objects(0).num_copies(), 0);
  EXPECT_EQ(s.rdt_objects(0).num_errors(), 2);
  EXPECT_TRUE(s.rdt_objects(0).tensor_bytes().empty());
}

TEST(RdtMemoryStatsTest, ObjectRefInfoUntouched) {
  RdtMemoryStats stats;
  ObjectID object_id = ObjectID::FromRandom();
  stats.Set(object_id, {{"cpu", 4000}}, 1, 0);

  rpc::CoreWorkerStats s;
  s.add_object_refs();  // Pre-existing object ref row must not be modified.
  stats.AddToStats(&s, -1);

  EXPECT_EQ(s.object_refs_size(), 1);
  EXPECT_EQ(s.rdt_objects_total(), 1);
  EXPECT_EQ(s.rdt_objects_size(), 1);
}

TEST(RdtMemoryStatsTest, LimitsAndTotal) {
  RdtMemoryStats stats;
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();
  stats.Set(id1, {{"cpu", 1}}, 1, 0);
  stats.Set(id2, {{"cpu", 2}}, 1, 0);
  stats.Set(id3, {{"cpu", 3}}, 1, 0);

  // limit == 0: no rows appended, but total is still the full count.
  rpc::CoreWorkerStats s0;
  stats.AddToStats(&s0, 0);
  EXPECT_EQ(s0.rdt_objects_total(), 3);
  EXPECT_EQ(s0.rdt_objects_size(), 0);

  // limit == 1: one row appended, total still 3.
  rpc::CoreWorkerStats s1;
  stats.AddToStats(&s1, 1);
  EXPECT_EQ(s1.rdt_objects_total(), 3);
  EXPECT_EQ(s1.rdt_objects_size(), 1);

  // limit == -1: unlimited, all rows appended.
  rpc::CoreWorkerStats s_neg;
  stats.AddToStats(&s_neg, -1);
  EXPECT_EQ(s_neg.rdt_objects_total(), 3);
  EXPECT_EQ(s_neg.rdt_objects_size(), 3);
}

TEST(RdtMemoryStatsTest, ConcurrentSetterReader) {
  RdtMemoryStats stats;
  ObjectID object_id = ObjectID::FromRandom();
  std::atomic<bool> start{false};
  std::atomic<int> reader_errors{0};

  std::thread writer([&] {
    while (!start.load()) {
      std::this_thread::yield();
    }
    for (int i = 0; i < 2000; i++) {
      stats.Set(object_id, {{"cuda:0", 100}}, 1, 0);
      stats.Set(object_id, {}, 0, 0);
    }
  });

  std::thread reader([&] {
    while (!start.load()) {
      std::this_thread::yield();
    }
    for (int i = 0; i < 2000; i++) {
      rpc::CoreWorkerStats s;
      stats.AddToStats(&s, -1);
      // Single object id: total must equal the number of returned rows.
      if (s.rdt_objects_total() != s.rdt_objects_size() || s.rdt_objects_total() > 1) {
        reader_errors.fetch_add(1);
        continue;
      }
      for (const auto &info : s.rdt_objects()) {
        if (info.object_id() != object_id.Binary() || info.num_copies() != 1) {
          reader_errors.fetch_add(1);
        }
        auto it = info.tensor_bytes().find("cuda:0");
        if (it == info.tensor_bytes().end() || it->second != 100) {
          reader_errors.fetch_add(1);
        }
      }
    }
  });

  start.store(true);
  writer.join();
  reader.join();

  EXPECT_EQ(reader_errors.load(), 0);
  // No stale data after the final erase.
  rpc::CoreWorkerStats s;
  stats.AddToStats(&s, -1);
  EXPECT_EQ(s.rdt_objects_total(), 0);
  EXPECT_EQ(s.rdt_objects_size(), 0);
}

}  // namespace core
}  // namespace ray
