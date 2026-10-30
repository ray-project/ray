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

#include "ray/gcs/store_client/table_name_label.h"

#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

// The seven table names a StoreClient can actually be called with today,
// sourced the same way their producers source them -- TablePrefix_Name in the
// six GcsTable subclasses and in StoreClientInternalKV -- rather than as string
// literals. A new GcsTable whose name is not an enum constant therefore fails
// here instead of silently reporting OTHER in production.
TEST(TableNameLabelTest, EveryProducedTableNameSurvivesNormalization) {
  const std::vector<rpc::TablePrefix> produced = {
      rpc::TablePrefix::JOB,
      rpc::TablePrefix::ACTOR,
      rpc::TablePrefix::ACTOR_TASK_SPEC,
      rpc::TablePrefix::PLACEMENT_GROUP,
      rpc::TablePrefix::NODE,
      rpc::TablePrefix::WORKERS,
      rpc::TablePrefix::KV,
  };
  for (const auto prefix : produced) {
    const std::string &name = rpc::TablePrefix_Name(prefix);
    EXPECT_EQ(NormalizeTableNameLabel(name), name);
  }
}

// The exported label domain is exactly the enum plus the two sentinels; nothing
// in the enum is dropped or rewritten.
TEST(TableNameLabelTest, EveryEnumNameNormalizesToItself) {
  const auto *descriptor = rpc::TablePrefix_descriptor();
  ASSERT_GT(descriptor->value_count(), 0);
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const std::string &name = descriptor->value(i)->name();
    EXPECT_EQ(NormalizeTableNameLabel(name), name);
  }
}

TEST(TableNameLabelTest, UnknownNamesCollapse) {
  // What StoreClientTestBase passes today -- proof that the interface accepts
  // names outside the domain, which is why normalizing is not redundant.
  EXPECT_EQ(NormalizeTableNameLabel("test_table"), kUnknownTable);
  EXPECT_EQ(NormalizeTableNameLabel(""), kUnknownTable);
  // The shape a cardinality incident would take: a table name built from an id.
  EXPECT_EQ(NormalizeTableNameLabel("ACTOR_01ff00c0"), kUnknownTable);
  // Matching is case sensitive, like every other verb/name label in the GCS.
  EXPECT_EQ(NormalizeTableNameLabel("job"), kUnknownTable);
  EXPECT_EQ(NormalizeTableNameLabel(std::string(4096, 'x')), kUnknownTable);
}

// The sentinels have to stay outside the enum's namespace, or a real table
// would be indistinguishable from a collapsed one.
TEST(TableNameLabelTest, SentinelsAreNotEnumNames) {
  const auto *descriptor = rpc::TablePrefix_descriptor();
  for (int i = 0; i < descriptor->value_count(); ++i) {
    const std::string_view name(descriptor->value(i)->name());
    EXPECT_NE(name, kUnknownTable);
    EXPECT_NE(name, kJobCounterTable);
  }
  EXPECT_NE(kUnknownTable, kJobCounterTable);
}

}  // namespace gcs
}  // namespace ray
