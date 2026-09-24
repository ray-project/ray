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

#pragma once

#include <string_view>

namespace ray::gcs {

/// TableName label value for a table name that is not an rpc::TablePrefix
/// constant. Reaching this in production means a StoreClient caller invented a
/// table name; the label collapses rather than growing the exported series
/// count.
inline constexpr std::string_view kUnknownTable = "OTHER";

/// TableName label value for AsyncGetNextJobID, which addresses a plain
/// counter key rather than a GCS table and so has no TablePrefix. Named
/// explicitly instead of collapsing into kUnknownTable, and spelled to match
/// the label the Redis layer uses for the underlying INCRBY.
inline constexpr std::string_view kJobCounterTable = "JobCounter";

/// Maps a GCS table name onto a closed label domain: the names of the
/// rpc::TablePrefix enum, plus kUnknownTable.
///
/// Every table name a StoreClient sees today is produced by TablePrefix_Name
/// at its definition site -- the six GcsTable subclasses in gcs_table_storage.h
/// and StoreClientInternalKV -- so this normally returns its argument's
/// spelling unchanged. What it adds is that the domain is bounded by
/// construction rather than by convention: StoreClient takes an arbitrary
/// std::string, so without this a future caller could turn a job or actor id
/// into unbounded label cardinality.
///
/// The round trip through the enum is also what makes the result safe to
/// capture. Parsing proves the name is a TablePrefix constant; re-deriving the
/// name from the parsed value returns protobuf's generated static string
/// rather than a view into the caller's argument, so the result outlives the
/// call and can be held by a completion callback without copying a std::string
/// into it.
///
/// Self-maintaining: adding a TablePrefix value makes it a valid label with no
/// change here, and the domain can only grow by editing gcs.proto.
///
/// \param table_name The table name a StoreClient method was called with.
/// \return The matching enum name, or kUnknownTable. Points into static
/// storage, so it outlives the argument.
std::string_view NormalizeTableNameLabel(std::string_view table_name);

}  // namespace ray::gcs
