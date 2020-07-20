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

#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/util/logging.h"

/// Limit the ID range to test for collisions.
#define MAX_ID_TEST 8

/// Class to map string IDs to unique integer IDs and back.
class StringIdMap {
  absl::flat_hash_map<std::string, int64_t> string_to_int_;
  absl::flat_hash_map<int64_t, std::string> int_to_string_;
  std::hash<std::string> hasher_;

 public:
  StringIdMap(){};
  ~StringIdMap(){};

  /// Get integer ID associated with an existing string ID.
  ///
  /// \param String ID.
  /// \return The integer ID associated with the given string ID.
  int64_t Get(const std::string &string_id);

  /// Get string ID associated with an existing integer ID.
  ///
  /// \param Integre ID.
  /// \return The string ID associated with the given integer ID.
  std::string Get(uint64_t id);

  /// Insert a string ID and get the associated integer ID.
  ///
  /// \param String ID to be inserted.
  /// \param max_id The number of unique possible ids. This is used
  ///               to force collisions for testing. If -1, it is not used.
  /// \return The integer ID associated with string ID string_id.
  int64_t Insert(const std::string &string_id, uint8_t num_ids = 0);

  /// Delete an ID identified by its string format.
  ///
  /// \param ID to be deleted.
  void Remove(const std::string &string_id);

  /// Delete an ID identified by its integer format.
  ///
  /// \param ID to be deleted.
  void Remove(int64_t id);

  /// Get number of identifiers.
  int64_t Count();
};
