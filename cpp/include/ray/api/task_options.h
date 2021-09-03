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

#include <cmath>

namespace ray {
namespace internal {

inline void CheckTaskOptions(const std::unordered_map<std::string, double> &resources) {
  for (auto &pair : resources) {
    if (pair.first.empty() || pair.second == 0) {
      throw RayException("Resource values should be positive. Specified resource: " +
                         pair.first + " = " + std::to_string(pair.second) + ".");
    }
    // Note: A resource value should be an integer if it is greater than 1.0.
    // e.g. 3.0 is a valid resource value, but 3.5 is not.
    double intpart;
    if (pair.second > 1 && std::modf(pair.second, &intpart) != 0.0) {
      throw RayException(
          "A resource value should be an integer if it is greater than 1.0. Specified "
          "resource: " +
          pair.first + " = " + std::to_string(pair.second) + ".");
    }
  }
}

struct CallOptions {
  std::string name;
  std::unordered_map<std::string, double> resources;
};

struct ActorCreationOptions {
  std::string name;
  std::unordered_map<std::string, double> resources;
  int max_restarts = 0;
  int max_concurrency = 1;
};

enum class PlacementStrategy {
  PACK = 0,
  SPREAD = 1,
  STRICT_PACK = 2,
  STRICT_SPREAD = 3,
  UNRECOGNIZED = -1
};

struct PlacementGroupCreationOptions {
  bool global;
  std::string name;
  std::vector<std::unordered_map<std::string, double>> bundles;
  PlacementStrategy strategy;
};

}  // namespace internal

class PlacementGroup {
 public:
  PlacementGroup() = default;
  PlacementGroup(std::string id, internal::PlacementGroupCreationOptions options)
      : id_(std::move(id)), options_(std::move(options)) {}
  std::string GetID() { return id_; }
  std::string GetName() { return options_.name; }
  std::vector<std::unordered_map<std::string, double>> GetBundles() {
    return options_.bundles;
  }
  internal::PlacementStrategy GetStrategy() { return options_.strategy; }
  bool Wait(int timeout_seconds) { return callback_(id_, timeout_seconds); }
  void SetWaitCallbak(std::function<bool(const std::string &, int)> callback) {
    callback_ = std::move(callback);
  }

 private:
  std::string id_;
  internal::PlacementGroupCreationOptions options_;
  std::function<bool(const std::string &, int)> callback_;
};

}  // namespace ray