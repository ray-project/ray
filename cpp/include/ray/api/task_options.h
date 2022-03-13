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

}  // namespace internal

enum class PlacementStrategy {
  PACK = 0,
  SPREAD = 1,
  STRICT_PACK = 2,
  STRICT_SPREAD = 3,
  UNRECOGNIZED = -1
};

enum PlacementGroupState {
  PENDING = 0,
  CREATED = 1,
  REMOVED = 2,
  RESCHEDULING = 3,
  UNRECOGNIZED = -1,
};

struct PlacementGroupCreationOptions {
  std::string name;
  std::vector<std::unordered_map<std::string, double>> bundles;
  PlacementStrategy strategy;
};

class PlacementGroup {
 public:
  PlacementGroup() = default;
  PlacementGroup(std::string id,
                 PlacementGroupCreationOptions options,
                 PlacementGroupState state = PlacementGroupState::UNRECOGNIZED)
      : id_(std::move(id)), options_(std::move(options)), state_(state) {}
  std::string GetID() const { return id_; }
  std::string GetName() { return options_.name; }
  std::vector<std::unordered_map<std::string, double>> GetBundles() {
    return options_.bundles;
  }
  ray::PlacementGroupState GetState() { return state_; }
  PlacementStrategy GetStrategy() { return options_.strategy; }
  bool Wait(int timeout_seconds) { return callback_(id_, timeout_seconds); }
  void SetWaitCallbak(std::function<bool(const std::string &, int)> callback) {
    callback_ = std::move(callback);
  }
  bool Empty() const { return id_.empty(); }

 private:
  std::string id_;
  PlacementGroupCreationOptions options_;
  PlacementGroupState state_;
  std::function<bool(const std::string &, int)> callback_;
};

namespace internal {

struct CallOptions {
  std::string name;
  std::unordered_map<std::string, double> resources;
  PlacementGroup group;
  int bundle_index;
};

struct ActorCreationOptions {
  std::string name;
  std::unordered_map<std::string, double> resources;
  int max_restarts = 0;
  int max_concurrency = 1;
  PlacementGroup group;
  int bundle_index;
};
}  // namespace internal

}  // namespace ray