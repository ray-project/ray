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

#pragma once

#include <memory>

#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// Narrow interface that other GCS components depend on when they need to
/// push data into the resource manager. Keeping it minimal lets tests
/// substitute a fake without spinning up the full GCS resource manager
/// dependency graph.
class GcsResourceManagerInterface {
 public:
  virtual ~GcsResourceManagerInterface() = default;

  /// Update the placement group load info that the autoscaler consumes through
  /// the resource usage broadcast. Called by GcsPlacementGroupManager whenever
  /// the per-shape pending-PG counts change.
  virtual void UpdatePlacementGroupLoad(
      std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) = 0;
};

}  // namespace gcs
}  // namespace ray
