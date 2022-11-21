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
#include <memory>

#include "ray/gcs/store_client/gcs_kv_manager.h"
#include "src/ray/protobuf/usage.pb.h"

namespace ray {
namespace gcs {

/// \class InternalKVInterface
/// The interface for internal kv implementation. Ideally we should merge this
/// with store client, but due to compatibility issue, we keep them separated
/// right now.
class GcsUsageReporter {
 public:
  GcsUsageReporter(InternalKVInterface &kv);
  void RecordExtraUsageTag(usage::TagKey key, std::string value);

  privat : InternalKVInterface &kv_;
};

}  // namespace gcs
}  // namespace ray
