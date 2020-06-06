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

#include "bundle_spec.h"
namespace ray {

void BundleSpecification::ComputeResources() {
  auto unit_resource = MapFromProtobuf(message_->unit_resources());

  if (unit_resource.empty()) {
    // A static nil object is used here to avoid allocating the empty object every time.
    unit_resource_ = ResourceSet::Nil();
  } else {
    unit_resource_.reset(new ResourceSet(unit_resource));
  }
}

const ResourceSet &BundleSpecification::GetRequiredResources() const {
  return *unit_resource_;
}

BundleID BundleSpecification::BundleID() const {
  if (message_->bundle_id().empty() /* e.g., empty proto default */) {
    return BundleID::Nil();
  }
  return BundleID::FromBinary(message_->bundle_id());
}

// uint64_t BundleSpecification::UnitCount() const {
//     // TODO(AlisaWu): fill this function.
// }
}  // namespace ray