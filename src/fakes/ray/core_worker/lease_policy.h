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

#include "ray/core_worker/lease_policy.h"

namespace ray {
namespace core {

class FakeLeasePolicy : public LeasePolicyInterface {
 public:
  FakeLeasePolicy() = default;

  std::pair<rpc::Address, bool> GetBestNodeForTask(
      const TaskSpecification &spec) override {
    // Return a default empty address and false
    rpc::Address address;
    return std::make_pair(address, false);
  }

  virtual ~FakeLeasePolicy() = default;
};

}  // namespace core
}  // namespace ray
