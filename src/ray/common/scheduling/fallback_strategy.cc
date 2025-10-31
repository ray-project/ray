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

#include "ray/common/scheduling/fallback_strategy.h"

#include <memory>
#include <vector>

#include "ray/util/logging.h"

namespace ray {

void FallbackOption::ToProto(rpc::FallbackOption *proto) const {
  RAY_CHECK(proto != nullptr);
  label_selector.ToProto(proto->mutable_label_selector());
  // When a new option is added, add its serialization here.
}

std::shared_ptr<std::vector<FallbackOption>> ParseFallbackStrategy(
    const google::protobuf::RepeatedPtrField<rpc::FallbackOption> &strategy_proto_list) {
  auto strategy_list = std::make_shared<std::vector<FallbackOption>>();
  strategy_list->reserve(strategy_proto_list.size());

  for (const auto &strategy_proto : strategy_proto_list) {
    strategy_list->emplace_back(strategy_proto.label_selector());
  }

  return strategy_list;
}

rpc::FallbackStrategy SerializeFallbackStrategy(
    const std::vector<FallbackOption> &strategy_list) {
  rpc::FallbackStrategy strategy_proto;
  for (const auto &options : strategy_list) {
    options.ToProto(strategy_proto.add_options());
  }

  return strategy_proto;
}

}  // namespace ray
