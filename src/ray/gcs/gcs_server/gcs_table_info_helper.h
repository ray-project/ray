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

#include "ray/common/id.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/util.h"

namespace ray {
namespace gcs {

inline std::shared_ptr<rpc::JobTableData> GenJobTableData(const rpc::JobTableData &data) {
  auto job_table_data = std::make_shared<rpc::JobTableData>(data);
  job_table_data->set_timestamp(current_time_ms());
  return job_table_data;
}

inline std::shared_ptr<rpc::ActorTableData> GenActorTableData(
    const rpc::ActorTableData &data) {
  auto actor_table_data = std::make_shared<rpc::ActorTableData>(data);
  actor_table_data->set_timestamp(current_time_ms());
  return actor_table_data;
}

inline std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(const rpc::GcsNodeInfo &data) {
  auto node_info = std::make_shared<rpc::GcsNodeInfo>(data);
  node_info->set_timestamp(current_time_ms());
  return node_info;
}

inline void UpdateNodeState(rpc::GcsNodeInfo &node_info,
                            const rpc::GcsNodeInfo_GcsNodeState &state) {
  node_info.set_state(state);
  node_info.set_timestamp(current_time_ms());
}

}  // namespace gcs
}  // namespace ray
