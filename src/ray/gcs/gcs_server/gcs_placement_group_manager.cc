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


#include"ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/pb_util.h>
#include <ray/protobuf/gcs.pb.h>

namespace ray {
namespace gcs {

// TODO(AlisaWu): initial GcsPlacementGroupManager

void GcsPlacementGroupManager::HandleCreatePlacementGroup(const rpc::CreatePlacementGroupRequest &request, rpc::CreatePlacementGroupReply *reply,
                                                    rpc::SendReplyCallback send_reply_callback){
    auto placement_group_id =
    PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id());
    RAY_LOG(INFO) << "Registering Placemnet Group, Placement Group id = " << placement_group_id.Binary();
    // AddPlacementGroup(std::make_shared<rpc::PlacementGroupSpec>(request.placement_group_spec()));
    // auto on_done = [this, placement_group_id, request, reply,
    //                 send_reply_callback](const Status &status) {
    //     RAY_CHECK_OK(status);
    //     RAY_LOG(INFO) << "Finished registering plcaement group info, placement group id = " << placement_group_id;
    //     GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    // };
    // TODO(AlisaWu): complete this function.
}

void GcsPlacementGroupManager::AddPlacementGroup(std::shared_ptr<rpc::PlacementGroupTableData> placement_group){
//   auto placement_group_id = PlacementGroupID::FromBinary(placement_group->placement_group_id());
//   auto iter = alive_placement_groups_.find(placement_group_id);
//   if (iter == alive_placement_groups_.end()) {
//     alive_placement_groups_.emplace(placement_group_id, placement_group);
//   }
// TODO(AlisaWu): complete this function.
}

} //namespace
} //namespace

