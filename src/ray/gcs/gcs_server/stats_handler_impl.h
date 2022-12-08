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

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `StatsHandler`.
class DefaultStatsHandler : public rpc::StatsHandler {
 public:
  explicit DefaultStatsHandler(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
      : gcs_table_storage_(std::move(gcs_table_storage)) {
    for (int index = 0; index < RayConfig::instance().maximum_profile_table_rows_count();
         ++index) {
      ids_.emplace_back(UniqueID::FromRandom());
    }
  }

  void HandleAddProfileData(AddProfileDataRequest request,
                            AddProfileDataReply *reply,
                            SendReplyCallback send_reply_callback) override;

  void HandleGetAllProfileInfo(rpc::GetAllProfileInfoRequest request,
                               rpc::GetAllProfileInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

 private:
  int cursor_ = 0;
  std::vector<UniqueID> ids_;

  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

}  // namespace rpc
}  // namespace ray
