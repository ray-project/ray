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

#include <gtest/gtest_prod.h>

#include "ray/gcs/autoscaler/autoscaling_service.h"

namespace ray {
namespace autoscaler {


AutoscalerServiceHandle::AutoscalerServiceHandle(std::unique_ptr<Autoscaler> autoscaler)
  autoscaler_(std::move(autoscaler)) {}

void AutoscalerServiceHandler::HandleUpdateClusterLoad(UpdateClusterResourceLoadRequest request,
                            UpdateClusterResourceLoadResponse *reply,
                            SendReplyCallback send_reply_callback) {
  gcs_resource_manager_->update(request);
  send_reply_callback([reply](){
    //
  })
}

void AutoscalerServiceHandler::HandleGetScalingState(GetScalingStateRequest request,
                                     GetScalingStateResponse *reply,
                                     SendReplyCallback send_reply_callback) {
  autoscaler_->GetScalingState();
   send_reply_callback([reply](){
    //
  }) 
}
 private:
  std::vector<Autoscaler> autoscaler_;

};
}  // namespace autoscaler
}  // namespace ray
