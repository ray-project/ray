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

#include <gtest/gtest_prod.h>

#include "ray/gcs/autoscaler/autoscaler.h"

namespace ray {
namespace autoscaler {

// the serivce side of autoscaler.
class AutoscalerServiceHandler {
 public:
   AutoscalerServiceHandler (std::unique_ptr<Autoscaler> autoscaler);

   virtual void HandleUpdateClusterLoad(UpdateClusterResourceLoadRequest request,
                            UpdateClusterResourceLoadResponse *reply,
                            SendReplyCallback send_reply_callback);

  virtual void HandleGetScalingState(GetScalingStateRequest request,
                                     GetScalingStateResponse *reply,
                                     SendReplyCallback send_reply_callback);
 private:
  std::unique_ptr<Autoscaler> autoscaler_;
  std::unique_ptr<GcsResourceManager> gcs_resource_manager_;

};
}  // namespace autoscaler
}  // namespace ray
