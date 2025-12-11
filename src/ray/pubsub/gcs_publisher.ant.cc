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

#include "ray/pubsub/gcs_publisher.h"

namespace ray {
namespace pubsub {

void GcsPublisher::PublishVirtualCluster(const VirtualClusterID &id,
                                         const rpc::VirtualClusterTableData &message) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::RAY_VIRTUAL_CLUSTER_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_virtual_cluster_message() = message;
  publisher_->Publish(msg);
}

}  // namespace pubsub
}  // namespace ray
