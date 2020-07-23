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

#include "ray/core_worker/actor_reporter.h"

#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_accessor.h"

namespace ray {

void ActorReporter::PublishTerminatedActor(const TaskSpecification &actor_creation_task) {
  auto actor_id = actor_creation_task.ActorCreationId();
  auto data = gcs::CreateActorTableData(actor_creation_task, rpc::Address(),
                                        rpc::ActorTableData::DEAD, 0);

  auto update_callback = [actor_id](Status status) {
    if (!status.ok()) {
      // Only one node at a time should succeed at creating or updating the actor.
      RAY_LOG(ERROR) << "Failed to update state to DEAD for actor " << actor_id
                     << ", error: " << status.ToString();
    }
  };
  RAY_CHECK_OK(gcs_client_->Actors().AsyncRegister(data, update_callback));
}

}  // namespace ray
