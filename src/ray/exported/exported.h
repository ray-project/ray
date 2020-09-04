#pragma once
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/core_worker/common.h"

namespace ray {
namespace exported_ {

void SubmitActorTask(const ActorID &peer_actor_id,
                     std::shared_ptr<LocalMemoryBuffer> buffer, RayFunction &function,
                     int return_num, std::vector<ObjectID> &return_ids);

}  // namespace exported_
}  // namespace ray