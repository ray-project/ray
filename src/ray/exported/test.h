#pragma once
#include "ray/common/id.h"
#include "ray/common/buffer.h"
#include "ray/core_worker/common.h"

namespace ray_exported {
namespace ray {

using namespace ::ray;
void SendInternal(const ActorID& peer_actor_id, std::shared_ptr<LocalMemoryBuffer> buffer,
                             RayFunction &function, int return_num,
                             std::vector<ObjectID> &return_ids);

}
}