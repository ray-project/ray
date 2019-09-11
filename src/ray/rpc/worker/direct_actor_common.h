#ifndef RAY_RPC_DIRECT_ACTOR_COMMON_H
#define RAY_RPC_DIRECT_ACTOR_COMMON_H

#include "src/ray/protobuf/direct_actor.grpc.pb.h"
#include "src/ray/protobuf/direct_actor.pb.h"

namespace ray {
namespace rpc {

/// The maximum number of requests in flight per client.
const int64_t kMaxBytesInFlight = 16 * 1024 * 1024;

/// The base size in bytes per request.
const int64_t kBaseRequestSize = 1024;

/// Get the estimated size in bytes of the given task.
const static int64_t RequestSizeInBytes(const PushTaskRequest &request) {
  int64_t size = kBaseRequestSize;
  for (auto &arg : request.task_spec().args()) {
    size += arg.data().size();
  }
  return size;
}

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_DIRECT_ACTOR_COMMON_H
