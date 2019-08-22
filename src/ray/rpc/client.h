#ifndef RAY_RPC_CLIENT_H
#define RAY_RPC_CLIENT_H

#include "ray/protobuf/common.pb.h"

namespace ray {
namespace rpc {

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

}  // namespace rpc
}  // namespace ray

#endif