#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <grpcpp/grpcpp.h>
#include <map>

#include "ray/common/id.h"
#include "ray/rpc/constants.h"

namespace ray {
namespace rpc {

template <typename GrpcService>
class GrpcClient {
 protected:
  /// Constructor.
  ///
  /// \param[in] strict_request_order Whether the server should handle requests from this
  /// client in the same order as they were sent.
  explicit GrpcClient(bool strict_request_order)
      : strict_request_order_(strict_request_order),
        client_id_(UniqueID::FromRandom().Hex()),
        current_request_index_(0) {}

  /// Get metadata for the next request to send.
  std::unordered_map<std::string, std::string> GetMetaForNextRequest() {
    std::unordered_map<std::string, std::string> ret;
    ret[CLIENT_ID_META_KEY] = client_id_;
    if (strict_request_order_) {
      // Add request index to the meta if `strict_request_order` is enabled.
      ret[REQUEST_INDEX_META_KEY] = std::to_string(current_request_index_++);
    }
    return ret;
  }

  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;

  bool strict_request_order_;

  std::string client_id_;

  uint64_t current_request_index_;
};

}  // namespace rpc
}  // namespace ray

#endif
