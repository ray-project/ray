#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <map>
#include <grpcpp/grpcpp.h>

#include "ray/common/id.h"

namespace ray {
namespace rpc {

template <typename GrpcService>
class GrpcClient {
 protected:
  explicit GrpcClient(bool keep_request_order)
      : keep_request_order_(keep_request_order),
        client_id_(UniqueID::FromRandom().Hex()),
        current_request_index_(0) {}

  std::unordered_map<std::string, std::string> GetMetaForNextRequest() {
    std::unordered_map<std::string, std::string> ret;
    ret["client_id"] = client_id_;
    if (keep_request_order_) {
      ret["request_index"] = current_request_index_++;
    }
    return ret;
  }

  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;

  bool keep_request_order_;

  std::string client_id_;

  uint64_t current_request_index_;
};

}  // namespace rpc
}  // namespace ray

#endif
