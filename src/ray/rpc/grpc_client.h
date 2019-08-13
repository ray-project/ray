#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <grpcpp/grpcpp.h>

namespace ray {
namespace rpc {

template <typename GrpcService>
class GrpcClient {
 protected:
  GrpcClient(bool keep_request_order)
      : keep_request_order_(keep_request_order),
        client_id_(UniqueID::FromRandom().Binary()),
        current_request_index_(0) {}

  std::unordered_map<std::string, std::string> GetMetaForNextRequest() {
    std::unordered_map<std::string, std::string> ret;
    ret["CLIENT_ID"] = client_id_;
    if (keep_request_order_) {
      ret["REQUEST_INDEX"] = current_request_index_++;
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
