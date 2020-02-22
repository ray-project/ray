#include "ray/rpc/node_manager/node_manager_server.h"

namespace ray {
namespace rpc {

grpc::Service &NodeManagerGrpcService::GetGrpcService() { return service_; }

NodeManagerGrpcService::NodeManagerGrpcService(boost::asio::io_service &io_service,
                                               NodeManagerServiceHandler &service_handler)
    : GrpcService(io_service), service_handler_(service_handler) {}

}  // namespace rpc
}  // namespace ray
