#include "ray/rpc/object_manager/object_manager_server.h"

namespace ray {
namespace rpc {

grpc::Service &rpc::ObjectManagerGrpcService::GetGrpcService() { return service_; }

ObjectManagerGrpcService::ObjectManagerGrpcService(
    boost::asio::io_service &io_service, ObjectManagerServiceHandler &service_handler)
    : GrpcService(io_service), service_handler_(service_handler) {}

}  // namespace rpc
}  // namespace ray
