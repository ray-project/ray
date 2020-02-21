#include "ray/rpc/worker/core_worker_server.h"

namespace ray {
namespace rpc {

CoreWorkerGrpcService::CoreWorkerGrpcService(boost::asio::io_service &main_service,
                                             CoreWorkerServiceHandler &service_handler)
    : GrpcService(main_service), service_handler_(service_handler) {}
grpc::Service &CoreWorkerGrpcService::GetGrpcService() { return service_; }

}  // namespace rpc
}  // namespace ray
