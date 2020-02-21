#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

JobInfoGrpcService::JobInfoGrpcService(boost::asio::io_service &io_service,
                                       JobInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}

grpc::Service &JobInfoGrpcService::GetGrpcService() { return service_; }

ActorInfoGrpcService::ActorInfoGrpcService(boost::asio::io_service &io_service,
                                           ActorInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}

grpc::Service &ActorInfoGrpcService::GetGrpcService() { return service_; }

NodeInfoGrpcService::NodeInfoGrpcService(boost::asio::io_service &io_service,
                                         NodeInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}

grpc::Service &NodeInfoGrpcService::GetGrpcService() { return service_; }

ObjectInfoGrpcService::ObjectInfoGrpcService(boost::asio::io_service &io_service,
                                             ObjectInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}

grpc::Service &ObjectInfoGrpcService::GetGrpcService() { return service_; }

TaskInfoGrpcService::TaskInfoGrpcService(boost::asio::io_service &io_service,
                                         TaskInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}

grpc::Service &TaskInfoGrpcService::GetGrpcService() { return service_; }

StatsGrpcService::StatsGrpcService(boost::asio::io_service &io_service,
                                   StatsGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}

grpc::Service &StatsGrpcService::GetGrpcService() { return service_; }

ErrorInfoGrpcService::ErrorInfoGrpcService(boost::asio::io_service &io_service,
                                           ErrorInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}
grpc::Service &ErrorInfoGrpcService::GetGrpcService() { return service_; }

WorkerInfoGrpcService::WorkerInfoGrpcService(boost::asio::io_service &io_service,
                                             WorkerInfoGcsServiceHandler &handler)
    : GrpcService(io_service), service_handler_(handler) {}
grpc::Service &WorkerInfoGrpcService::GetGrpcService() { return service_; }

}  // namespace rpc
}  // namespace ray
