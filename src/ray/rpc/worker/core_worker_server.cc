#include "ray/rpc/worker/core_worker_server.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace rpc {

#define RAY_CORE_WORKER_RPC_HANDLER(HANDLER, CONCURRENCY)                             \
  std::unique_ptr<ServerCallFactory> HANDLER##_call_factory(                          \
      new ServerCallFactoryImpl<CoreWorkerService, CoreWorker, HANDLER##Request,      \
                                HANDLER##Reply>(                                      \
          service_, &CoreWorkerService::AsyncService::Request##HANDLER, core_worker_, \
          &CoreWorker::Handle##HANDLER, cq, main_service_));                          \
  server_call_factories_and_concurrencies->emplace_back(                              \
      std::move(HANDLER##_call_factory), CONCURRENCY);

CoreWorkerGrpcService::CoreWorkerGrpcService(boost::asio::io_service &main_service,
                                             CoreWorker &core_worker)
    : GrpcService(main_service), core_worker_(core_worker){};

void CoreWorkerGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
        *server_call_factories_and_concurrencies) {
  RAY_CORE_WORKER_RPC_HANDLERS
}

}  // namespace rpc
}  // namespace ray
