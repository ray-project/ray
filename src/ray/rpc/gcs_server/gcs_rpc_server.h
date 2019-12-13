#ifndef RAY_RPC_GCS_RPC_SERVER_H
#define RAY_RPC_GCS_RPC_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

class JobInfoHandler {
 public:
  virtual ~JobInfoHandler() = default;

  virtual void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `JobInfoGcsService`.
class JobInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoGrpcService(boost::asio::io_service &io_service,
                              JobInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    std::unique_ptr<ServerCallFactory> add_job_call_factory(
        new ServerCallFactoryImpl<JobInfoGcsService, JobInfoHandler, AddJobRequest,
                                  AddJobReply>(
            service_, &JobInfoGcsService::AsyncService::RequestAddJob, service_handler_,
            &JobInfoHandler::HandleAddJob, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(add_job_call_factory),
                                                          1);

    std::unique_ptr<ServerCallFactory> mark_job_finished_call_factory(
        new ServerCallFactoryImpl<JobInfoGcsService, JobInfoHandler,
                                  MarkJobFinishedRequest, MarkJobFinishedReply>(
            service_, &JobInfoGcsService::AsyncService::RequestMarkJobFinished,
            service_handler_, &JobInfoHandler::HandleMarkJobFinished, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(mark_job_finished_call_factory), 1);
  }

 private:
  /// The grpc async service object.
  JobInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  JobInfoHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
