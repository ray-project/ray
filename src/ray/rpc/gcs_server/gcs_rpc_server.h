#pragma once

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

class JobInfoAccessHandler {
 public:
  virtual ~JobInfoAccessHandler() = default;

  virtual void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `JobInfoAccessService`.
class JobInfoAccessGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoAccessGrpcService(boost::asio::io_service &io_service,
                                    JobInfoAccessHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    std::unique_ptr<ServerCallFactory> add_job_call_factory(
        new ServerCallFactoryImpl<JobInfoAccessService, JobInfoAccessHandler,
                                  AddJobRequest, AddJobReply>(
            service_, &JobInfoAccessService::AsyncService::RequestAddJob,
            service_handler_, &JobInfoAccessHandler::HandleAddJob, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(add_job_call_factory),
                                                          1);

    std::unique_ptr<ServerCallFactory> mark_job_finished_call_factory(
        new ServerCallFactoryImpl<JobInfoAccessService, JobInfoAccessHandler,
                                  MarkJobFinishedRequest, MarkJobFinishedReply>(
            service_, &JobInfoAccessService::AsyncService::RequestMarkJobFinished,
            service_handler_, &JobInfoAccessHandler::HandleMarkJobFinished, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(mark_job_finished_call_factory), 1);
  }

 private:
  /// The grpc async service object.
  JobInfoAccessService::AsyncService service_;
  /// The service handler that actually handle the requests.
  JobInfoAccessHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
