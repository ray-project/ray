#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/status.h"
#include "ray/rpc/util.h"

namespace ray {

class UntypedClientCall {
 public:
  virtual void OnReplyReceived();
};

class ClientCallManager;

template <class Reply, class Callback>
class ClientCall {
 public:
  void OnReplyReceived() const { callback_(GrpcStatusToRayStatus(status_), reply_); }

 private:
  ClientCall(const Callback &callback) : callback_(callback) {}
  Reply reply_;
  Callback callback_;
  std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> response_reader_;
  ::grpc::Status status_;
  ::grpc::ClientContext context_;

  friend ClientCallManager;
};

class ClientCallManager {
 public:
  ClientCallManager(boost::asio::io_service &main_service) : main_service_(main_service) {
    auto poll = [this]() {
      void *got_tag;
      bool ok = false;
      while (cq_.Next(&got_tag, &ok)) {
        RAY_CHECK(ok);
        UntypedClientCall *call = reinterpret_cast<UntypedClientCall *>(got_tag);
        main_service_.post([call]() {
          call->OnReplyReceived();
          delete call;
        });
      }
    };
    polling_thread_.reset(new std::thread(std::move(poll)));
  }

  ~ClientCallManager() { cq_.Shutdown(); }

  template <class GrpcService, class Request, class Reply, class Callback>
  void CreateCall(const std::unique_ptr<typename GrpcService::Stub> &stub,
                  const Request &request, const Callback &callback) {
    auto call = new ClientCall<Reply, Callback>(callback);
    call->response_reader_ =
        stub->PrepareAsyncForwardTask(&call->context_, request, &cq_);
    call->response_reader_->StartCall();
    call->response_reader_->Finish(&call->reply_, &call->status_, (void *)call);
  }

 private:
  boost::asio::io_service &main_service_;

  std::unique_ptr<std::thread> polling_thread_;

  ::grpc::CompletionQueue cq_;
};

}  // namespace ray

#endif
