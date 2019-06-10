#ifndef RAY_RPC_GRPC_REQUEST_H
#define RAY_RPC_GRPC_REQUEST_H

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"

namespace ray {

using RequestDoneCallback = std::function<void(Status)>;

class UntypedGrpcRequest {
 public:
  virtual void RequestReceived(bool ok) = 0;
};

class GrpcRequestTag {
 public:
  enum class TagType { REQUEST_RECEIVED, REPLY_SENT };

  GrpcRequestTag(UntypedGrpcRequest *grpc_request, const TagType tag_type)
      : grpc_request_(grpc_request), tag_type_(tag_type) {}

  void OnCompleted(bool ok) {
    switch (tag_type_) {
    case TagType::REQUEST_RECEIVED:
      grpc_request_->RequestReceived(ok);
      break;
    case TagType::REPLY_SENT:
      delete grpc_request_;
      break;
    }
  }

 private:
  UntypedGrpcRequest *grpc_request_;
  const TagType tag_type_;
};

template <class GrpcService, class ServiceHandler, class Request, class Reply>
class GrpcRequest : public UntypedGrpcRequest {
  // Represents the generic signature of a generated
  // `GrpcService::RequestFoo()` method, where `Foo` is the name of an
  // RPC method.
  using EnqueueFunction = void (GrpcService::*)(
      ::grpc::ServerContext *, Request *, ::grpc::ServerAsyncResponseWriter<Reply> *,
      ::grpc::CompletionQueue *, ::grpc::ServerCompletionQueue *, void *);

  // Represents the generic signature of a `Service::HandleFoo()`
  // method, where `Foo` is the name of an RPC method.
  using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                         RequestDoneCallback);

 public:
  GrpcRequest(GrpcService *grpc_service, EnqueueFunction enqueue_function,
              ServiceHandler *service_handler,
              HandleRequestFunction handle_request_function,
              ::grpc::ServerCompletionQueue *cq)
      : grpc_service_(grpc_service),
        enqueue_function_(enqueue_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        cq_(cq),
        responder_(&ctx_) {
    (grpc_service_->*enqueue_function_)(&ctx_, &request_, &responder_, cq_, cq_,
                                        &request_received_tag_);
  }

  void RequestReceived(bool ok) override {
    if (ok) {
      new GrpcRequest(grpc_service_, enqueue_function_, service_handler_,
                      handle_request_function_, cq_);
      (service_handler_->*handle_request_function_)(
                             request_, &reply_,
                             [this](Status status) { SendResponse(status); });
    }
  }

 private:
  void SendResponse(Status status) {
    if (status.ok()) {
      responder_.Finish(reply_, ::grpc::Status::OK, &reply_sent_tag_);
    }
  }

  GrpcService *grpc_service_;
  EnqueueFunction enqueue_function_;

  ServiceHandler *service_handler_;
  HandleRequestFunction handle_request_function_;

  ::grpc::ServerCompletionQueue *cq_;

  ::grpc::ServerContext ctx_;
  ::grpc::ServerAsyncResponseWriter<Reply> responder_;

  Request request_;
  Reply reply_;

  GrpcRequestTag request_received_tag_{this, GrpcRequestTag::TagType::REQUEST_RECEIVED};
  GrpcRequestTag reply_sent_tag_{this, GrpcRequestTag::TagType::REPLY_SENT};
};  // namespace ray

}  // namespace ray

#endif
