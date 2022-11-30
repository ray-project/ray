#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/rpc/test/helloworld.pb.h"
#include "src/ray/rpc/test/helloworld.grpc.pb.h"

using namespace ray;
using namespace ray::rpc;
using namespace helloworld;

class GreeterHandler {
 public:
  virtual void HandleSayHello(SayHelloRequest request,             
                              SayHelloReply *reply,                
                              SendReplyCallback send_reply_callback)  = 0;
  virtual ~GreeterHandler() {}
};

class GreeterServiceHandler : public GreeterHandler {
 public:
  void HandleSayHello(SayHelloRequest request,             
                      SayHelloReply *reply,                
                      SendReplyCallback send_reply_callback)  override {
    *reply->mutable_response() = std::move(*request.mutable_request());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
};

class GreeterGrpcService : public GrpcService {
 public:
  GreeterGrpcService(instrumented_io_context &main_service,
                     GreeterServiceHandler &service_handler)
      : GrpcService(main_service), service_handler_(service_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(Greeter, SayHello, -1)
  }
  
  /// The grpc async service object.
  Greeter::AsyncService service_;

  /// The service handler that actually handles the requests.
  GreeterServiceHandler &service_handler_;
};

int main() {
  return 0;
}
