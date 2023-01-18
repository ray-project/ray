// Copyright 2022 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/rpc/test/grpc_bench/helloworld.grpc.pb.h"
#include "src/ray/rpc/test/grpc_bench/helloworld.pb.h"

using namespace ray;
using namespace ray::rpc;
using namespace helloworld;

class GreeterHandler {
 public:
  virtual void HandleSayHello(SayHelloRequest request,
                              SayHelloReply *reply,
                              SendReplyCallback send_reply_callback) = 0;
  virtual ~GreeterHandler() {}
};

class GreeterServiceHandler : public GreeterHandler {
 public:
  void HandleSayHello(SayHelloRequest request,
                      SayHelloReply *reply,
                      SendReplyCallback send_reply_callback) override {
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override{
      RPC_SERVICE_HANDLER_SERVER_METRICS_DISABLED(Greeter, SayHello, -1)}

  /// The grpc async service object.
  Greeter::AsyncService service_;

  /// The service handler that actually handles the requests.
  GreeterServiceHandler &service_handler_;
};

int main() {
  const auto env = std::getenv("GRPC_SERVER_CPUS");
  const auto parallelism = env ? std::atoi(env) : std::thread::hardware_concurrency();

  GrpcServer server("grpc_bench", 50051, false, parallelism);
  instrumented_io_context main_service;
  std::thread t([&main_service] {
    boost::asio::io_service::work work(main_service);
    main_service.run();
  });
  GreeterServiceHandler handler;
  GreeterGrpcService grpc_service(main_service, handler);
  server.RegisterService(grpc_service);
  server.Run();
  t.join();
  return 0;
}
