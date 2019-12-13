#include <boost/asio/basic_socket.hpp>
#include "ray/protobuf/gcs.pb.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/gcs/gcs_server/actor_info_handler_impl.h"

namespace ray {
class GcsServerTest : public ::testing::Test {
 public:
  GcsServerTest() : options_("127.0.0.1", 6379, "", true) {}

  virtual void SetUp() {
    work_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    redis_gcs_client_.reset(new gcs::RedisGcsClient(options_));
    RAY_CHECK_OK(redis_gcs_client_->Connect(io_service_));

    rpc_server_.reset(new rpc::GrpcServer(name_, port_));
    actor_info_handler_.reset(new rpc::DefaultActorInfoHandler(*redis_gcs_client_));
    actor_info_service_.reset(
        new rpc::ActorInfoGrpcService(io_service_, *actor_info_handler_));

    rpc_server_->RegisterService(*actor_info_service_);
    rpc_server_->Run();

    thread_.reset(new std::thread([this] {
      io_context_.run();
    }));
  }

  virtual void TearDown() {
    redis_gcs_client_->Disconnect();

    io_service_.stop();
    work_thread_->join();
    work_thread_.reset();

    redis_gcs_client_.reset();

    io_context_.stop();
    thread_->join();
    rpc_server_->Shutdown();
  }

  boost::asio::io_context io_context_;

 protected:

// private:
  std::string name_ = "gcs_manager_test";
  std::string address_ = "127.0.0.1";
  const uint32_t port_ = 5566;

  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<rpc::GrpcServer> rpc_server_;
  std::unique_ptr<rpc::ActorInfoGrpcService> actor_info_service_;
  std::unique_ptr<rpc::DefaultActorInfoHandler> actor_info_handler_;

  gcs::GcsClientOptions options_;
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> work_thread_;
};

TEST_F(GcsServerTest, MyActorInfo) {
  RAY_LOG(INFO) << "hahahahahahahaha";
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
