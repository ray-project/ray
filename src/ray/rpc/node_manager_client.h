#ifndef RAY_RPC_NODE_MANAGER_CLIENT_H
#define RAY_RPC_NODE_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "ray/common/status.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

class NodeManagerClient {
 public:
  using ForwardTaskCallback =
      std::function<void(const Status &status, const ForwardTaskReply &reply)>;

  NodeManagerClient(boost::asio::io_service &main_service,
                    const std::string &node_manager_address, const int node_manager_port);

  void ForwardTask(const ForwardTaskRequest &request,
                   const ForwardTaskCallback &callback);

 private:
  boost::asio::io_service &main_service_;
  std::unique_ptr<NodeManagerService::Stub> stub_;

  grpc::CompletionQueue cq_;

  struct CallbackItem {
    ForwardTaskReply reply;
    ForwardTaskCallback callback;
  };

  std::mutex mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<CallbackItem>> pending_callbacks_;

  std::shared_ptr<std::thread> polling_thread_;
};

}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
