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
  using ForwardTaskSuccessCallback =
      std::function<void(const ForwardTaskReply &reply)>;
  using ForwardTaskFailureCallback = std::function<void(const Status &error)>;

  NodeManagerClient(boost::asio::io_service &main_service,
                    std::string node_manager_address, int node_manager_port);

  Status ForwardTask(const ForwardTaskRequest &request,
                     const ForwardTaskSuccessCallback &success_callback,
                     const ForwardTaskFailureCallback &failure_callback);

 private:
  boost::asio::io_service &main_service_;
  std::unique_ptr<NodeManagerService::Stub> stub_;

  grpc::CompletionQueue cq_;

  struct Callback {
    ForwardTaskReply reply;
    ForwardTaskSuccessCallback success_callback;
    ForwardTaskFailureCallback failure_callback;
  };

  std::mutex mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<Callback>> callbacks_;

  std::shared_ptr<std::thread> polling_thread_;
};

}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
