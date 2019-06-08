#include "node_manager_client.h"

namespace ray {

NodeManagerClient::NodeManagerClient(boost::asio::io_service &main_service,
                                     std::string node_manager_address,
                                     int node_manager_port)
    : main_service_(main_service) {
  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateChannel(node_manager_address + ":" + std::to_string(node_manager_port),
                          grpc::InsecureChannelCredentials());
  stub_ = NodeManagerService::NewStub(channel);

  auto polling_thread = [this]() {
    void *got_tag;
    bool ok = false;
    while (cq_.Next(&got_tag, &ok)) {
      int64_t index = reinterpret_cast<int64_t>(got_tag);
      std::shared_ptr<Callback> callback;
      {
        std::unique_lock<std::mutex> guard(mutex_);
        callback = callbacks_[index];
        callbacks_.erase(index);
      }
      if (ok) {
        main_service_.post([callback]() { callback->success_callback(callback->reply); });
      } else {
        main_service_.post(
            [callback]() { callback->failure_callback(Status::IOError("")); });
      }
    }
  };
  polling_thread_ = std::make_shared<std::thread>(polling_thread);
}

Status NodeManagerClient::ForwardTask(
    const ForwardTaskRequest &request, const ForwardTaskSuccessCallback &success_callback,
    const ForwardTaskFailureCallback &failure_callback) {
  grpc::ClientContext context;
  std::unique_ptr<grpc::ClientAsyncResponseReader<ForwardTaskReply> > rpc(
      stub_->PrepareAsyncForwardTask(&context, request, &cq_));
  rpc->StartCall();

  uint64_t index;
  auto callback = std::make_shared<Callback>();
  callback->success_callback = success_callback;
  callback->failure_callback = failure_callback;
  {
    std::unique_lock<std::mutex> guard(mutex_);
    index = callbacks_.size();
    callbacks_.emplace(index, callback);
  }

  grpc::Status status;
  rpc->Finish(&callback->reply, &status, (void *)index);

  return Status::OK();
}

}  // namespace ray
