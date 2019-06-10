#include "node_manager_client.h"

namespace ray {

NodeManagerClient::NodeManagerClient(boost::asio::io_service &main_service,
                                     const std::string &node_manager_address,
                                     const int node_manager_port)
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
      std::shared_ptr<CallbackItem> callback_item;
      {
        std::unique_lock<std::mutex> guard(mutex_);
        callback_item = pending_callbacks_[index];
        pending_callbacks_.erase(index);
      }
      Status status = ok ? Status::OK() : Status::IOError("");
      main_service_.post([callback_item, status]() {
        callback_item->callback(status, callback_item->reply);
      });
    }
  };
  polling_thread_ = std::make_shared<std::thread>(polling_thread);
}

void NodeManagerClient::ForwardTask(const ForwardTaskRequest &request,
                                    const ForwardTaskCallback &callback) {
  grpc::ClientContext context;
  std::unique_ptr<grpc::ClientAsyncResponseReader<ForwardTaskReply> > rpc(
      stub_->PrepareAsyncForwardTask(&context, request, &cq_));
  rpc->StartCall();

  uint64_t index;
  auto callback_item = std::make_shared<CallbackItem>();
  callback_item->callback = callback;
  {
    std::unique_lock<std::mutex> guard(mutex_);
    index = pending_callbacks_.size();
    pending_callbacks_.emplace(index, callback_item);
  }

  grpc::Status status;
  rpc->Finish(&callback_item->reply, &status, (void *)index);
  if (!status.ok()) {
    // TODO
  }
}

}  // namespace ray
