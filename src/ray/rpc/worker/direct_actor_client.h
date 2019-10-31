#ifndef RAY_RPC_DIRECT_ACTOR_CLIENT_H
#define RAY_RPC_DIRECT_ACTOR_CLIENT_H

#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "absl/base/thread_annotations.h"

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/direct_actor_common.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/direct_actor.grpc.pb.h"
#include "src/ray/protobuf/direct_actor.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a direct actor server.
class DirectActorClient : public std::enable_shared_from_this<DirectActorClient> {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  static std::shared_ptr<DirectActorClient> make(const std::string &address,
                                                 const int port,
                                                 ClientCallManager &client_call_manager) {
    auto instance = new DirectActorClient(address, port, client_call_manager);
    return std::shared_ptr<DirectActorClient>(instance);
  }

  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  DirectActorClient(const std::string &address, const int port,
                    ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = DirectActorService::NewStub(channel);
  };

  /// Push a task.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status PushTask(std::unique_ptr<PushTaskRequest> request,
                       const ClientCallback<PushTaskReply> &callback) {
    request->set_sequence_number(request->task_spec().actor_task_spec().actor_counter());
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (request->task_spec().caller_id() != cur_caller_id_) {
        // We are running a new task, reset the seq no counter.
        max_finished_seq_no_ = -1;
        cur_caller_id_ = request->task_spec().caller_id();
      }
      send_queue_.push_back(std::make_pair(std::move(request), callback));
    }
    SendRequests();
    return ray::Status::OK();
  }

  /// Notify a wait has completed for direct actor call arguments.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status DirectActorCallArgWaitComplete(
      const DirectActorCallArgWaitCompleteRequest &request,
      const ClientCallback<DirectActorCallArgWaitCompleteReply> &callback) {
    auto call = client_call_manager_.CreateCall<DirectActorService,
                                                DirectActorCallArgWaitCompleteRequest,
                                                DirectActorCallArgWaitCompleteReply>(
        *stub_, &DirectActorService::Stub::PrepareAsyncDirectActorCallArgWaitComplete,
        request, callback);
    return call->GetStatus();
  }

  /// Send as many pending tasks as possible. This method is thread-safe.
  ///
  /// The client will guarantee no more than kMaxBytesInFlight bytes of RPCs are being
  /// sent at once. This prevents the server scheduling queue from being overwhelmed.
  /// See direct_actor.proto for a description of the ordering protocol.
  void SendRequests() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto this_ptr = this->shared_from_this();

    while (!send_queue_.empty() && rpc_bytes_in_flight_ < kMaxBytesInFlight) {
      auto pair = std::move(*send_queue_.begin());
      send_queue_.pop_front();

      auto request = std::move(pair.first);
      auto callback = pair.second;
      int64_t task_size = RequestSizeInBytes(*request);
      int64_t seq_no = request->sequence_number();
      request->set_client_processed_up_to(max_finished_seq_no_);
      rpc_bytes_in_flight_ += task_size;

      client_call_manager_.CreateCall<DirectActorService, PushTaskRequest, PushTaskReply>(
          *stub_, &DirectActorService::Stub::PrepareAsyncPushTask, *request,
          [this, this_ptr, seq_no, task_size, callback](Status status,
                                                        const rpc::PushTaskReply &reply) {
            {
              std::lock_guard<std::mutex> lock(mutex_);
              if (seq_no > max_finished_seq_no_) {
                max_finished_seq_no_ = seq_no;
              }
              rpc_bytes_in_flight_ -= task_size;
              RAY_CHECK(rpc_bytes_in_flight_ >= 0);
            }
            SendRequests();
            callback(status, reply);
          });
    }

    if (!send_queue_.empty()) {
      RAY_LOG(DEBUG) << "client send queue size " << send_queue_.size();
    }
  }

 private:
  /// Protects against unsafe concurrent access from the callback thread.
  std::mutex mutex_;

  /// The gRPC-generated stub.
  std::unique_ptr<DirectActorService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;

  /// Queue of requests to send.
  std::deque<std::pair<std::unique_ptr<PushTaskRequest>, ClientCallback<PushTaskReply>>>
      send_queue_ GUARDED_BY(mutex_);

  /// The number of bytes currently in flight.
  int64_t rpc_bytes_in_flight_ GUARDED_BY(mutex_) = 0;

  /// The max sequence number we have processed responses for.
  int64_t max_finished_seq_no_ GUARDED_BY(mutex_) = -1;

  /// The task id we are currently sending requests for. When this changes,
  /// the max finished seq no counter is reset.
  std::string cur_caller_id_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_DIRECT_ACTOR_CLIENT_H
