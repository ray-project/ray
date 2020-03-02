#ifndef RAY_RPC_CORE_WORKER_CLIENT_H
#define RAY_RPC_CORE_WORKER_CLIENT_H

#include <grpcpp/grpcpp.h>

#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/hash/hash.h"
#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace rpc {

/// The maximum number of requests in flight per client.
const int64_t kMaxBytesInFlight = 16 * 1024 * 1024;

/// The base size in bytes per request.
const int64_t kBaseRequestSize = 1024;

/// Get the estimated size in bytes of the given task.
const static int64_t RequestSizeInBytes(const PushTaskRequest &request) {
  int64_t size = kBaseRequestSize;
  for (auto &arg : request.task_spec().args()) {
    size += arg.data().size();
  }
  return size;
}

// Shared between direct actor and task submitters.
class CoreWorkerClientInterface;

// TODO(swang): Remove and replace with rpc::Address.
class WorkerAddress {
 public:
  WorkerAddress(const rpc::Address &address)
      : ip_address(address.ip_address()),
        port(address.port()),
        worker_id(WorkerID::FromBinary(address.worker_id())),
        raylet_id(ClientID::FromBinary(address.raylet_id())) {}
  template <typename H>
  friend H AbslHashValue(H h, const WorkerAddress &w) {
    return H::combine(std::move(h), w.ip_address, w.port, w.worker_id, w.raylet_id);
  }

  bool operator==(const WorkerAddress &other) const {
    return other.ip_address == ip_address && other.port == port &&
           other.worker_id == worker_id && other.raylet_id == raylet_id;
  }

  rpc::Address ToProto() const {
    rpc::Address addr;
    addr.set_raylet_id(raylet_id.Binary());
    addr.set_ip_address(ip_address);
    addr.set_port(port);
    addr.set_worker_id(worker_id.Binary());
    return addr;
  }

  /// The ip address of the worker.
  const std::string ip_address;
  /// The local port of the worker.
  const int port;
  /// The unique id of the worker.
  const WorkerID worker_id;
  /// The unique id of the worker raylet.
  const ClientID raylet_id;
};

typedef std::function<std::shared_ptr<CoreWorkerClientInterface>(const rpc::Address &)>
    ClientFactoryFn;

/// Abstract client interface for testing.
class CoreWorkerClientInterface {
 public:
  virtual const rpc::Address &Addr() const {
    static const rpc::Address empty_addr_;
    return empty_addr_;
  }

  /// This is called by the Raylet to assign a task to the worker.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status AssignTask(const AssignTaskRequest &request,
                                 const ClientCallback<AssignTaskReply> &callback) {
    return Status::NotImplemented("");
  }

  /// Push an actor task directly from worker to worker.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status PushActorTask(std::unique_ptr<PushTaskRequest> request,
                                    const ClientCallback<PushTaskReply> &callback) {
    return Status::NotImplemented("");
  }

  /// Similar to PushActorTask, but sets no ordering constraint. This is used to
  /// push non-actor tasks directly to a worker.
  virtual ray::Status PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                                     const ClientCallback<PushTaskReply> &callback) {
    return Status::NotImplemented("");
  }

  /// Notify a wait has completed for direct actor call arguments.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status DirectActorCallArgWaitComplete(
      const DirectActorCallArgWaitCompleteRequest &request,
      const ClientCallback<DirectActorCallArgWaitCompleteReply> &callback) {
    return Status::NotImplemented("");
  }

  /// Ask the owner of an object about the object's current status.
  virtual ray::Status GetObjectStatus(
      const GetObjectStatusRequest &request,
      const ClientCallback<GetObjectStatusReply> &callback) {
    return Status::NotImplemented("");
  }

  /// Notify the owner of an object that the object has been pinned.
  virtual ray::Status WaitForObjectEviction(
      const WaitForObjectEvictionRequest &request,
      const ClientCallback<WaitForObjectEvictionReply> &callback) {
    return Status::NotImplemented("");
  }

  /// Tell this actor to exit immediately.
  virtual ray::Status KillActor(const KillActorRequest &request,
                                const ClientCallback<KillActorReply> &callback) {
    return Status::NotImplemented("");
  }

  virtual ray::Status GetCoreWorkerStats(
      const GetCoreWorkerStatsRequest &request,
      const ClientCallback<GetCoreWorkerStatsReply> &callback) {
    return Status::NotImplemented("");
  }

  virtual ray::Status LocalGC(const LocalGCRequest &request,
                              const ClientCallback<LocalGCReply> &callback) {
    return Status::NotImplemented("");
  }

  virtual ray::Status WaitForRefRemoved(
      const WaitForRefRemovedRequest &request,
      const ClientCallback<WaitForRefRemovedReply> &callback) {
    return Status::NotImplemented("");
  }

  virtual ~CoreWorkerClientInterface(){};
};

/// Client used for communicating with a remote worker server.
class CoreWorkerClient : public std::enable_shared_from_this<CoreWorkerClient>,
                         public CoreWorkerClientInterface {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the worker server.
  /// \param[in] port Port of the worker server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  CoreWorkerClient(const rpc::Address &address, ClientCallManager &client_call_manager)
      : addr_(address), client_call_manager_(client_call_manager) {
    grpc_client_ =
        std::unique_ptr<GrpcClient<CoreWorkerService>>(new GrpcClient<CoreWorkerService>(
            addr_.ip_address(), addr_.port(), client_call_manager));
  };

  const rpc::Address &Addr() const override { return addr_; }

  RPC_CLIENT_METHOD(CoreWorkerService, AssignTask, grpc_client_, override)

  RPC_CLIENT_METHOD(CoreWorkerService, DirectActorCallArgWaitComplete, grpc_client_,
                    override)

  RPC_CLIENT_METHOD(CoreWorkerService, GetObjectStatus, grpc_client_, override)

  RPC_CLIENT_METHOD(CoreWorkerService, KillActor, grpc_client_, override)

  RPC_CLIENT_METHOD(CoreWorkerService, WaitForObjectEviction, grpc_client_, override)

  RPC_CLIENT_METHOD(CoreWorkerService, GetCoreWorkerStats, grpc_client_, override)

  RPC_CLIENT_METHOD(CoreWorkerService, LocalGC, grpc_client_, override)

  RPC_CLIENT_METHOD(CoreWorkerService, WaitForRefRemoved, grpc_client_, override)

  ray::Status PushActorTask(std::unique_ptr<PushTaskRequest> request,
                            const ClientCallback<PushTaskReply> &callback) override {
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

  ray::Status PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                             const ClientCallback<PushTaskReply> &callback) override {
    request->set_sequence_number(-1);
    request->set_client_processed_up_to(-1);
    return INVOKE_RPC_CALL(CoreWorkerService, PushTask, *request, callback, grpc_client_);
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

      auto rpc_callback = [this, this_ptr, seq_no, task_size, callback](
                              Status status, const rpc::PushTaskReply &reply) {
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
      };

      INVOKE_RPC_CALL(CoreWorkerService, PushTask, *request, rpc_callback, grpc_client_);
    }

    if (!send_queue_.empty()) {
      RAY_LOG(DEBUG) << "client send queue size " << send_queue_.size();
    }
  }

 private:
  /// Protects against unsafe concurrent access from the callback thread.
  std::mutex mutex_;

  /// Address of the remote worker.
  rpc::Address addr_;

  /// The RPC client.
  std::unique_ptr<GrpcClient<CoreWorkerService>> grpc_client_;

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

#endif  // RAY_RPC_CORE_WORKER_CLIENT_H
