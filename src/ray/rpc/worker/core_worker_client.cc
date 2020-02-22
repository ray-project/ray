#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace rpc {

ray::Status CoreWorkerClient::PushActorTask(
    std::unique_ptr<PushTaskRequest> request,
    const ClientCallback<PushTaskReply> &callback) {
  request->set_sequence_number(request->task_spec().actor_task_spec().actor_counter());
  {
    absl::MutexLock lock(&mutex_);
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

ray::Status CoreWorkerClient::PushNormalTask(
    std::unique_ptr<PushTaskRequest> request,
    const ClientCallback<PushTaskReply> &callback) {
  request->set_sequence_number(-1);
  request->set_client_processed_up_to(-1);
  return INVOKE_RPC_CALL(CoreWorkerService, PushTask, *request, callback, grpc_client_);
}

WorkerAddress::WorkerAddress(const Address &address)
    : ip_address(address.ip_address()),
      port(address.port()),
      worker_id(WorkerID::FromBinary(address.worker_id())),
      raylet_id(ClientID::FromBinary(address.raylet_id())) {}

bool WorkerAddress::operator==(const WorkerAddress &other) const {
  return other.ip_address == ip_address && other.port == port &&
         other.worker_id == worker_id && other.raylet_id == raylet_id;
}

Address WorkerAddress::ToProto() const {
  Address addr;
  addr.set_raylet_id(raylet_id.Binary());
  addr.set_ip_address(ip_address);
  addr.set_port(port);
  addr.set_worker_id(worker_id.Binary());
  return addr;
}

const Address &CoreWorkerClientInterface::Addr() const {
  static const Address empty_addr_;
  return empty_addr_;
}

const Address &CoreWorkerClient::Addr() const { return addr_; }

void CoreWorkerClient::SendRequests() {
  absl::MutexLock lock(&mutex_);
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
                            Status status, const PushTaskReply &reply) {
      {
        absl::MutexLock lock(&mutex_);
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
CoreWorkerClientInterface::~CoreWorkerClientInterface() {}

CoreWorkerClient::CoreWorkerClient(const rpc::Address &address,
                                   ClientCallManager &client_call_manager)
    : addr_(address), client_call_manager_(client_call_manager) {
  grpc_client_ =
      std::unique_ptr<GrpcClient<CoreWorkerService>>(new GrpcClient<CoreWorkerService>(
          addr_.ip_address(), addr_.port(), client_call_manager));
}

}  // namespace rpc
}  // namespace ray
