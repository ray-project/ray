#ifndef ORCHESTRA_WORKER_H
#define ORCHESTRA_WORKER_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

#include "orchestra.grpc.pb.h"
#include "orchestra/orchestra.h"
#include "ipc.h"
#include "serialize.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;

class WorkerServiceImpl final : public WorkerService::Service {
public:
  WorkerServiceImpl(const std::string& worker_address)
    : worker_address_(worker_address) {
    send_queue_.connect(worker_address_, false);
  }
  Status InvokeCall(ServerContext* context, const InvokeCallRequest* request, InvokeCallReply* reply) override;
private:
  std::string worker_address_;
  Call call_; // copy of the current call
  MessageQueue<Call*> send_queue_;
};

class Worker {
 public:
  Worker(const std::string& worker_address, std::shared_ptr<Channel> scheduler_channel, std::shared_ptr<Channel> objstore_channel);

  // submit a remote call to the scheduler
  RemoteCallReply remote_call(RemoteCallRequest* request);
  // send request to the scheduler to register this worker
  void register_worker(const std::string& worker_address, const std::string& objstore_address);
  // get a new object reference that is registered with the scheduler
  ObjRef get_objref();
  // request an object to be delivered to the local object store
  void request_object(ObjRef objref);
  // stores an object to the local object store
  void put_object(ObjRef objref, const Obj* obj, std::vector<ObjRef> &contained_objrefs);
  // retrieve serialized object from local object store
  slice get_object(ObjRef objref);
  // stores an arrow object to the local object store
  // FIXME(pcm): Once we have structs in arrow, get rid of the memcpy here
  void put_arrow(ObjRef objref, PyArrayObject* array);
  // gets an arrow object from the local object store
  PyArrayObject* get_arrow(ObjRef objref);
  // determine if the object stored in objref is an arrow object // TODO(pcm): more general mechanism for this?
  bool is_arrow(ObjRef objref);
  // make `alias_objref` refer to the same object that `target_objref` refers to
  void alias_objrefs(ObjRef alias_objref, ObjRef target_objref);
  // increment the reference count for objref
  void increment_reference_count(std::vector<ObjRef> &objref);
  // decrement the reference count for objref
  void decrement_reference_count(std::vector<ObjRef> &objref);
  // register function with scheduler
  void register_function(const std::string& name, size_t num_return_vals);
  // start the worker server which accepts tasks from the scheduler and stores
  // it in the message queue, which is read by the Python interpreter
  void start_worker_service();
  // wait for next task from the RPC system
  Call* receive_next_task();
  // tell the scheduler that we are done with the current task and request the next one
  void notify_task_completed();
  // disconnect the worker
  void disconnect();
  // return connected_
  bool connected();
  // get info about scheduler state
  void scheduler_info(ClientContext &context, SchedulerInfoRequest &request, SchedulerInfoReply &reply);

 private:
  bool connected_;
  const size_t CHUNK_SIZE = 8 * 1024;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  std::unique_ptr<ObjStore::Stub> objstore_stub_;
  std::thread worker_server_thread_;
  MessageQueue<Call*> receive_queue_;
  managed_shared_memory segment_;
  WorkerId workerid_;
  std::string worker_address_;
  MessageQueue<ObjRequest> request_obj_queue_;
  MessageQueue<ObjHandle> receive_obj_queue_;
  MemorySegmentPool segmentpool_;
};

#endif
