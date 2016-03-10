#ifndef ORCHESTRA_WORKER_H
#define ORCHESTRA_WORKER_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>

using namespace boost::interprocess;

#include <grpc++/grpc++.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

#include "orchestra.grpc.pb.h"
#include "orchestra/orchestra.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;

class WorkerServiceImpl final : public WorkerService::Service {
public:
  WorkerServiceImpl(const std::string& worker_address)
    : worker_address_(worker_address) {}
  Status InvokeCall(ServerContext* context, const InvokeCallRequest* request, InvokeCallReply* reply) override;
private:
  std::string worker_address_;
  Call call_; // copy of the current call
};

class Worker {
 public:
  Worker(const std::string& worker_address, std::shared_ptr<Channel> scheduler_channel, std::shared_ptr<Channel> objstore_channel)
      : worker_address_(worker_address),
        scheduler_stub_(Scheduler::NewStub(scheduler_channel)),
        objstore_stub_(ObjStore::NewStub(objstore_channel))
    {}

  // submit a remote call to the scheduler
  RemoteCallReply remote_call(RemoteCallRequest* request);
  // send request to the scheduler to register this worker
  void register_worker(const std::string& worker_address, const std::string& objstore_address);
  // push object to local object store, register it with the server and return object reference
  ObjRef push_object(const Obj* obj);
  // pull object from a potentially remote object store
  slice pull_object(ObjRef objref);
  // stores an object to the local object store
  void put_object(ObjRef objref, const Obj* obj);
  // retrieve serialized object from local object store
  slice get_object(ObjRef objref);
  // register function with scheduler
  void register_function(const std::string& name, size_t num_return_vals);
  // start the worker server which accepts tasks from the scheduler and stores
  // it in the message queue, which is read by the Python interpreter
  void start_worker_service();
  // wait for next task from the RPC system
  Call* receive_next_task();

 private:
  const size_t CHUNK_SIZE = 8 * 1024;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  std::unique_ptr<ObjStore::Stub> objstore_stub_;
  std::thread worker_server_thread_;
  std::thread other_thread_;
  managed_shared_memory segment_;
  WorkerId workerid_;
  std::string worker_address_;
};

#endif
