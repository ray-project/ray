#ifndef ORCHESTRA_SCHEDULER_H
#define ORCHESTRA_SCHEDULER_H


#include <deque>
#include <memory>
#include <algorithm>
#include <iostream>

#include <grpc++/grpc++.h>

#include "orchestra/orchestra.h"
#include "orchestra.grpc.pb.h"
#include "types.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerReader;
using grpc::ServerContext;
using grpc::Status;

using grpc::ClientContext;

using grpc::Channel;

struct WorkerHandle {
  std::shared_ptr<Channel> channel;
  std::unique_ptr<WorkerService::Stub> worker_stub;
  ObjStoreId objstoreid;
};

struct ObjStoreHandle {
  std::shared_ptr<Channel> channel;
  std::unique_ptr<ObjStore::Stub> objstore_stub;
  std::string address;
};

class SchedulerService : public Scheduler::Service {
public:
  Status RemoteCall(ServerContext* context, const RemoteCallRequest* request, RemoteCallReply* reply) override;
  Status PushObj(ServerContext* context, const PushObjRequest* request, PushObjReply* reply) override;
  Status PullObj(ServerContext* context, const PullObjRequest* request, AckReply* reply) override;
  Status RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) override;
  Status RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) override;
  Status RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) override;
  Status ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) override;
  Status WorkerReady(ServerContext* context, const WorkerReadyRequest* request, AckReply* reply) override;
  Status GetDebugInfo(ServerContext* context, const GetDebugInfoRequest* request, GetDebugInfoReply* reply) override;

  // ask an object store to send object to another objectstore
  void deliver_object(ObjRef objref, ObjStoreId from, ObjStoreId to);
  // assign a task to a worker
  void schedule();
  // execute a task on a worker and ship required object references
  void submit_task(std::unique_ptr<Call> call, WorkerId workerid);
  // checks if the dependencies of the task are met
  bool can_run(const Call& task);
  // register a worker and its object store (if it has not been registered yet)
  WorkerId register_worker(const std::string& worker_address, const std::string& objstore_address);
  // register a new object with the scheduler and return its object reference
  ObjRef register_new_object();
  // register the location of the object reference in the object table
  void add_location(ObjRef objref, ObjStoreId objstoreid);
  // get object store associated with a workerid
  ObjStoreId get_store(WorkerId workerid);
  // register a function with the scheduler
  void register_function(const std::string& name, WorkerId workerid, size_t num_return_vals);
  // get debugging information for the scheduler
  void debug_info(const GetDebugInfoRequest& request, GetDebugInfoReply* reply);
private:
  // pick an objectstore that holds a given object (needs protection by objtable_lock_)
  ObjStoreId pick_objstore(ObjRef objref);

  // Vector of all workers registered in the system. Their index in this vector
  // is the workerid.
  std::vector<WorkerHandle> workers_;
  std::mutex workers_lock_;
  // Vector of all workers that are currently idle.
  std::vector<WorkerId> avail_workers_;
  std::mutex avail_workers_lock_;
  // Vector of all object stores registered in the system. Their index in this
  // vector is the objstoreid.
  std::vector<ObjStoreHandle> objstores_;
  grpc::mutex objstores_lock_;
  // Mapping from objref to list of object stores where the object is stored.
  ObjTable objtable_;
  std::mutex objtable_lock_;
  // Hash map from function names to workers where the function is registered.
  FnTable fntable_;
  std::mutex fntable_lock_;
  // List of pending tasks.
  std::deque<std::unique_ptr<Call> > tasks_;
  std::mutex tasks_lock_;
};

#endif
