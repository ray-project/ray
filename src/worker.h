#ifndef RAY_WORKER_H
#define RAY_WORKER_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>

#include <Python.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

#include "ray.grpc.pb.h"
#include "ray/ray.h"
#include "ipc.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;

class WorkerServiceImpl final : public WorkerService::Service {
public:
  WorkerServiceImpl(const std::string& worker_address);
  Status ExecuteTask(ServerContext* context, const ExecuteTaskRequest* request, ExecuteTaskReply* reply) override;
  Status ImportFunction(ServerContext* context, const ImportFunctionRequest* request, ImportFunctionReply* reply) override;
  Status Die(ServerContext* context, const DieRequest* request, DieReply* reply) override;
  Status ImportReusableVariable(ServerContext* context, const ImportReusableVariableRequest* request, AckReply* reply) override;
private:
  std::string worker_address_;
  MessageQueue<WorkerMessage*> send_queue_;
};

class Worker {
 public:
  Worker(const std::string& worker_address, std::shared_ptr<Channel> scheduler_channel, std::shared_ptr<Channel> objstore_channel);

  // Submit a remote task to the scheduler. If the function in the task is not
  // registered with the scheduler, we will sleep for retry_wait_milliseconds
  // and try to resubmit the task to the scheduler up to max_retries more times.
  SubmitTaskReply submit_task(SubmitTaskRequest* request, int max_retries = 120, int retry_wait_milliseconds = 500);
  // Requests the scheduler to kill workers
  bool kill_workers(ClientContext &context);
  // send request to the scheduler to register this worker
  void register_worker(const std::string& worker_address, const std::string& objstore_address, bool is_driver);
  // get a new object ID that is registered with the scheduler
  ObjectID get_objectid();
  // request an object to be delivered to the local object store
  void request_object(ObjectID objectid);
  // stores an object to the local object store
  void put_object(ObjectID objectid, const Obj* obj, std::vector<ObjectID> &contained_objectids);
  // retrieve serialized object from local object store
  slice get_object(ObjectID objectid);
  // Allocates buffer for objectid with size of size
  const char* allocate_buffer(ObjectID objectid, int64_t size, SegmentId& segmentid);
  // Finishes buffer with segmentid and an offset of metadata_ofset
  PyObject* finish_buffer(ObjectID objectid, SegmentId segmentid, int64_t metadata_offset);
  // Gets the buffer for objectid
  const char* get_buffer(ObjectID objectid, int64_t& size, SegmentId& segmentid, int64_t& metadata_offset);
  // determine if the object stored in objectid is an arrow object // TODO(pcm): more general mechanism for this?
  bool is_arrow(ObjectID objectid);
  // unmap the segment containing an object from the local address space
  void unmap_object(ObjectID objectid);
  // make `alias_objectid` refer to the same object that `target_objectid` refers to
  void alias_objectids(ObjectID alias_objectid, ObjectID target_objectid);
  // increment the reference count for objectid
  void increment_reference_count(std::vector<ObjectID> &objectid);
  // decrement the reference count for objectid
  void decrement_reference_count(std::vector<ObjectID> &objectid);
  // register function with scheduler
  void register_function(const std::string& name, size_t num_return_vals);
  // start the worker server which accepts tasks from the scheduler and stores
  // it in the message queue, which is read by the Python interpreter
  void start_worker_service();
  // wait for next task from the RPC system. If null, it means there are no more tasks and the worker should shut down.
  std::unique_ptr<WorkerMessage> receive_next_message();
  // tell the scheduler that we are done with the current task and request the
  // next one, if task_succeeded is false, this tells the scheduler that the
  // task threw an exception
  void notify_task_completed(bool task_succeeded, std::string error_message);
  // disconnect the worker
  void disconnect();
  // return connected_
  bool connected();
  // get info about scheduler state
  void scheduler_info(ClientContext &context, SchedulerInfoRequest &request, SchedulerInfoReply &reply);
  // get task statuses from scheduler
  void task_info(ClientContext &context, TaskInfoRequest &request, TaskInfoReply &reply);
  // export function to workers
  bool export_function(const std::string& function);
  // export reusable variable to workers
  void export_reusable_variable(const std::string& name, const std::string& initializer, const std::string& reinitializer);

 private:
  bool connected_;
  const size_t CHUNK_SIZE = 8 * 1024;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  std::thread worker_server_thread_;
  MessageQueue<WorkerMessage*> receive_queue_;
  bip::managed_shared_memory segment_;
  WorkerId workerid_;
  ObjStoreId objstoreid_;
  std::string worker_address_;
  MessageQueue<ObjRequest> request_obj_queue_;
  MessageQueue<ObjHandle> receive_obj_queue_;
  std::shared_ptr<MemorySegmentPool> segmentpool_;
};

#endif
