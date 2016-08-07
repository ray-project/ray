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

// These three constants are used to define the mode that a worker is running
// in. Right now, this is mostly used for determining how to print information
// about task failures.
enum Mode {SCRIPT_MODE, WORKER_MODE, PYTHON_MODE, SILENT_MODE};

class WorkerServiceImpl final : public WorkerService::Service {
public:
  WorkerServiceImpl(const std::string& worker_address, Mode mode);
  Status ExecuteTask(ServerContext* context, const ExecuteTaskRequest* request, AckReply* reply) override;
  Status ImportRemoteFunction(ServerContext* context, const ImportRemoteFunctionRequest* request, AckReply* reply) override;
  Status Die(ServerContext* context, const DieRequest* request, AckReply* reply) override;
  Status ImportReusableVariable(ServerContext* context, const ImportReusableVariableRequest* request, AckReply* reply) override;
  Status PrintErrorMessage(ServerContext* context, const PrintErrorMessageRequest* request, AckReply* reply) override;
private:
  std::string worker_address_;
  MessageQueue<WorkerMessage*> send_queue_;
  // This is true if the worker service is part of a driver process and false
  // if it is part of a worker process.
  Mode mode_;
};

class Worker {
 public:
  Worker(const std::string& scheduler_address);

  // Submit a remote task to the scheduler. If the function in the task is not
  // registered with the scheduler, we will sleep for retry_wait_milliseconds
  // and try to resubmit the task to the scheduler up to max_retries more times.
  SubmitTaskReply submit_task(SubmitTaskRequest* request, int max_retries = 120, int retry_wait_milliseconds = 500);
  // Requests the scheduler to kill workers
  bool kill_workers(ClientContext &context);
  // send request to the scheduler to register this worker
  void register_worker(const std::string& ip_address, const std::string& objstore_address, bool is_driver);
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
  // Notify the scheduler that a remote function has been imported successfully.
  void register_remote_function(const std::string& name, size_t num_return_vals);
  // Notify the scheduler that a failure has occurred.
  void notify_failure(FailedType type, const std::string& name, const std::string& error_message);
  // Start the worker server which accepts commands from the scheduler. For
  // workers, these commands are stored in the message queue, which is read by
  // the Python interpreter. For drivers, these commands are only for printing
  // error messages.
  void start_worker_service(Mode mode);
  // wait for next task from the RPC system. If null, it means there are no more tasks and the worker should shut down.
  std::unique_ptr<WorkerMessage> receive_next_message();
  // tell the scheduler that we are done with the current task and request the
  // next one.
  void notify_task_completed();
  // disconnect the worker
  void disconnect();
  // return connected_
  bool connected() { return connected_; }
  // get info about scheduler state
  void scheduler_info(ClientContext &context, SchedulerInfoRequest &request, SchedulerInfoReply &reply);
  // get task statuses from scheduler
  void task_info(ClientContext &context, TaskInfoRequest &request, TaskInfoReply &reply);
  // export function to workers
  bool export_remote_function(const std::string& function_name, const std::string& function);
  // export reusable variable to workers
  void export_reusable_variable(const std::string& name, const std::string& initializer, const std::string& reinitializer);
  // return the worker address
  const char* get_worker_address() { return worker_address_.c_str(); }

 private:
  bool connected_;
  const size_t CHUNK_SIZE = 8 * 1024;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  std::unique_ptr<std::thread> worker_server_thread_;
  MessageQueue<WorkerMessage*> receive_queue_;
  bip::managed_shared_memory segment_;
  WorkerId workerid_;
  ObjStoreId objstoreid_;
  std::string scheduler_address_;
  std::string objstore_address_;
  std::string worker_address_;
  MessageQueue<ObjRequest> request_obj_queue_;
  MessageQueue<ObjHandle> receive_obj_queue_;
  std::shared_ptr<MemorySegmentPool> segmentpool_;
};

#endif
