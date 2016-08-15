#include "worker.h"

#include <atomic>
#include <random>
#include <chrono>
#include <thread>

#include "utils.h"

extern "C" {
  static PyObject *RayError;
}

inline WorkerServiceImpl::WorkerServiceImpl(const std::string& send_queue_name, Mode mode)
  : mode_(mode) {
  RAY_LOG(RAY_INFO, "Worker service connecting to queue " << send_queue_name);
  RAY_CHECK(send_queue_.connect(send_queue_name, false), "error connecting send_queue_");
}

Status WorkerServiceImpl::ExecuteTask(ServerContext* context, const ExecuteTaskRequest* request, AckReply* reply) {
  RAY_CHECK(mode_ == Mode::WORKER_MODE, "ExecuteTask can only be called on workers.");
  RAY_LOG(RAY_INFO, "invoked task " << request->task().name());
  std::unique_ptr<WorkerMessage> message(new WorkerMessage());
  message->mutable_task()->CopyFrom(request->task());
  {
    WorkerMessage* message_ptr = message.get();
    RAY_CHECK(send_queue_.send(&message_ptr), "Failed to send message from the worker service to the worker because the message queue was full.");
  }
  message.release();
  return Status::OK;
}

Status WorkerServiceImpl::ImportRemoteFunction(ServerContext* context, const ImportRemoteFunctionRequest* request, AckReply* reply) {
  RAY_CHECK(mode_ == Mode::WORKER_MODE, "ImportRemoteFunction can only be called on workers.");
  std::unique_ptr<WorkerMessage> message(new WorkerMessage());
  message->mutable_function()->CopyFrom(request->function());
  RAY_LOG(RAY_INFO, "importing function");
  {
    WorkerMessage* message_ptr = message.get();
    RAY_CHECK(send_queue_.send(&message_ptr), "Failed to send message from the worker service to the worker because the message queue was full.");
  }
  message.release();
  return Status::OK;
}

Status WorkerServiceImpl::ImportReusableVariable(ServerContext* context, const ImportReusableVariableRequest* request, AckReply* reply) {
  RAY_CHECK(mode_ == Mode::WORKER_MODE, "ImportReusableVariable can only be called on workers.");
  std::unique_ptr<WorkerMessage> message(new WorkerMessage());
  message->mutable_reusable_variable()->CopyFrom(request->reusable_variable());
  RAY_LOG(RAY_INFO, "importing reusable variable");
  {
    WorkerMessage* message_ptr = message.get();
    RAY_CHECK(send_queue_.send(&message_ptr), "Failed to send message from the worker service to the worker because the message queue was full.");
  }
  message.release();
  return Status::OK;
}

Status WorkerServiceImpl::Die(ServerContext* context, const DieRequest* request, AckReply* reply) {
  RAY_CHECK(mode_ == Mode::WORKER_MODE, "Die can only be called on workers.");
  WorkerMessage* message_ptr = NULL;
  RAY_CHECK(send_queue_.send(&message_ptr), "Failed to send message from the worker service to the worker because the message queue was full.");
  return Status::OK;
}

Status WorkerServiceImpl::PrintErrorMessage(ServerContext* context, const PrintErrorMessageRequest* request, AckReply* reply) {
  RAY_CHECK(mode_ != Mode::WORKER_MODE, "PrintErrorMessage can only be called on drivers.");
  if (mode_ == Mode::SILENT_MODE) {
    // Do not log error messages in this case. This is just used for the tests.
    return Status::OK;
  }
  const Failure failure = request->failure();
  WorkerId workerid = failure.workerid();
  if (failure.type() == FailedType::FailedTask) {
    // A task threw an exception while executing.
    std::cout << "Error: Worker " << workerid << " failed to execute function " << failure.name() << ". Failed with error message:\n" << failure.error_message() << std::endl;
  } else if (failure.type() == FailedType::FailedRemoteFunctionImport) {
    // An exception was thrown while a remote function was being imported.
    std::cout << "Error: Worker " << workerid << " failed to import remote function " << failure.name() << ", failed with error message:\n" << failure.error_message() << std::endl;
  } else if (failure.type() == FailedType::FailedReusableVariableImport) {
    // An exception was thrown while a reusable variable was being imported.
    std::cout << "Error: Worker " << workerid << " failed to import reusable variable " << failure.name() << ", failed with error message:\n" << failure.error_message() << std::endl;
  } else if (failure.type() == FailedType::FailedReinitializeReusableVariable) {
    // An exception was thrown while a reusable variable was being reinitialized.
    std::cout << "Error: Worker " << workerid << " failed to reinitialize a reusable variable after running remote function " << failure.name() << ", failed with error message:\n" << failure.error_message() << std::endl;
  } else {
    RAY_CHECK(false, "This code should be unreachable.")
  }
  return Status::OK;
}

Worker::Worker(const std::string& node_ip_address, const std::string& scheduler_address, Mode mode)
    : scheduler_address_(scheduler_address),
      node_ip_address_(node_ip_address),
      mode_(mode) {
  auto scheduler_channel = grpc::CreateChannel(scheduler_address, grpc::InsecureChannelCredentials());
  scheduler_stub_ = Scheduler::NewStub(scheduler_channel);
  // Generate a random string to use for naming the message queue to avoid
  // collisions with message queues created by other workers.
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<int> queue_name_generator(0, 10000000);
  receive_queue_name_ = "worker_receive_queue:" + std::to_string(queue_name_generator(rng));
  RAY_LOG(RAY_INFO, "Worker creating queue " << receive_queue_name_);
  RAY_CHECK(receive_queue_.connect(receive_queue_name_, true), "error connecting receive_queue_");
}


SubmitTaskReply Worker::submit_task(SubmitTaskRequest* request, int max_retries, int retry_wait_milliseconds) {
  RAY_CHECK(connected_, "Attempted to perform submit_task but failed.");
  SubmitTaskReply reply;
  request->set_workerid(workerid_);
  for (int i = 0; i < 1 + max_retries; ++i) {
    ClientContext context;
    RAY_CHECK_GRPC(scheduler_stub_->SubmitTask(&context, *request, &reply));
    if (reply.function_registered()) {
      break;
    }
    RAY_LOG(RAY_INFO, "The function " << request->task().name() << " was not registered, so attempting to resubmit the task.");
    std::this_thread::sleep_for(std::chrono::milliseconds(retry_wait_milliseconds));
  }
  return reply;
}

bool Worker::kill_workers(ClientContext &context) {
  KillWorkersRequest request;
  KillWorkersReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->KillWorkers(&context, request, &reply));
  return reply.success();
}

void Worker::register_worker(const std::string& node_ip_address, const std::string& objstore_address, bool is_driver) {
  if (mode_ == Mode::WORKER_MODE) {
    start_worker_service(mode_);
    RAY_CHECK(!worker_address_.empty(), "The worker address is empty. This should be initialized by start_worker_service, so it is possible that the thread synchronization failed.")
  }
  unsigned int retry_wait_milliseconds = 20;
  RegisterWorkerRequest request;
  request.set_node_ip_address(node_ip_address);
  request.set_worker_address(worker_address_);
  // The object store address can be the empty string, in which case the
  // scheduler will assign an object store address.
  request.set_objstore_address(objstore_address);
  request.set_is_driver(is_driver);
  RegisterWorkerReply reply;
  Status status;
  // TODO: HACK: retrying is a hack
  for (int i = 0; i < 5; ++i) {
    ClientContext context;
    status = scheduler_stub_->RegisterWorker(&context, request, &reply);
    if (status.error_code() != grpc::UNAVAILABLE) {
      break;
    }
    // Note that each pass through the loop may take substantially longer than
    // retry_wait_milliseconds because grpc may do its own retrying.
    std::this_thread::sleep_for(std::chrono::milliseconds(retry_wait_milliseconds));
  }
  RAY_CHECK_GRPC(status);
  workerid_ = reply.workerid();
  objstoreid_ = reply.objstoreid();
  objstore_address_ = reply.objstore_address();
  segmentpool_ = std::make_shared<MemorySegmentPool>(objstoreid_, objstore_address_, false);
  // Connect to the queue for sending requests to the object store.
  std::string request_obj_queue_name = std::string("queue:") + objstore_address_ + std::string(":obj");
  RAY_LOG(RAY_INFO, "Worker connecting to queue with name " << request_obj_queue_name << " to send requests to the object store.");
  RAY_CHECK(request_obj_queue_.connect(request_obj_queue_name, false), "error connecting request_obj_queue_");
  // Create a queue for receiving messages from the object store.
  std::string receive_obj_queue_name = std::string("queue:") + objstore_address_ + std::string(":worker:") + std::to_string(workerid_) + std::string(":obj");
  RAY_LOG(RAY_INFO, "Worker creating queue with name " << receive_obj_queue_name << " to receive messages from the object store.");
  RAY_CHECK(receive_obj_queue_.connect(receive_obj_queue_name, true), "error connecting receive_obj_queue_");
  connected_ = true;
  return;
}

void Worker::request_object(ObjectID objectid) {
  RAY_CHECK(connected_, "Attempted to perform request_object but failed.");
  RequestObjRequest request;
  request.set_workerid(workerid_);
  request.set_objectid(objectid);
  AckReply reply;
  ClientContext context;
  RAY_CHECK_GRPC(scheduler_stub_->RequestObj(&context, request, &reply));
  return;
}

ObjectID Worker::get_objectid() {
  // first get objectid for the new object
  RAY_CHECK(connected_, "Attempted to perform get_objectid but failed.");
  PutObjRequest request;
  request.set_workerid(workerid_);
  PutObjReply reply;
  ClientContext context;
  RAY_CHECK_GRPC(scheduler_stub_->PutObj(&context, request, &reply));
  return reply.objectid();
}

slice Worker::get_object(ObjectID objectid) {
  // get_object assumes that objectid is a canonical objectid
  RAY_CHECK(connected_, "Attempted to perform get_object but failed.");
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::GET;
  request.objectid = objectid;
  RAY_CHECK(request_obj_queue_.send(&request), "Failed to send request from the worker to the object store because the message queue was full.");
  ObjHandle result;
  RAY_CHECK(receive_obj_queue_.receive(&result), "error receiving over IPC");
  slice slice;
  slice.data = segmentpool_->get_address(result);
  slice.len = result.size();
  slice.segmentid = result.segmentid();
  return slice;
}

// TODO(pcm): More error handling
// contained_objectids is a vector of all the objectids contained in obj
void Worker::put_object(ObjectID objectid, const Obj* obj, std::vector<ObjectID> &contained_objectids) {
  RAY_CHECK(connected_, "Attempted to perform put_object but failed.");
  std::string data;
  obj->SerializeToString(&data); // TODO(pcm): get rid of this serialization
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::ALLOC;
  request.objectid = objectid;
  request.size = data.size();
  RAY_CHECK(request_obj_queue_.send(&request), "error sending over IPC");
  if (contained_objectids.size() > 0) {
    RAY_LOG(RAY_REFCOUNT, "In put_object, calling increment_reference_count for contained objectids");
    increment_reference_count(contained_objectids); // Notify the scheduler that some object references are serialized in the objstore.
  }
  ObjHandle result;
  RAY_CHECK(receive_obj_queue_.receive(&result), "error receiving over IPC");
  uint8_t* target = segmentpool_->get_address(result);
  std::memcpy(target, data.data(), data.size());
  // We immediately unmap here; if the object is going to be accessed again, it will be mapped again;
  // This is reqired because we do not have a mechanism to unmap the object later.
  segmentpool_->unmap_segment(result.segmentid());
  request.type = ObjRequestType::WORKER_DONE;
  request.metadata_offset = 0;
  RAY_CHECK(request_obj_queue_.send(&request), "Failed to send request from the worker to the object store because the message queue was full.");

  // Notify the scheduler about the objectids that we are serializing in the objstore.
  AddContainedObjectIDsRequest contained_objectids_request;
  contained_objectids_request.set_objectid(objectid);
  for (int i = 0; i < contained_objectids.size(); ++i) {
    contained_objectids_request.add_contained_objectid(contained_objectids[i]); // TODO(rkn): The naming here is bad
  }
  AckReply reply;
  ClientContext context;
   RAY_CHECK_GRPC(scheduler_stub_->AddContainedObjectIDs(&context, contained_objectids_request, &reply));
}

#define CHECK_ARROW_STATUS(s, msg)                              \
  do {                                                          \
    arrow::Status _s = (s);                                     \
    if (!_s.ok()) {                                             \
      std::string _errmsg = std::string(msg) + _s.ToString();   \
      PyErr_SetString(RayError, _errmsg.c_str());            \
      return NULL;                                              \
    }                                                           \
  } while (0);

const char* Worker::allocate_buffer(ObjectID objectid, int64_t size, SegmentId& segmentid) {
  RAY_CHECK(connected_, "Attempted to perform put_arrow but failed.");
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::ALLOC;
  request.objectid = objectid;
  request.size = size;
  RAY_CHECK(request_obj_queue_.send(&request), "Failed to send request from the worker to the object store because the message queue was full.");
  ObjHandle result;
  RAY_CHECK(receive_obj_queue_.receive(&result), "error receiving over IPC");
  const char* address = reinterpret_cast<const char*>(segmentpool_->get_address(result));
  segmentid = result.segmentid();
  return address;
}

PyObject* Worker::finish_buffer(ObjectID objectid, SegmentId segmentid, int64_t metadata_offset) {
  segmentpool_->unmap_segment(segmentid);
  ObjRequest request;
  request.workerid = workerid_;
  request.objectid = objectid;
  request.type = ObjRequestType::WORKER_DONE;
  request.metadata_offset = metadata_offset;
  RAY_CHECK(request_obj_queue_.send(&request), "Failed to send request from the worker to the object store because the message queue was full.");
  Py_RETURN_NONE;
}

const char* Worker::get_buffer(ObjectID objectid, int64_t &size, SegmentId& segmentid, int64_t& metadata_offset) {
  RAY_CHECK(connected_, "Attempted to perform get_arrow but failed.");
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::GET;
  request.objectid = objectid;
  RAY_CHECK(request_obj_queue_.send(&request), "Failed to send request from the worker to the object store because the message queue was full.");
  ObjHandle result;
  RAY_CHECK(receive_obj_queue_.receive(&result), "error receiving over IPC");
  const char* address = reinterpret_cast<const char*>(segmentpool_->get_address(result));
  size = result.size();
  segmentid = result.segmentid();
  metadata_offset = result.metadata_offset();
  return address;
}

bool Worker::is_arrow(ObjectID objectid) {
  RAY_CHECK(connected_, "Attempted to perform is_arrow but failed.");
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::GET;
  request.objectid = objectid;
  RAY_CHECK(request_obj_queue_.send(&request), "Failed to send request from the worker to the object store because the message queue was full.");
  ObjHandle result;
  RAY_CHECK(receive_obj_queue_.receive(&result), "error receiving over IPC");
  return result.metadata_offset() != 0;
}

void Worker::unmap_object(ObjectID objectid) {
  if (!connected_) {
    RAY_LOG(RAY_DEBUG, "Attempted to perform unmap_object but failed.");
    return;
  }
  segmentpool_->unmap_segment(objectid);
}

void Worker::alias_objectids(ObjectID alias_objectid, ObjectID target_objectid) {
  RAY_CHECK(connected_, "Attempted to perform alias_objectids but failed.");
  ClientContext context;
  AliasObjectIDsRequest request;
  request.set_alias_objectid(alias_objectid);
  request.set_target_objectid(target_objectid);
  AckReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->AliasObjectIDs(&context, request, &reply));
}

void Worker::increment_reference_count(std::vector<ObjectID> &objectids) {
  if (!connected_) {
    RAY_LOG(RAY_DEBUG, "Attempting to increment_reference_count for objectids, but connected_ = " << connected_ << " so returning instead.");
    return;
  }
  if (objectids.size() > 0) {
    ClientContext context;
    IncrementRefCountRequest request;
    for (int i = 0; i < objectids.size(); ++i) {
      RAY_LOG(RAY_REFCOUNT, "Incrementing reference count for objectid " << objectids[i]);
      request.add_objectid(objectids[i]);
    }
    AckReply reply;
    RAY_CHECK_GRPC(scheduler_stub_->IncrementRefCount(&context, request, &reply));
  }
}

void Worker::decrement_reference_count(std::vector<ObjectID> &objectids) {
  if (!connected_) {
    RAY_LOG(RAY_DEBUG, "Attempting to decrement_reference_count, but connected_ = " << connected_ << " so returning instead.");
    return;
  }
  if (objectids.size() > 0) {
    ClientContext context;
    DecrementRefCountRequest request;
    for (int i = 0; i < objectids.size(); ++i) {
      RAY_LOG(RAY_REFCOUNT, "Decrementing reference count for objectid " << objectids[i]);
      request.add_objectid(objectids[i]);
    }
    AckReply reply;
    RAY_CHECK_GRPC(scheduler_stub_->DecrementRefCount(&context, request, &reply));
  }
}

void Worker::register_remote_function(const std::string& name, size_t num_return_vals) {
  RAY_CHECK(connected_, "Attempted to perform register_function but failed.");
  ClientContext context;
  RegisterRemoteFunctionRequest request;
  request.set_workerid(workerid_);
  request.set_function_name(name);
  request.set_num_return_vals(num_return_vals);
  AckReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->RegisterRemoteFunction(&context, request, &reply));
}

void Worker::notify_failure(FailedType type, const std::string& name, const std::string& error_message) {
  RAY_CHECK(connected_, "Attempted to perform notify_failure but failed.");
  ClientContext context;
  NotifyFailureRequest request;
  request.mutable_failure()->set_type(type);
  request.mutable_failure()->set_workerid(workerid_);
  request.mutable_failure()->set_worker_address(worker_address_);
  request.mutable_failure()->set_name(name);
  request.mutable_failure()->set_error_message(error_message);
  AckReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->NotifyFailure(&context, request, &reply));
}

std::unique_ptr<WorkerMessage> Worker::receive_next_message() {
  WorkerMessage* message_ptr;
  RAY_CHECK(receive_queue_.receive(&message_ptr), "error receiving over IPC");
  return std::unique_ptr<WorkerMessage>(message_ptr);
}

void Worker::ready_for_new_task() {
  RAY_CHECK(connected_, "Attempted to perform ready_for_new_task but failed.");
  ClientContext context;
  ReadyForNewTaskRequest request;
  request.set_workerid(workerid_);
  AckReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->ReadyForNewTask(&context, request, &reply));
}

void Worker::disconnect() {
  connected_ = false;
  // Shut down the worker service. This will cause the call to server->Wait() to
  // return.
  // server_ptr_->Shutdown();
  // Wait for the thread that launched the worker service to return.
  // worker_server_thread_.join();
}

// TODO(rkn): Should we be using pointers or references? And should they be const?
void Worker::scheduler_info(ClientContext &context, SchedulerInfoRequest &request, SchedulerInfoReply &reply) {
  RAY_CHECK(connected_, "Attempted to get scheduler info but failed.");
  RAY_CHECK_GRPC(scheduler_stub_->SchedulerInfo(&context, request, &reply));
}

void Worker::task_info(ClientContext &context, TaskInfoRequest &request, TaskInfoReply &reply) {
  RAY_CHECK(connected_, "Attempted to get worker info but failed.");
  RAY_CHECK_GRPC(scheduler_stub_->TaskInfo(&context, request, &reply));
}

bool Worker::export_remote_function(const std::string& function_name, const std::string& function) {
  RAY_CHECK(connected_, "Attempted to export function but failed.");
  ClientContext context;
  ExportRemoteFunctionRequest request;
  request.mutable_function()->set_name(function_name);
  request.mutable_function()->set_implementation(function);
  AckReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->ExportRemoteFunction(&context, request, &reply));
  return true;
}

void Worker::export_reusable_variable(const std::string& name, const std::string& initializer, const std::string& reinitializer) {
  RAY_CHECK(connected_, "Attempted to export reusable variable but failed.");
  ClientContext context;
  ExportReusableVariableRequest request;
  request.mutable_reusable_variable()->set_name(name);
  request.mutable_reusable_variable()->mutable_initializer()->set_implementation(initializer);
  request.mutable_reusable_variable()->mutable_reinitializer()->set_implementation(reinitializer);
  AckReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->ExportReusableVariable(&context, request, &reply));
}

// Communication between the WorkerServer and the Worker happens via a message
// queue. This is because the Python interpreter needs to be single threaded
// (in our case running in the main thread), whereas the WorkerService will
// run in a separate thread and potentially utilize multiple threads.
void Worker::start_worker_service(Mode mode) {
  // Use atomics so the worker service thread can signal the outside thread that
  // the worker service has been started.
  std::atomic_bool worker_service_started;
  worker_service_started.store(false);
  // Launch a new thread for running the worker service. We store this as a
  // field so that we can clean it up when we disconnect the worker.
  worker_server_thread_ = std::thread([this, mode, &worker_service_started]() {
    // Create the worker service.
    WorkerServiceImpl service(receive_queue_name_, mode);
    ServerBuilder builder;
    // Let GRPC choose an unused port.
    int port;
    builder.AddListeningPort(std::string("0.0.0.0:0"), grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (server == nullptr) {
      RAY_CHECK(false, "Failed to create the worker service.");
    }
    worker_address_ = node_ip_address_ + ":" + std::to_string(port);
    server_ptr_ = server.get();
    RAY_LOG(RAY_INFO, "worker server listening at " << worker_address_);
    worker_service_started.store(true);
    // Wait for work and process work. This method does not return until
    // Shutdown is called from a different thread.
    server->Wait();
    RAY_LOG(RAY_INFO, "Worker service thread returning.")
  });
  // Wait for the worker service to start. This essentially implements a
  // condition variable using atomics, but that failed on Mac OS X on Travis.
  while (!worker_service_started.load()) {
    RAY_LOG(RAY_INFO, "Looping while waiting for the worker service to start.");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}
