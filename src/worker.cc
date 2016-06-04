#include "worker.h"

#include "utils.h"

#include <pynumbuf/serialize.h>

extern "C" {
  static PyObject *HaloError;
}

Status WorkerServiceImpl::ExecuteTask(ServerContext* context, const ExecuteTaskRequest* request, ExecuteTaskReply* reply) {
  task_ = request->task(); // Copy task
  HALO_LOG(HALO_INFO, "invoked task " << request->task().name());
  Task* taskptr = &task_;
  send_queue_.send(&taskptr);
  return Status::OK;
}

Worker::Worker(const std::string& worker_address, std::shared_ptr<Channel> scheduler_channel, std::shared_ptr<Channel> objstore_channel)
    : worker_address_(worker_address),
      scheduler_stub_(Scheduler::NewStub(scheduler_channel)),
      objstore_stub_(ObjStore::NewStub(objstore_channel)) {
  receive_queue_.connect(worker_address_, true);
  connected_ = true;
}

SubmitTaskReply Worker::submit_task(SubmitTaskRequest* request) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform submit_task, but connected_ = " << connected_ << ".");
  }
  SubmitTaskReply reply;
  ClientContext context;
  Status status = scheduler_stub_->SubmitTask(&context, *request, &reply);
  return reply;
}

void Worker::register_worker(const std::string& worker_address, const std::string& objstore_address) {
  RegisterWorkerRequest request;
  request.set_worker_address(worker_address);
  request.set_objstore_address(objstore_address);
  RegisterWorkerReply reply;
  ClientContext context;
  Status status = scheduler_stub_->RegisterWorker(&context, request, &reply);
  workerid_ = reply.workerid();
  objstoreid_ = reply.objstoreid();
  segmentpool_ = std::make_shared<MemorySegmentPool>(objstoreid_, false);
  request_obj_queue_.connect(std::string("queue:") + objstore_address + std::string(":obj"), false);
  std::string queue_name = std::string("queue:") + objstore_address + std::string(":worker:") + std::to_string(workerid_) + std::string(":obj");
  receive_obj_queue_.connect(queue_name, true);
  return;
}

void Worker::request_object(ObjRef objref) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform request_object, but connected_ = " << connected_ << ".");
  }
  RequestObjRequest request;
  request.set_workerid(workerid_);
  request.set_objref(objref);
  AckReply reply;
  ClientContext context;
  Status status = scheduler_stub_->RequestObj(&context, request, &reply);
  return;
}

ObjRef Worker::get_objref() {
  // first get objref for the new object
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform get_objref, but connected_ = " << connected_ << ".");
  }
  PushObjRequest push_request;
  PushObjReply push_reply;
  ClientContext push_context;
  Status push_status = scheduler_stub_->PushObj(&push_context, push_request, &push_reply);
  return push_reply.objref();
}

slice Worker::get_object(ObjRef objref) {
  // get_object assumes that objref is a canonical objref
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform get_object, but connected_ = " << connected_ << ".");
  }
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::GET;
  request.objref = objref;
  request_obj_queue_.send(&request);
  ObjHandle result;
  receive_obj_queue_.receive(&result);
  slice slice;
  slice.data = segmentpool_->get_address(result);
  slice.len = result.size();
  return slice;
}

// TODO(pcm): More error handling
// contained_objrefs is a vector of all the objrefs contained in obj
void Worker::put_object(ObjRef objref, const Obj* obj, std::vector<ObjRef> &contained_objrefs) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform put_object, but connected_ = " << connected_ << ".");
  }
  std::string data;
  obj->SerializeToString(&data); // TODO(pcm): get rid of this serialization
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::ALLOC;
  request.objref = objref;
  request.size = data.size();
  request_obj_queue_.send(&request);
  if (contained_objrefs.size() > 0) {
    HALO_LOG(HALO_REFCOUNT, "In put_object, calling increment_reference_count for contained objrefs");
    increment_reference_count(contained_objrefs); // Notify the scheduler that some object references are serialized in the objstore.
  }
  ObjHandle result;
  receive_obj_queue_.receive(&result);
  uint8_t* target = segmentpool_->get_address(result);
  std::memcpy(target, &data[0], data.size());
  request.type = ObjRequestType::WORKER_DONE;
  request.metadata_offset = 0;
  request_obj_queue_.send(&request);

  // Notify the scheduler about the objrefs that we are serializing in the objstore.
  AddContainedObjRefsRequest contained_objrefs_request;
  contained_objrefs_request.set_objref(objref);
  for (int i = 0; i < contained_objrefs.size(); ++i) {
    contained_objrefs_request.add_contained_objref(contained_objrefs[i]); // TODO(rkn): The naming here is bad
  }
  AckReply reply;
  ClientContext context;
  scheduler_stub_->AddContainedObjRefs(&context, contained_objrefs_request, &reply);
}

#define CHECK_ARROW_STATUS(s, msg)                              \
  do {                                                          \
    arrow::Status _s = (s);                                     \
    if (!_s.ok()) {                                             \
      std::string _errmsg = std::string(msg) + _s.ToString();   \
      PyErr_SetString(HaloError, _errmsg.c_str());            \
      return NULL;                                              \
    }                                                           \
  } while (0);

PyObject* Worker::put_arrow(ObjRef objref, PyObject* value) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform put_arrow, but connected_ = " << connected_ << ".");
  }
  ObjRequest request;
  pynumbuf::PythonObjectWriter writer;
  int64_t size;
  CHECK_ARROW_STATUS(writer.AssemblePayload(value), "error during AssemblePayload: ");
  CHECK_ARROW_STATUS(writer.GetTotalSize(&size), "error during GetTotalSize: ");
  request.workerid = workerid_;
  request.type = ObjRequestType::ALLOC;
  request.objref = objref;
  request.size = size;
  request_obj_queue_.send(&request);
  ObjHandle result;
  receive_obj_queue_.receive(&result);
  int64_t metadata_offset;
  uint8_t* address = segmentpool_->get_address(result);
  auto source = std::make_shared<BufferMemorySource>(address, size);
  CHECK_ARROW_STATUS(writer.Write(source.get(), &metadata_offset), "error during Write: ");
  request.type = ObjRequestType::WORKER_DONE;
  request.metadata_offset = metadata_offset;
  request_obj_queue_.send(&request);
  Py_RETURN_NONE;
}

PyObject* Worker::get_arrow(ObjRef objref) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform get_arrow, but connected_ = " << connected_ << ".");
  }
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::GET;
  request.objref = objref;
  request_obj_queue_.send(&request);
  ObjHandle result;
  receive_obj_queue_.receive(&result);
  uint8_t* address = segmentpool_->get_address(result);
  auto source = std::make_shared<BufferMemorySource>(address, result.size());
  PyObject* value;
  CHECK_ARROW_STATUS(pynumbuf::ReadPythonObjectFrom(source.get(), result.metadata_offset(), &value), "error during ReadPythonObjectFrom: ");
  return value;
}

bool Worker::is_arrow(ObjRef objref) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform is_arrow, but connected_ = " << connected_ << ".");
  }
  ObjRequest request;
  request.workerid = workerid_;
  request.type = ObjRequestType::GET;
  request.objref = objref;
  request_obj_queue_.send(&request);
  ObjHandle result;
  receive_obj_queue_.receive(&result);
  return result.metadata_offset() != 0;
}

void Worker::alias_objrefs(ObjRef alias_objref, ObjRef target_objref) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform alias_objrefs, but connected_ = " << connected_ << ".");
  }
  ClientContext context;
  AliasObjRefsRequest request;
  request.set_alias_objref(alias_objref);
  request.set_target_objref(target_objref);
  AckReply reply;
  scheduler_stub_->AliasObjRefs(&context, request, &reply);
}

void Worker::increment_reference_count(std::vector<ObjRef> &objrefs) {
  if (!connected_) {
    HALO_LOG(HALO_DEBUG, "Attempting to increment_reference_count for objrefs, but connected_ = " << connected_ << " so returning instead.");
    return;
  }
  if (objrefs.size() > 0) {
    ClientContext context;
    IncrementRefCountRequest request;
    for (int i = 0; i < objrefs.size(); ++i) {
      HALO_LOG(HALO_REFCOUNT, "Incrementing reference count for objref " << objrefs[i]);
      request.add_objref(objrefs[i]);
    }
    AckReply reply;
    scheduler_stub_->IncrementRefCount(&context, request, &reply);
  }
}

void Worker::decrement_reference_count(std::vector<ObjRef> &objrefs) {
  if (!connected_) {
    HALO_LOG(HALO_DEBUG, "Attempting to decrement_reference_count, but connected_ = " << connected_ << " so returning instead.");
    return;
  }
  if (objrefs.size() > 0) {
    ClientContext context;
    DecrementRefCountRequest request;
    for (int i = 0; i < objrefs.size(); ++i) {
      HALO_LOG(HALO_REFCOUNT, "Decrementing reference count for objref " << objrefs[i]);
      request.add_objref(objrefs[i]);
    }
    AckReply reply;
    scheduler_stub_->DecrementRefCount(&context, request, &reply);
  }
}

void Worker::register_function(const std::string& name, size_t num_return_vals) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform register_function, but connected_ = " << connected_ << ".");
  }
  ClientContext context;
  RegisterFunctionRequest request;
  request.set_fnname(name);
  request.set_num_return_vals(num_return_vals);
  request.set_workerid(workerid_);
  AckReply reply;
  scheduler_stub_->RegisterFunction(&context, request, &reply);
}

Task* Worker::receive_next_task() {
  Task* task;
  receive_queue_.receive(&task);
  return task;
}

void Worker::notify_task_completed() {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to perform notify_task_completed, but connected_ = " << connected_ << ".");
  }
  ClientContext context;
  WorkerReadyRequest request;
  request.set_workerid(workerid_);
  AckReply reply;
  scheduler_stub_->WorkerReady(&context, request, &reply);
}

void Worker::disconnect() {
  connected_ = false;
}

bool Worker::connected() {
  return connected_;
}

// TODO(rkn): Should we be using pointers or references? And should they be const?
void Worker::scheduler_info(ClientContext &context, SchedulerInfoRequest &request, SchedulerInfoReply &reply) {
  if (!connected_) {
    HALO_LOG(HALO_FATAL, "Attempting to get scheduler info, but connected_ = " << connected_ << ".");
  }
  scheduler_stub_->SchedulerInfo(&context, request, &reply);
}

// Communication between the WorkerServer and the Worker happens via a message
// queue. This is because the Python interpreter needs to be single threaded
// (in our case running in the main thread), whereas the WorkerService will
// run in a separate thread and potentially utilize multiple threads.
void Worker::start_worker_service() {
  const char* service_addr = worker_address_.c_str();
  worker_server_thread_ = std::thread([service_addr]() {
    std::string service_address(service_addr);
    std::string::iterator split_point = split_ip_address(service_address);
    std::string port;
    port.assign(split_point, service_address.end());
    WorkerServiceImpl service(service_address);
    ServerBuilder builder;
    builder.AddListeningPort(std::string("0.0.0.0:") + port, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    HALO_LOG(HALO_INFO, "worker server listening on " << service_address);
    server->Wait();
  });
}
