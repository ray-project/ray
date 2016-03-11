# include "worker.h"

Status WorkerServiceImpl::InvokeCall(ServerContext* context, const InvokeCallRequest* request, InvokeCallReply* reply) {
  call_ = request->call(); // Copy call
  ORCH_LOG(ORCH_INFO, "invoked task " << request->call().name());
  try {
    Call* callptr = &call_;
    message_queue mq(open_only, worker_address_.c_str());
    mq.send(&callptr, sizeof(Call*), 0);
  }
  catch(interprocess_exception &ex){
    message_queue::remove(worker_address_.c_str());
    std::cout << ex.what() << std::endl;
    // TODO: return Status;
  }
  message_queue::remove(worker_address_.c_str());
  return Status::OK;
}

RemoteCallReply Worker::remote_call(RemoteCallRequest* request) {
  RemoteCallReply reply;
  ClientContext context;
  Status status = scheduler_stub_->RemoteCall(&context, *request, &reply);
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
  return;
}

slice Worker::pull_object(ObjRef objref) {
  PullObjRequest request;
  request.set_workerid(workerid_);
  request.set_objref(objref);
  AckReply reply;
  ClientContext context;
  Status status = scheduler_stub_->PullObj(&context, request, &reply);
  return get_object(objref);
}

ObjRef Worker::push_object(const Obj* obj) {
  // first get objref for the new object
  PushObjRequest push_request;
  PushObjReply push_reply;
  ClientContext push_context;
  Status push_status = scheduler_stub_->PushObj(&push_context, push_request, &push_reply);
  ObjRef objref = push_reply.objref();
  // then stream the object to the object store
  put_object(objref, obj);
  return objref;
}

slice Worker::get_object(ObjRef objref) {
  ClientContext context;
  GetObjRequest request;
  request.set_objref(objref);
  GetObjReply reply;
  objstore_stub_->GetObj(&context, request, &reply);
  segment_ = managed_shared_memory(open_only, reply.bucket().c_str());
  slice slice;
  slice.data = static_cast<char*>(segment_.get_address_from_handle(reply.handle()));
  slice.len = reply.size();
  return slice;
}

// TODO: Do this with shared memory
void Worker::put_object(ObjRef objref, const Obj* obj) {
  ObjChunk chunk;
  std::string data;
  obj->SerializeToString(&data);
  size_t totalsize = data.size();
  ClientContext context;
  AckReply reply;
  std::unique_ptr<ClientWriter<ObjChunk> > writer(
    objstore_stub_->StreamObj(&context, &reply));
  const char* head = data.c_str();
  for (size_t i = 0; i < data.length(); i += CHUNK_SIZE) {
    chunk.set_objref(objref);
    chunk.set_totalsize(totalsize);
    chunk.set_data(head + i, std::min(CHUNK_SIZE, data.length() - i));
    if (!writer->Write(chunk)) {
      ORCH_LOG(ORCH_FATAL, "write failed during put_object");
      // TODO(pcm): better error handling
    }
  }
  writer->WritesDone();
  Status status = writer->Finish();
  // TODO(pcm): error handling
}

void Worker::register_function(const std::string& name, size_t num_return_vals) {
  ClientContext context;
  RegisterFunctionRequest request;
  request.set_fnname(name);
  request.set_num_return_vals(num_return_vals);
  request.set_workerid(workerid_);
  AckReply reply;
  scheduler_stub_->RegisterFunction(&context, request, &reply);
}

// Communication between the WorkerServer and the Worker happens via a message
// queue. This is because the Python interpreter needs to be single threaded
// (in our case running in the main thread), whereas the WorkerService will
// run in a separate thread and potentially utilize multiple threads.
void Worker::start_worker_service() {
  const char* server_address = worker_address_.c_str();
  worker_server_thread_ = std::thread([server_address]() {
    WorkerServiceImpl service(server_address);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    ORCH_LOG(ORCH_INFO, "worker server listening on " << server_address);
    server->Wait();
  });
}

Call* Worker::receive_next_task() {
  const char* message_queue_name = worker_address_.c_str();
  try {
    message_queue::remove(message_queue_name);
    message_queue mq(create_only, message_queue_name, 1, sizeof(Call*));
    unsigned int priority;
    message_queue::size_type recvd_size;
    Call* call;
    while (true) {
      mq.receive(&call, sizeof(Call*), recvd_size, priority);
      return call;
    }
  }
  catch(interprocess_exception &ex){
  	message_queue::remove(message_queue_name);
    std::cout << ex.what() << std::endl;
  }
}
