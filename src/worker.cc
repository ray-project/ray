# include "worker.h"

Status WorkerServiceImpl::InvokeCall(ServerContext* context, const InvokeCallRequest* request, InvokeCallReply* reply) {
  call_ = request->call();
  std::cout << "invoke call request" << std::endl;
  try {
    Call* callptr = &call_;
    message_queue mq(open_only, worker_address_.c_str());
    std::cout << "before send: num args" << call_.arg_size() << std::endl;
    mq.send(&callptr, sizeof(Call*), 0);
  }
  catch(interprocess_exception &ex){
    message_queue::remove(worker_address_.c_str());
    std::cout << ex.what() << std::endl;
    // TODO: return Status;
  }
  message_queue::remove(worker_address_.c_str());
  std::cout << "notified server" << std::endl;
  return Status::OK;
}

size_t Worker::remote_call(RemoteCallRequest* request) {
  RemoteCallReply reply;
  ClientContext context;
  Status status = scheduler_stub_->RemoteCall(&context, *request, &reply);
  // TODO: Return results: return reply.result(0);
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

ObjRef Worker::push_obj(Obj* obj) {
  // first get objref for the new object
  PushObjRequest push_request;
  PushObjReply push_reply;
  ClientContext push_context;
  Status push_status = scheduler_stub_->PushObj(&push_context, push_request, &push_reply);
  ObjRef objref = push_reply.objref();
  // then stream the object to the object store
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
    std::cout << "chunk totalsize" << std::endl;
    chunk.set_totalsize(totalsize);
    chunk.set_data(head + i, std::min(CHUNK_SIZE, data.length() - i));
    if (!writer->Write(chunk)) {
      std::cout << "write failed" << std::endl;
      // TODO: Better error handling: throw std::runtime_error("write failed");
    }
  }
  writer->WritesDone();
  Status status = writer->Finish();
  return objref;
}

slice Worker::get_serialized_obj(ObjRef objref) {
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
    std::cout << "WorkerServer listening on " << server_address << std::endl;
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
      std::cout << "got call" << call << std::endl;
      std::cout << "after send: num args" << call->arg_size() << std::endl;
      return call;
    }
  }
  catch(interprocess_exception &ex){
  	message_queue::remove(message_queue_name);
    std::cout << ex.what() << std::endl;
  }
}
