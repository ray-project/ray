#include "objstore.h"

const size_t ObjStoreClient::CHUNK_SIZE = 8 * 1024;

// this method needs to be protected by a objstore_lock_
Status ObjStoreClient::upload_data_to(slice data, ObjRef objref, ObjStore::Stub& stub) {
  ObjChunk chunk;
  ClientContext context;
  AckReply reply;
  std::unique_ptr<ClientWriter<ObjChunk> > writer(stub.StreamObj(&context, &reply));
  const char* head = data.data;
  for (size_t i = 0; i < data.len; i += CHUNK_SIZE) {
    chunk.set_objref(objref);
    chunk.set_totalsize(data.len);
    chunk.set_data(head + i, std::min(CHUNK_SIZE, data.len - i));
    if (!writer->Write(chunk)) {
      ORCH_LOG(ORCH_FATAL, "stream connection prematurely closed")
    }
  }
  writer->WritesDone();
  return writer->Finish();
}

ObjStoreService::ObjStoreService(const std::string& objstore_address, std::shared_ptr<Channel> scheduler_channel)
  : scheduler_stub_(Scheduler::NewStub(scheduler_channel)) {
  ClientContext context;
  RegisterObjStoreRequest request;
  request.set_address(objstore_address);
  RegisterObjStoreReply reply;
  scheduler_stub_->RegisterObjStore(&context, request, &reply);
  objstoreid_ = reply.objstoreid();
}

ObjStoreService::~ObjStoreService() {
  for (const auto& segment_name : memory_names_) {
    shared_memory_object::remove(segment_name.c_str());
  }
}

// this method needs to be protected by a memory_lock_
void ObjStoreService::allocate_memory(ObjRef objref, size_t size) {
  std::ostringstream stream;
  stream << "obj-" << memory_names_.size();
  std::string name = stream.str();
  // Make sure that the name is not taken yet
  shared_memory_object::remove(name.c_str());
  memory_names_.push_back(name);
  // Make room for boost::interprocess metadata
  size_t new_size = (size / page_size + 2) * page_size;
  shared_object& object = memory_[objref];
  object.name = name;
  object.memory = std::make_shared<managed_shared_memory>(create_only, name.c_str(), new_size);
  object.ptr.data = static_cast<char*>(memory_[objref].memory->allocate(size));
  object.ptr.len = size;
}

// this method needs to be protected by a objstores_lock_
ObjStore::Stub& ObjStoreService::get_objstore_stub(const std::string& objstore_address) {
  auto iter = objstores_.find(objstore_address);
  if (iter != objstores_.end())
    return *(iter->second);
  auto channel = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
  objstores_.emplace(objstore_address, ObjStore::NewStub(channel));
  return *objstores_[objstore_address];
}

Status ObjStoreService::DeliverObj(ServerContext* context, const DeliverObjRequest* request, AckReply* reply) {
  std::lock_guard<std::mutex> objstores_lock(objstores_lock_);
  ObjStore::Stub& stub = get_objstore_stub(request->objstore_address());
  ObjRef objref = request->objref();
  Status status = ObjStoreClient::upload_data_to(memory_[objref].ptr, objref, stub);
  return status;
}

Status ObjStoreService::DebugInfo(ServerContext* context, const DebugInfoRequest* request, DebugInfoReply* reply) {
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  for (const auto& entry : memory_) {
    reply->add_objref(entry.first);
  }
  return Status::OK;
}

Status ObjStoreService::GetObj(ServerContext* context, const GetObjRequest* request, GetObjReply* reply) {
  // TODO(pcm): There is one remaining case where this can fail, i.e. if an object is
  // to be delivered from another store but hasn't yet arrived
  ObjRef objref = request->objref();
  memory_lock_.lock();
  shared_object& object = memory_[objref];
  reply->set_bucket(object.name);
  auto handle = object.memory->get_handle_from_address(object.ptr.data);
  reply->set_handle(handle);
  reply->set_size(object.ptr.len);
  memory_lock_.unlock();
  return Status::OK;
}

Status ObjStoreService::StreamObj(ServerContext* context, ServerReader<ObjChunk>* reader, AckReply* reply) {
  ORCH_LOG(ORCH_VERBOSE, "begin to stream data to object store " << objstoreid_);
  memory_lock_.lock();
  ObjChunk chunk;
  ObjRef objref = 0;
  size_t totalsize = 0;
  if (reader->Read(&chunk)) {
    objref = chunk.objref();
    totalsize = chunk.totalsize();
    allocate_memory(objref, totalsize);
  }
  size_t num_bytes = 0;
  char* data = memory_[objref].ptr.data;

  do {
    if (num_bytes + chunk.data().size() > totalsize) {
      memory_lock_.unlock();
      return Status::CANCELLED;
    }
    std::memcpy(data, chunk.data().c_str(), chunk.data().size());
    data += chunk.data().size();
    num_bytes += chunk.data().size();
  } while (reader->Read(&chunk));

  ORCH_LOG(ORCH_VERBOSE, "finished streaming data, objref was " << objref << " and size was " << num_bytes);

  memory_lock_.unlock();

  ClientContext objready_context;
  ObjReadyRequest objready_request;
  objready_request.set_objref(objref);
  objready_request.set_objstoreid(objstoreid_);
  AckReply objready_reply;
  scheduler_stub_->ObjReady(&objready_context, objready_request, &objready_reply);

  return Status::OK;
}

void start_objstore(const char* scheduler_addr, const char* objstore_addr) {
  auto scheduler_channel = grpc::CreateChannel(scheduler_addr, grpc::InsecureChannelCredentials());
  ORCH_LOG(ORCH_INFO, "object store " << objstore_addr << " connected to scheduler " << scheduler_addr);
  std::string objstore_address(objstore_addr);
  ObjStoreService service(objstore_address, scheduler_channel);
  ServerBuilder builder;
  builder.AddListeningPort(std::string(objstore_addr), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 3) {
    return 1;
  }

  start_objstore(argv[1], argv[2]);

  return 0;
}
