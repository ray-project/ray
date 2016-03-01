#include "objstore.h"

const size_t ObjStoreClient::CHUNK_SIZE = 8 * 1024;

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
      std::cout << "write failed" << std::endl;
      // throw std::runtime_error("write failed");
    }
  }
  writer->WritesDone();
  return writer->Finish();
}

void ObjStoreServer::allocate_memory(ObjRef objref, size_t size) {
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

ObjStore::Stub& ObjStoreServer::get_objstore_stub(const std::string& objstore_address) {
  auto iter = objstores_.find(objstore_address);
  if (iter != objstores_.end())
    return *(iter->second);
  auto channel = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
  objstores_.emplace(objstore_address, ObjStore::NewStub(channel));
  return *objstores_[objstore_address];
}

Status ObjStoreServer::DeliverObj(ServerContext* context, const DeliverObjRequest* request, AckReply* reply) {
  ObjStore::Stub& stub = get_objstore_stub(request->objstore_address());
  ObjRef objref = request->objref();
  // TODO: Have to introduce wait condition
  return ObjStoreClient::upload_data_to(memory_[objref].ptr, objref, stub);
}

Status ObjStoreServer::DebugInfo(ServerContext* context, const DebugInfoRequest* request, DebugInfoReply* reply) {
  for (const auto& entry : memory_) {
    reply->add_objref(entry.first);
  }
  return Status::OK;
}

Status ObjStoreServer::GetObj(ServerContext* context, const GetObjRequest* request, GetObjReply* reply) {
  ObjRef objref = request->objref();
  std::cout << "getobj lock";
  memory_lock_.lock();
  shared_object& object = memory_[objref];
  reply->set_bucket(object.name);
  auto handle = object.memory->get_handle_from_address(object.ptr.data);
  reply->set_handle(handle);
  reply->set_size(object.ptr.len);
  memory_lock_.unlock();
  std::cout << "getobj unlock";
  return Status::OK;
}

Status ObjStoreServer::StreamObj(ServerContext* context, ServerReader<ObjChunk>* reader, AckReply* reply) {
  std::cout << "stream obj lock" << std::endl;
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

  std::cout << "before loop " << totalsize << std::endl;

  do {
    if (num_bytes + chunk.data().size() > totalsize) {
      std::cout << "cancelled" << std::endl;
      memory_lock_.unlock();
      return Status::CANCELLED;
    }
    std::memcpy(data, chunk.data().c_str(), chunk.data().size());
    data += chunk.data().size();
    num_bytes += chunk.data().size();
    std::cout << "looping " << num_bytes << std::endl;
  } while (reader->Read(&chunk));

  std::cout << "finished" << std::endl;
  memory_lock_.unlock();
  std::cout << "stream obj unlock" << std::endl;
  return Status::OK;
}

void start_objstore(const char* objstore_address) {
  ObjStoreServer service;
  ServerBuilder builder;

  builder.AddListeningPort(std::string(objstore_address), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    return 1;
  }

  start_objstore(argv[1]);

  return 0;
}
