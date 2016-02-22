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

void ObjStoreServiceImpl::allocate_memory(ObjRef objref, size_t size) {
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

void start_objstore(const char* objstore_address) {
  ObjStoreServiceImpl service;
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
