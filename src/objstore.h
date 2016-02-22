#ifndef ORCHESTRA_OBJSTORE_SERVER_H
#define ORCHESTRA_OBJSTORE_SERVER_H

#include <unordered_map>
#include <memory>
#include <iostream>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <grpc++/grpc++.h>

using namespace boost::interprocess;

#include "orchestra/orchestra.h"
#include "orchestra.grpc.pb.h"
#include "types.pb.h"

#include "orchlib.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerReader;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::ClientWriter;
using grpc::Status;

using grpc::Channel;

class ObjStoreClient {
public:
  static const size_t CHUNK_SIZE;
  static Status upload_data_to(slice data, ObjRef objref, ObjStore::Stub& stub);
};

struct shared_object {
  std::string name;
  std::shared_ptr<managed_shared_memory> memory;
  slice ptr;
};

class ObjStoreServiceImpl final : public ObjStore::Service {
  std::vector<std::string> memory_names_;
  std::unordered_map<ObjRef, shared_object> memory_;
  std::mutex memory_lock_;
  size_t page_size = mapped_region::get_page_size();
  std::unordered_map<std::string, std::unique_ptr<ObjStore::Stub>> objstores_;

  void allocate_memory(ObjRef objref, size_t size);

  // check if we already connected to the other objstore, if yes, return reference to connection, otherwise connect
  ObjStore::Stub& get_objstore_stub(const std::string& objstore_address) {
    auto iter = objstores_.find(objstore_address);
    if (iter != objstores_.end())
      return *(iter->second);
    auto channel = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
    objstores_.emplace(objstore_address, ObjStore::NewStub(channel));
    return *objstores_[objstore_address];
  }

public:
  ObjStoreServiceImpl() {}

  ~ObjStoreServiceImpl() {
    for (const auto& segment_name : memory_names_) {
      shared_memory_object::remove(segment_name.c_str());
    }
  }

  Status DeliverObj(ServerContext* context, const DeliverObjRequest* request, AckReply* reply) override {
    ObjStore::Stub& stub = get_objstore_stub(request->objstore_address());
    ObjRef objref = request->objref();

    // TODO: Have to introduce wait condition

    return ObjStoreClient::upload_data_to(memory_[objref].ptr, objref, stub);
  }

  Status DebugInfo(ServerContext* context, const DebugInfoRequest* request, DebugInfoReply* reply) override {
    for (const auto& entry : memory_) {
      reply->add_objref(entry.first);
    }
    return Status::OK;
  }

  Status GetObj(ServerContext* context, const GetObjRequest* request, GetObjReply* reply) override {
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

  Status StreamObj(ServerContext* context, ServerReader<ObjChunk>* reader, AckReply* reply) override {
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
};

#endif
