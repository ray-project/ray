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

class ObjStoreService final : public ObjStore::Service {
public:
  ObjStoreService(const std::string& objstore_address, std::shared_ptr<Channel> scheduler_channel);
  ~ObjStoreService();

  Status DeliverObj(ServerContext* context, const DeliverObjRequest* request, AckReply* reply) override;
  Status ObjStoreDebugInfo(ServerContext* context, const ObjStoreDebugInfoRequest* request, ObjStoreDebugInfoReply* reply) override;
  Status GetObj(ServerContext* context, const GetObjRequest* request, GetObjReply* reply) override;
  Status StreamObj(ServerContext* context, ServerReader<ObjChunk>* reader, AckReply* reply) override;
private:
  void allocate_memory(ObjRef objref, size_t size);
  // check if we already connected to the other objstore, if yes, return reference to connection, otherwise connect
  ObjStore::Stub& get_objstore_stub(const std::string& objstore_address);

  std::vector<std::string> memory_names_;
  std::unordered_map<ObjRef, shared_object> memory_;
  std::mutex memory_lock_;
  size_t page_size = mapped_region::get_page_size();
  std::unordered_map<std::string, std::unique_ptr<ObjStore::Stub>> objstores_;
  std::mutex objstores_lock_;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  ObjStoreId objstoreid_; // id of this objectstore in the scheduler object store table
};

#endif
