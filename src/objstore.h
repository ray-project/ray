#ifndef ORCHESTRA_OBJSTORE_H
#define ORCHESTRA_OBJSTORE_H

#include <unordered_map>
#include <memory>
#include <thread>
#include <iostream>
#include <grpc++/grpc++.h>

#include "orchestra/orchestra.h"
#include "orchestra.grpc.pb.h"
#include "types.pb.h"
#include "ipc.h"

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

enum MemoryStatusType {READY = 0, NOT_READY = 1, DEALLOCATED = 2, NOT_PRESENT = 3};

class ObjStoreService final : public ObjStore::Service {
public:
  ObjStoreService(const std::string& objstore_address, std::shared_ptr<Channel> scheduler_channel);

  // Status DeliverObj(ServerContext* context, const DeliverObjRequest* request, AckReply* reply) override;
  // Status StreamObj(ServerContext* context, ServerReader<ObjChunk>* reader, AckReply* reply) override;
  Status NotifyAlias(ServerContext* context, const NotifyAliasRequest* request, AckReply* reply) override;
  Status DeallocateObject(ServerContext* context, const DeallocateObjectRequest* request, AckReply* reply) override;
  Status ObjStoreInfo(ServerContext* context, const ObjStoreInfoRequest* request, ObjStoreInfoReply* reply) override;
  void start_objstore_service();
private:
  // check if we already connected to the other objstore, if yes, return reference to connection, otherwise connect
  ObjStore::Stub& get_objstore_stub(const std::string& objstore_address);
  void process_worker_request(const ObjRequest request);
  void process_objstore_request(const ObjRequest request);
  void process_requests();
  void process_pulls_for_objref(ObjRef objref);

  std::string objstore_address_;
  ObjStoreId objstoreid_; // id of this objectstore in the scheduler object store table
  MemorySegmentPool segmentpool_;
  std::vector<std::pair<ObjHandle, MemoryStatusType> > memory_; // object reference -> (memory address, memory status)
  std::mutex memory_lock_;
  std::unordered_map<std::string, std::unique_ptr<ObjStore::Stub>> objstores_;
  std::mutex objstores_lock_;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  std::vector<std::pair<WorkerId, ObjRef> > pull_queue_;
  std::mutex pull_queue_lock_;
  MessageQueue<ObjRequest> recv_queue_;
  std::vector<MessageQueue<ObjHandle> > send_queues_; // workerid -> queue
  std::thread communicator_thread_;
};

#endif
