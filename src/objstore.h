#ifndef RAY_OBJSTORE_H
#define RAY_OBJSTORE_H

#include <unordered_map>
#include <memory>
#include <thread>
#include <iostream>
#include <grpc++/grpc++.h>

#include "ray/ray.h"
#include "ray.grpc.pb.h"
#include "types.pb.h"
#include "ipc.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerReader;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::ServerWriter;
using grpc::ClientReader;
using grpc::Status;

using grpc::Channel;

// READY:       This is used to indicate that the object has been copied from a
//              worker and is ready to be used.
// NOT_READY:   This is used to indicate that memory has been allocated for the
//              object, but the object hasn't been copied from a worker yet.
// DEALLOCATED: This is used to indicate that the object has been deallocated.
// NOT_PRESENT: This is used to indicate that space has not been allocated for
//              this object in this object store.
// PRE_ALLOCED: This is used to indicate that the memory has not yet been
//              alloced, but it will be alloced soon. This is set when we call
//              StartDelivery.
enum MemoryStatusType {READY = 0, NOT_READY = 1, DEALLOCATED = 2, NOT_PRESENT = 3, PRE_ALLOCED = 4};

class ObjStoreService final : public ObjStore::Service {
public:
  ObjStoreService(std::shared_ptr<Channel> scheduler_channel);

  Status StartDelivery(ServerContext* context, const StartDeliveryRequest* request, AckReply* reply) override;
  Status StreamObjTo(ServerContext* context, const StreamObjToRequest* request, ServerWriter<ObjChunk>* writer) override;
  Status NotifyAlias(ServerContext* context, const NotifyAliasRequest* request, AckReply* reply) override;
  Status DeallocateObject(ServerContext* context, const DeallocateObjectRequest* request, AckReply* reply) override;
  Status ObjStoreInfo(ServerContext* context, const ObjStoreInfoRequest* request, ObjStoreInfoReply* reply) override;
  void start_objstore_service();
  void register_objstore(const std::string& objstore_address, const std::string& recv_queue_name);
private:
  void get_data_from(ObjectID objectid, ObjStore::Stub& stub);
  // check if we already connected to the other objstore, if yes, return reference to connection, otherwise connect
  ObjStore::Stub& get_objstore_stub(const std::string& objstore_address);
  void process_worker_request(const ObjRequest request);
  void process_objstore_request(const ObjRequest request);
  void process_requests();
  void process_gets_for_objectid(ObjectID objectid);
  ObjHandle alloc(ObjectID objectid, size_t size);
  void object_ready(ObjectID objectid, size_t metadata_offset);

  static const size_t CHUNK_SIZE;
  std::string objstore_address_;
  ObjStoreId objstoreid_; // id of this objectstore in the scheduler object store table
  std::shared_ptr<MemorySegmentPool> segmentpool_;
  std::mutex segmentpool_lock_;
  std::vector<std::pair<ObjHandle, MemoryStatusType> > memory_; // object ID -> (memory address, memory status)
  std::mutex memory_lock_;
  std::unordered_map<std::string, std::unique_ptr<ObjStore::Stub>> objstores_;
  std::mutex objstores_lock_;
  std::unique_ptr<Scheduler::Stub> scheduler_stub_;
  std::vector<std::pair<WorkerId, ObjectID> > get_queue_;
  std::mutex get_queue_lock_;
  MessageQueue<ObjRequest> recv_queue_; // This queue is used by workers to send tasks to the object store.
  std::vector<MessageQueue<ObjHandle> > send_queues_; // This maps workerid -> queue. The object store uses these queues to send replies to the relevant workers.
  std::thread communicator_thread_;

  std::vector<std::shared_ptr<std::thread> > delivery_threads_; // TODO(rkn): document
  // TODO(rkn): possibly add lock, and properly remove these threads from the delivery_threads_ when the deliveries are done

};

#endif
