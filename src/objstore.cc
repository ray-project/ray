#include "objstore.h"

#include <chrono>
#include "utils.h"

const size_t ObjStoreService::CHUNK_SIZE = 8 * 1024;

// this method needs to be protected by a objstore_lock_
// TODO(rkn): Make sure that we do not in fact need the objstore_lock_. We want multiple deliveries to be able to happen simultaneously.
void ObjStoreService::get_data_from(ObjectID objectid, ObjStore::Stub& stub) {
  RAY_LOG(RAY_DEBUG, "Objstore " << objstoreid_ << " is beginning to get objectid " << objectid);
  ObjChunk chunk;
  ClientContext context;
  StreamObjToRequest stream_request;
  stream_request.set_objectid(objectid);
  std::unique_ptr<ClientReader<ObjChunk> > reader(stub.StreamObjTo(&context, stream_request));

  size_t total_size = 0;
  ObjHandle handle;
  if (reader->Read(&chunk)) {
    total_size = chunk.total_size();
    handle = alloc(objectid, total_size);
  }
  size_t num_bytes = 0;
  segmentpool_lock_.lock();
  uint8_t* data = segmentpool_->get_address(handle);
  segmentpool_lock_.unlock();
  do {
    RAY_CHECK_LE(num_bytes + chunk.data().size(), total_size, "The reader attempted to stream too many bytes.");
    std::memcpy(data, chunk.data().c_str(), chunk.data().size());
    data += chunk.data().size();
    num_bytes += chunk.data().size();
  } while (reader->Read(&chunk));
  RAY_CHECK_GRPC(reader->Finish());

  // finalize object
  RAY_CHECK_EQ(num_bytes, total_size, "Streamed objectid " << objectid << ", but num_bytes != total_size");
  object_ready(objectid, chunk.metadata_offset());
  RAY_LOG(RAY_DEBUG, "finished streaming data, objectid was " << objectid << " and size was " << num_bytes);
}

ObjStoreService::ObjStoreService(const std::string& scheduler_address)
    : scheduler_address_(scheduler_address) {
}

void ObjStoreService::register_objstore() {
  RAY_CHECK(!objstore_address_.empty(), "The object store address must be set before register_objstore is called.");
  // Create the scheduler stub.
  auto scheduler_channel = grpc::CreateChannel(scheduler_address_, grpc::InsecureChannelCredentials());
  scheduler_stub_ = Scheduler::NewStub(scheduler_channel);

  // Create message queue to receive requests from workers.
  std::string recv_queue_name = std::string("queue:") + objstore_address_ + std::string(":obj");
  RAY_LOG(RAY_INFO, "Object store creating queue with name " << recv_queue_name << " to receive requests from workers.");
  RAY_CHECK(recv_queue_.connect(recv_queue_name, true), "error connecting recv_queue_");
  // Register the objecet store with the scheduler.
  ClientContext context;
  RegisterObjStoreRequest request;
  request.set_objstore_address(objstore_address_);
  RegisterObjStoreReply reply;
  RAY_CHECK_GRPC(scheduler_stub_->RegisterObjStore(&context, request, &reply));
  objstoreid_ = reply.objstoreid();
  segmentpool_ = std::make_shared<MemorySegmentPool>(objstoreid_, objstore_address_, true);
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

Status ObjStoreService::StartDelivery(ServerContext* context, const StartDeliveryRequest* request, AckReply* reply) {
  // TODO(rkn): We're pushing the delivery task onto a new thread so that this method can return immediately. This matters
  // because the scheduler holds a lock while DeliverObj is being called. The correct solution is to make DeliverObj
  // an asynchronous call (and similarly with the rest of the object store service methods).
  std::string address = request->objstore_address();
  ObjectID objectid = request->objectid();
  {
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    if (objectid >= memory_.size()) {
      memory_.resize(objectid + 1, std::make_pair(ObjHandle(), MemoryStatusType::NOT_PRESENT));
    }
    if (memory_[objectid].second == MemoryStatusType::NOT_PRESENT) {
    }
    else {
      RAY_CHECK_NEQ(memory_[objectid].second, MemoryStatusType::DEALLOCATED, "Objstore " << objstoreid_ << " is attempting to get objectid " << objectid << ", but memory_[objectid] == DEALLOCATED.");
      RAY_LOG(RAY_DEBUG, "Objstore " << objstoreid_ << " already has objectid " << objectid << " or it is already being shipped, so no need to get it again.");
      return Status::OK;
    }
    memory_[objectid].second = MemoryStatusType::PRE_ALLOCED;
  }
  delivery_threads_.push_back(std::make_shared<std::thread>([this, address, objectid]() {
    std::lock_guard<std::mutex> objstores_lock(objstores_lock_);
    ObjStore::Stub& stub = get_objstore_stub(address);
    get_data_from(objectid, stub);
  }));
  return Status::OK;
}

Status ObjStoreService::ObjStoreInfo(ServerContext* context, const ObjStoreInfoRequest* request, ObjStoreInfoReply* reply) {
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  for (size_t i = 0; i < memory_.size(); ++i) {
    if (memory_[i].second == MemoryStatusType::READY) { // is the object available?
      reply->add_objectid(i);
    }
  }
  /*
  for (int i = 0; i < request->objectid_size(); ++i) {
    ObjectID objectid = request->objectid(i);
    Obj* obj = new Obj();
    std::string data(memory_[objectid].ptr.data, memory_[objectid].ptr.len); // copies, but for debugging should be ok
    obj->ParseFromString(data);
    reply->mutable_obj()->AddAllocated(obj);
  }
  */
  return Status::OK;
}

Status ObjStoreService::StreamObjTo(ServerContext* context, const StreamObjToRequest* request, ServerWriter<ObjChunk>* writer) {
  RAY_LOG(RAY_DEBUG, "begin to stream data from object store " << objstoreid_);
  ObjChunk chunk;
  ObjectID objectid = request->objectid();
  memory_lock_.lock();
  RAY_CHECK_LT(objectid, memory_.size(), "Objstore " << objstoreid_ << " is attempting to use objectid " << objectid << " in StreamObjTo, but this objectid is not present in the object store.");
  RAY_CHECK_EQ(memory_[objectid].second, MemoryStatusType::READY, "Objstore " << objstoreid_ << " is attempting to stream objectid " << objectid << ", but memory_[objectid].second != MemoryStatusType::READY.");
  ObjHandle handle = memory_[objectid].first;
  memory_lock_.unlock(); // TODO(rkn): Make sure we don't still need to hold on to this lock.
  segmentpool_lock_.lock();
  const uint8_t* head = segmentpool_->get_address(handle);
  segmentpool_lock_.unlock();
  size_t size = handle.size();
  for (size_t i = 0; i < size; i += CHUNK_SIZE) {
    chunk.set_metadata_offset(handle.metadata_offset());
    chunk.set_total_size(size);
    chunk.set_data(head + i, std::min(CHUNK_SIZE, size - i));
    RAY_CHECK(writer->Write(chunk), "stream connection prematurely closed")
  }
  return Status::OK;
}

Status ObjStoreService::NotifyAlias(ServerContext* context, const NotifyAliasRequest* request, AckReply* reply) {
  // NotifyAlias assumes that the objstore already holds canonical_objectid
  ObjectID alias_objectid = request->alias_objectid();
  ObjectID canonical_objectid = request->canonical_objectid();
  RAY_LOG(RAY_DEBUG, "Aliasing objectid " << alias_objectid << " with objectid " << canonical_objectid);
  {
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    RAY_CHECK_LT(canonical_objectid, memory_.size(), "Attempting to alias objectid " << alias_objectid << " with objectid " << canonical_objectid << ", but objectid " << canonical_objectid << " is not in the objstore.")
    RAY_CHECK_NEQ(memory_[canonical_objectid].second, MemoryStatusType::NOT_READY, "Attempting to alias objectid " << alias_objectid << " with objectid " << canonical_objectid << ", but objectid " << canonical_objectid << " is not ready yet in the objstore.")
    RAY_CHECK_NEQ(memory_[canonical_objectid].second, MemoryStatusType::NOT_PRESENT, "Attempting to alias objectid " << alias_objectid << " with objectid " << canonical_objectid << ", but objectid " << canonical_objectid << " is not present in the objstore.")
    RAY_CHECK_NEQ(memory_[canonical_objectid].second, MemoryStatusType::DEALLOCATED, "Attempting to alias objectid " << alias_objectid << " with objectid " << canonical_objectid << ", but objectid " << canonical_objectid << " has already been deallocated.")
    if (alias_objectid >= memory_.size()) {
      memory_.resize(alias_objectid + 1, std::make_pair(ObjHandle(), MemoryStatusType::NOT_PRESENT));
    }
    memory_[alias_objectid].first = memory_[canonical_objectid].first;
    memory_[alias_objectid].second = MemoryStatusType::READY;
  }
  ObjRequest done_request;
  done_request.type = ObjRequestType::ALIAS_DONE;
  done_request.objectid = alias_objectid;
  RAY_CHECK(recv_queue_.send(&done_request), "error sending over IPC");
  return Status::OK;
}

Status ObjStoreService::DeallocateObject(ServerContext* context, const DeallocateObjectRequest* request, AckReply* reply) {
  ObjectID canonical_objectid = request->canonical_objectid();
  RAY_LOG(RAY_INFO, "Deallocating canonical_objectid " << canonical_objectid);
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  RAY_CHECK_EQ(memory_[canonical_objectid].second, MemoryStatusType::READY, "Attempting to deallocate canonical_objectid " << canonical_objectid << ", but memory_[canonical_objectid].second = " << memory_[canonical_objectid].second);
  RAY_CHECK_LT(canonical_objectid, memory_.size(), "Attempting to deallocate canonical_objectid " << canonical_objectid << ", but it is not in the objstore.");
  segmentpool_lock_.lock();
  segmentpool_->deallocate(memory_[canonical_objectid].first);
  segmentpool_lock_.unlock();
  memory_[canonical_objectid].second = MemoryStatusType::DEALLOCATED;
  return Status::OK;
}

// This table describes how the memory status changes in response to requests.
//
// MemoryStatus | ObjRequest  | New MemoryStatus | action performed
// -------------+-------------+------------------+----------------------------
// NOT_PRESENT  | ALLOC       | NOT_READY        | allocate object
// NOT_READY    | WORKER_DONE | READY            | send ObjReady to scheduler
// NOT_READY    | GET         | NOT_READY        | add to get queue
// READY        | GET         | READY            | return handle
// READY        | DEALLOC     | DEALLOCATED      | deallocate
// -------------+-------------+------------------+----------------------------
void ObjStoreService::process_objstore_request(const ObjRequest request) {
  switch (request.type) {
    case ObjRequestType::ALIAS_DONE: {
        process_gets_for_objectid(request.objectid);
      }
      break;
    default: {
        RAY_CHECK(false, "Attempting to process request of type " <<  request.type << ". This code should be unreachable.");
      }
  }
}

void ObjStoreService::process_worker_request(const ObjRequest request) {
  if (request.workerid >= send_queues_.size()) {
    send_queues_.resize(request.workerid + 1);
  }
  if (!send_queues_[request.workerid].connected()) {
    std::string queue_name = std::string("queue:") + objstore_address_ + std::string(":worker:") + std::to_string(request.workerid) + std::string(":obj");
    RAY_CHECK(send_queues_[request.workerid].connect(queue_name, false), "error connecting receive_queue_");
  }
  {
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    if (request.objectid >= memory_.size()) {
      memory_.resize(request.objectid + 1, std::make_pair(ObjHandle(), MemoryStatusType::NOT_PRESENT));
    }
  }
  switch (request.type) {
    case ObjRequestType::ALLOC: {
        ObjHandle handle = alloc(request.objectid, request.size); // This method acquires memory_lock_
        RAY_CHECK(send_queues_[request.workerid].send(&handle), "error sending over IPC");
      }
      break;
    case ObjRequestType::GET: {
        std::lock_guard<std::mutex> memory_lock(memory_lock_);
        std::pair<ObjHandle, MemoryStatusType>& item = memory_[request.objectid];
        if (item.second == MemoryStatusType::READY) {
          RAY_LOG(RAY_DEBUG, "Responding to GET request: returning objectid " << request.objectid);
          RAY_CHECK(send_queues_[request.workerid].send(&item.first), "error sending over IPC");
        } else if (item.second == MemoryStatusType::NOT_READY || item.second == MemoryStatusType::NOT_PRESENT || item.second == MemoryStatusType::PRE_ALLOCED) {
          std::lock_guard<std::mutex> lock(get_queue_lock_);
          get_queue_.push_back(std::make_pair(request.workerid, request.objectid));
        } else {
          RAY_CHECK(false, "A worker requested objectid " << request.objectid << ", but memory_[objectid].second = " << memory_[request.objectid].second);
        }
      }
      break;
    case ObjRequestType::WORKER_DONE: {
        object_ready(request.objectid, request.metadata_offset); // This method acquires memory_lock_
      }
      break;
    default: {
        RAY_CHECK(false, "Attempting to process request of type " <<  request.type << ". This code should be unreachable.");
      }
  }
}

void ObjStoreService::process_requests() {
  // TODO(rkn): Should memory_lock_ be used in this method?
  ObjRequest request;
  while (true) {
    RAY_CHECK(recv_queue_.receive(&request), "error receiving over IPC");
    switch (request.type) {
      case ObjRequestType::ALLOC: {
          RAY_LOG(RAY_VERBOSE, "Request (worker " << request.workerid << " to objstore " << objstoreid_ << "): Allocate object with objectid " << request.objectid << " and size " << request.size);
          process_worker_request(request);
        }
        break;
      case ObjRequestType::GET: {
          RAY_LOG(RAY_VERBOSE, "Request (worker " << request.workerid << " to objstore " << objstoreid_ << "): Get object with objectid " << request.objectid);
          process_worker_request(request);
        }
        break;
      case ObjRequestType::WORKER_DONE: {
          RAY_LOG(RAY_VERBOSE, "Request (worker " << request.workerid << " to objstore " << objstoreid_ << "): Finalize object with objectid " << request.objectid);
          process_worker_request(request);
        }
        break;
      case ObjRequestType::ALIAS_DONE: {
          process_objstore_request(request);
        }
        break;
      default: {
          RAY_CHECK(false, "Attempting to process request of type " <<  request.type << ". This code should be unreachable.");
        }
    }
  }
}

void ObjStoreService::process_gets_for_objectid(ObjectID objectid) {
  std::pair<ObjHandle, MemoryStatusType>& item = memory_[objectid];
  std::lock_guard<std::mutex> get_queue_lock(get_queue_lock_);
  for (size_t i = 0; i < get_queue_.size(); ++i) {
    if (get_queue_[i].second == objectid) {
      ObjHandle& elem = memory_[objectid].first;
      RAY_CHECK(send_queues_[get_queue_[i].first].send(&item.first), "error sending over IPC");
      // Remove the get task from the queue
      std::swap(get_queue_[i], get_queue_[get_queue_.size() - 1]);
      get_queue_.pop_back();
      i -= 1;
    }
  }
}

ObjHandle ObjStoreService::alloc(ObjectID objectid, size_t size) {
  segmentpool_lock_.lock();
  ObjHandle handle = segmentpool_->allocate(size);
  segmentpool_lock_.unlock();
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  RAY_LOG(RAY_VERBOSE, "Allocating space for objectid " << objectid << " on object store " << objstoreid_);
  RAY_CHECK(memory_[objectid].second == MemoryStatusType::NOT_PRESENT || memory_[objectid].second == MemoryStatusType::PRE_ALLOCED, "Attempting to allocate space for objectid " << objectid << ", but memory_[objectid].second = " << memory_[objectid].second);
  memory_[objectid].first = handle;
  memory_[objectid].second = MemoryStatusType::NOT_READY;
  return handle;
}

void ObjStoreService::object_ready(ObjectID objectid, size_t metadata_offset) {
  {
    RAY_LOG(RAY_INFO, "Object with ObjectID " << objectid << " is ready.");
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    std::pair<ObjHandle, MemoryStatusType>& item = memory_[objectid];
    RAY_CHECK_EQ(item.second, MemoryStatusType::NOT_READY, "A worker notified the object store that objectid " << objectid << " has been written to the object store, but memory_[objectid].second != NOT_READY.");
    item.first.set_metadata_offset(metadata_offset);
    item.second = MemoryStatusType::READY;
  }
  process_gets_for_objectid(objectid);
  // Tell the scheduler that the object arrived
  // TODO(pcm): put this in a separate thread so we don't have to pay the latency here
  ClientContext objready_context;
  ObjReadyRequest objready_request;
  objready_request.set_objectid(objectid);
  objready_request.set_objstoreid(objstoreid_);
  AckReply objready_reply;
  RAY_CHECK_GRPC(scheduler_stub_->ObjReady(&objready_context, objready_request, &objready_reply));
}

void ObjStoreService::start_objstore_service() {
  communicator_thread_ = std::thread([this]() {
    RAY_LOG(RAY_INFO, "started object store communicator server");
    process_requests();
  });
}

void set_logfile(const char* log_file_prefix, const std::string& node_ip_address, int port) {
  if (log_file_prefix) {
    std::string log_file_name = std::string(log_file_prefix) + "objstore-" + node_ip_address + "-" + std::to_string(port) + ".log";
    create_log_dir_or_die(log_file_name.c_str());
    global_ray_config.log_to_file = true;
    global_ray_config.logfile.open(log_file_name);
  } else {
    std::cout << "object store: writing logs to stdout; you can change this by passing --log-file-prefix <fileprefix> to ./objstore" << std::endl;
    global_ray_config.log_to_file = false;
  }
}

void start_objstore(const std::string& scheduler_address, const std::string& node_ip_address, const char* log_file_prefix) {
  // Initialize the object store.
  ObjStoreService service(scheduler_address);
  int port;
  ServerBuilder builder;
  // Get GRPC to assign an unused port.
  builder.AddListeningPort(std::string("0.0.0.0:0"), grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  if (server == nullptr) {
    RAY_CHECK(false, "Failed to create the object store server.")
  }
  // Set the object store address.
  service.set_objstore_address(node_ip_address + ":" + std::to_string(port));
  // Set the logfile.
  set_logfile(log_file_prefix, node_ip_address, port);
  // Register the object store with the scheduler.
  service.register_objstore();
  // Launch a thread to process incoming messages in the message queue from
  // the workers.
  service.start_objstore_service();
  // Process incoming GRPC calls. These may come from the schedeler or from
  // other object stores. This method does not return.
  server->Wait();
}

RayConfig global_ray_config;

int main(int argc, char** argv) {
  RAY_CHECK_GE(argc, 3, "object store: expected at least two arguments (scheduler ip address and object store ip address)");

  const char* log_file_prefix = nullptr;
  if (argc > 3) {
    log_file_prefix = get_cmd_option(argv, argv + argc, "--log-file-prefix");
  }

  start_objstore(argv[1], argv[2], log_file_prefix);

  return 0;
}
