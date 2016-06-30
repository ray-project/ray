#include "objstore.h"

#include <chrono>
#include "utils.h"

const size_t ObjStoreService::CHUNK_SIZE = 8 * 1024;

// this method needs to be protected by a objstore_lock_
// TODO(rkn): Make sure that we do not in fact need the objstore_lock_. We want multiple deliveries to be able to happen simultaneously.
void ObjStoreService::get_data_from(ObjRef objref, ObjStore::Stub& stub) {
  RAY_LOG(RAY_DEBUG, "Objstore " << objstoreid_ << " is beginning to get objref " << objref);
  ObjChunk chunk;
  ClientContext context;
  StreamObjToRequest stream_request;
  stream_request.set_objref(objref);
  std::unique_ptr<ClientReader<ObjChunk> > reader(stub.StreamObjTo(&context, stream_request));

  size_t total_size = 0;
  ObjHandle handle;
  if (reader->Read(&chunk)) {
    total_size = chunk.total_size();
    handle = alloc(objref, total_size);
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
  Status status = reader->Finish(); // Right now we don't use the status.

  // finalize object
  RAY_CHECK_EQ(num_bytes, total_size, "Streamed objref " << objref << ", but num_bytes != total_size");
  object_ready(objref, chunk.metadata_offset());
  RAY_LOG(RAY_DEBUG, "finished streaming data, objref was " << objref << " and size was " << num_bytes);
}

ObjStoreService::ObjStoreService(const std::string& objstore_address, std::shared_ptr<Channel> scheduler_channel)
  : scheduler_stub_(Scheduler::NewStub(scheduler_channel)), objstore_address_(objstore_address) {
  RAY_CHECK(recv_queue_.connect(std::string("queue:") + objstore_address + std::string(":obj"), true), "error connecting recv_queue_");
  ClientContext context;
  RegisterObjStoreRequest request;
  request.set_objstore_address(objstore_address);
  RegisterObjStoreReply reply;
  scheduler_stub_->RegisterObjStore(&context, request, &reply);
  objstoreid_ = reply.objstoreid();
  segmentpool_ = std::make_shared<MemorySegmentPool>(objstoreid_, true);
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
  ObjRef objref = request->objref();
  {
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    if (objref >= memory_.size()) {
      memory_.resize(objref + 1, std::make_pair(ObjHandle(), MemoryStatusType::NOT_PRESENT));
    }
    if (memory_[objref].second == MemoryStatusType::NOT_PRESENT) {
    }
    else {
      RAY_CHECK_NEQ(memory_[objref].second, MemoryStatusType::DEALLOCATED, "Objstore " << objstoreid_ << " is attempting to get objref " << objref << ", but memory_[objref] == DEALLOCATED.");
      RAY_LOG(RAY_DEBUG, "Objstore " << objstoreid_ << " already has objref " << objref << " or it is already being shipped, so no need to get it again.");
      return Status::OK;
    }
    memory_[objref].second = MemoryStatusType::PRE_ALLOCED;
  }
  delivery_threads_.push_back(std::make_shared<std::thread>([this, address, objref]() {
    std::lock_guard<std::mutex> objstores_lock(objstores_lock_);
    ObjStore::Stub& stub = get_objstore_stub(address);
    get_data_from(objref, stub);
  }));
  return Status::OK;
}

Status ObjStoreService::ObjStoreInfo(ServerContext* context, const ObjStoreInfoRequest* request, ObjStoreInfoReply* reply) {
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  for (size_t i = 0; i < memory_.size(); ++i) {
    if (memory_[i].second == MemoryStatusType::READY) { // is the object available?
      reply->add_objref(i);
    }
  }
  /*
  for (int i = 0; i < request->objref_size(); ++i) {
    ObjRef objref = request->objref(i);
    Obj* obj = new Obj();
    std::string data(memory_[objref].ptr.data, memory_[objref].ptr.len); // copies, but for debugging should be ok
    obj->ParseFromString(data);
    reply->mutable_obj()->AddAllocated(obj);
  }
  */
  return Status::OK;
}

Status ObjStoreService::StreamObjTo(ServerContext* context, const StreamObjToRequest* request, ServerWriter<ObjChunk>* writer) {
  RAY_LOG(RAY_DEBUG, "begin to stream data from object store " << objstoreid_);
  ObjChunk chunk;
  ObjRef objref = request->objref();
  memory_lock_.lock();
  RAY_CHECK_LT(objref, memory_.size(), "Objstore " << objstoreid_ << " is attempting to use objref " << objref << " in StreamObjTo, but this objref is not present in the object store.");
  RAY_CHECK_EQ(memory_[objref].second, MemoryStatusType::READY, "Objstore " << objstoreid_ << " is attempting to stream objref " << objref << ", but memory_[objref].second != MemoryStatusType::READY.");
  ObjHandle handle = memory_[objref].first;
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
  // NotifyAlias assumes that the objstore already holds canonical_objref
  ObjRef alias_objref = request->alias_objref();
  ObjRef canonical_objref = request->canonical_objref();
  RAY_LOG(RAY_DEBUG, "Aliasing objref " << alias_objref << " with objref " << canonical_objref);
  {
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    RAY_CHECK_LT(canonical_objref, memory_.size(), "Attempting to alias objref " << alias_objref << " with objref " << canonical_objref << ", but objref " << canonical_objref << " is not in the objstore.")
    RAY_CHECK_NEQ(memory_[canonical_objref].second, MemoryStatusType::NOT_READY, "Attempting to alias objref " << alias_objref << " with objref " << canonical_objref << ", but objref " << canonical_objref << " is not ready yet in the objstore.")
    RAY_CHECK_NEQ(memory_[canonical_objref].second, MemoryStatusType::NOT_PRESENT, "Attempting to alias objref " << alias_objref << " with objref " << canonical_objref << ", but objref " << canonical_objref << " is not present in the objstore.")
    RAY_CHECK_NEQ(memory_[canonical_objref].second, MemoryStatusType::DEALLOCATED, "Attempting to alias objref " << alias_objref << " with objref " << canonical_objref << ", but objref " << canonical_objref << " has already been deallocated.")
    if (alias_objref >= memory_.size()) {
      memory_.resize(alias_objref + 1, std::make_pair(ObjHandle(), MemoryStatusType::NOT_PRESENT));
    }
    memory_[alias_objref].first = memory_[canonical_objref].first;
    memory_[alias_objref].second = MemoryStatusType::READY;
  }
  ObjRequest done_request;
  done_request.type = ObjRequestType::ALIAS_DONE;
  done_request.objref = alias_objref;
  RAY_CHECK(recv_queue_.send(&done_request), "error sending over IPC");
  return Status::OK;
}

Status ObjStoreService::DeallocateObject(ServerContext* context, const DeallocateObjectRequest* request, AckReply* reply) {
  ObjRef canonical_objref = request->canonical_objref();
  RAY_LOG(RAY_INFO, "Deallocating canonical_objref " << canonical_objref);
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  RAY_CHECK_EQ(memory_[canonical_objref].second, MemoryStatusType::READY, "Attempting to deallocate canonical_objref " << canonical_objref << ", but memory_[canonical_objref].second = " << memory_[canonical_objref].second);
  RAY_CHECK_LT(canonical_objref, memory_.size(), "Attempting to deallocate canonical_objref " << canonical_objref << ", but it is not in the objstore.");
  segmentpool_lock_.lock();
  segmentpool_->deallocate(memory_[canonical_objref].first);
  segmentpool_lock_.unlock();
  memory_[canonical_objref].second = MemoryStatusType::DEALLOCATED;
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
        process_gets_for_objref(request.objref);
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
    if (request.objref >= memory_.size()) {
      memory_.resize(request.objref + 1, std::make_pair(ObjHandle(), MemoryStatusType::NOT_PRESENT));
    }
  }
  switch (request.type) {
    case ObjRequestType::ALLOC: {
        ObjHandle handle = alloc(request.objref, request.size); // This method acquires memory_lock_
        RAY_CHECK(send_queues_[request.workerid].send(&handle), "error sending over IPC");
      }
      break;
    case ObjRequestType::GET: {
        std::lock_guard<std::mutex> memory_lock(memory_lock_);
        std::pair<ObjHandle, MemoryStatusType>& item = memory_[request.objref];
        if (item.second == MemoryStatusType::READY) {
          RAY_LOG(RAY_DEBUG, "Responding to GET request: returning objref " << request.objref);
          RAY_CHECK(send_queues_[request.workerid].send(&item.first), "error sending over IPC");
        } else if (item.second == MemoryStatusType::NOT_READY || item.second == MemoryStatusType::NOT_PRESENT || item.second == MemoryStatusType::PRE_ALLOCED) {
          std::lock_guard<std::mutex> lock(get_queue_lock_);
          get_queue_.push_back(std::make_pair(request.workerid, request.objref));
        } else {
          RAY_CHECK(false, "A worker requested objref " << request.objref << ", but memory_[objref].second = " << memory_[request.objref].second);
        }
      }
      break;
    case ObjRequestType::WORKER_DONE: {
        object_ready(request.objref, request.metadata_offset); // This method acquires memory_lock_
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
          RAY_LOG(RAY_VERBOSE, "Request (worker " << request.workerid << " to objstore " << objstoreid_ << "): Allocate object with objref " << request.objref << " and size " << request.size);
          process_worker_request(request);
        }
        break;
      case ObjRequestType::GET: {
          RAY_LOG(RAY_VERBOSE, "Request (worker " << request.workerid << " to objstore " << objstoreid_ << "): Get object with objref " << request.objref);
          process_worker_request(request);
        }
        break;
      case ObjRequestType::WORKER_DONE: {
          RAY_LOG(RAY_VERBOSE, "Request (worker " << request.workerid << " to objstore " << objstoreid_ << "): Finalize object with objref " << request.objref);
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

void ObjStoreService::process_gets_for_objref(ObjRef objref) {
  std::pair<ObjHandle, MemoryStatusType>& item = memory_[objref];
  std::lock_guard<std::mutex> get_queue_lock(get_queue_lock_);
  for (size_t i = 0; i < get_queue_.size(); ++i) {
    if (get_queue_[i].second == objref) {
      ObjHandle& elem = memory_[objref].first;
      RAY_CHECK(send_queues_[get_queue_[i].first].send(&item.first), "error sending over IPC");
      // Remove the get task from the queue
      std::swap(get_queue_[i], get_queue_[get_queue_.size() - 1]);
      get_queue_.pop_back();
      i -= 1;
    }
  }
}

ObjHandle ObjStoreService::alloc(ObjRef objref, size_t size) {
  segmentpool_lock_.lock();
  ObjHandle handle = segmentpool_->allocate(size);
  segmentpool_lock_.unlock();
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  RAY_LOG(RAY_VERBOSE, "Allocating space for objref " << objref << " on object store " << objstoreid_);
  RAY_CHECK(memory_[objref].second == MemoryStatusType::NOT_PRESENT || memory_[objref].second == MemoryStatusType::PRE_ALLOCED, "Attempting to allocate space for objref " << objref << ", but memory_[objref].second = " << memory_[objref].second);
  memory_[objref].first = handle;
  memory_[objref].second = MemoryStatusType::NOT_READY;
  return handle;
}

void ObjStoreService::object_ready(ObjRef objref, size_t metadata_offset) {
  {
    RAY_LOG(RAY_INFO, "Objref " << objref << " is ready.");
    std::lock_guard<std::mutex> memory_lock(memory_lock_);
    std::pair<ObjHandle, MemoryStatusType>& item = memory_[objref];
    RAY_CHECK_EQ(item.second, MemoryStatusType::NOT_READY, "A worker notified the object store that objref " << objref << " has been written to the object store, but memory_[objref].second != NOT_READY.");
    item.first.set_metadata_offset(metadata_offset);
    item.second = MemoryStatusType::READY;
  }
  process_gets_for_objref(objref);
  // Tell the scheduler that the object arrived
  // TODO(pcm): put this in a separate thread so we don't have to pay the latency here
  ClientContext objready_context;
  ObjReadyRequest objready_request;
  objready_request.set_objref(objref);
  objready_request.set_objstoreid(objstoreid_);
  AckReply objready_reply;
  scheduler_stub_->ObjReady(&objready_context, objready_request, &objready_reply);
}

void ObjStoreService::start_objstore_service() {
  communicator_thread_ = std::thread([this]() {
    RAY_LOG(RAY_INFO, "started object store communicator server");
    process_requests();
  });
}

void start_objstore(const char* scheduler_addr, const char* objstore_addr) {
  auto scheduler_channel = grpc::CreateChannel(scheduler_addr, grpc::InsecureChannelCredentials());
  RAY_LOG(RAY_INFO, "object store " << objstore_addr << " connected to scheduler " << scheduler_addr);
  std::string objstore_address(objstore_addr);
  ObjStoreService service(objstore_address, scheduler_channel);
  service.start_objstore_service();
  std::string::iterator split_point = split_ip_address(objstore_address);
  std::string port;
  port.assign(split_point, objstore_address.end());
  ServerBuilder builder;
  builder.AddListeningPort(std::string("0.0.0.0:") + port, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}

RayConfig global_ray_config;

int main(int argc, char** argv) {
  RAY_CHECK_GE(argc, 3, "object store: expected at least two arguments (scheduler ip address and object store ip address)");

  if (argc > 3) {
    const char* log_file_name = get_cmd_option(argv, argv + argc, "--log-file-name");
    if (log_file_name) {
      std::cout << "object store: writing to log file " << log_file_name << std::endl;
      create_log_dir_or_die(log_file_name);
      global_ray_config.log_to_file = true;
      global_ray_config.logfile.open(log_file_name);
    } else {
      std::cout << "object store: writing logs to stdout; you can change this by passing --log-file-name <filename> to ./scheduler" << std::endl;
      global_ray_config.log_to_file = false;
    }
  }

  start_objstore(argv[1], argv[2]);

  return 0;
}
