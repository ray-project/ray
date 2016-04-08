#include "objstore.h"
#include <chrono>

const size_t ObjStoreClient::CHUNK_SIZE = 8 * 1024;

// this method needs to be protected by a objstore_lock_
Status ObjStoreClient::upload_data_to(slice data, ObjRef objref, ObjStore::Stub& stub) {
  ObjChunk chunk;
  ClientContext context;
  AckReply reply;
  std::unique_ptr<ClientWriter<ObjChunk> > writer(stub.StreamObj(&context, &reply));
  const uint8_t* head = data.data;
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
  : scheduler_stub_(Scheduler::NewStub(scheduler_channel)), segmentpool_(true), objstore_address_(objstore_address) {
  recv_queue_.connect(std::string("queue:") + objstore_address + std::string(":obj"), true);
  ClientContext context;
  RegisterObjStoreRequest request;
  request.set_objstore_address(objstore_address);
  RegisterObjStoreReply reply;
  scheduler_stub_->RegisterObjStore(&context, request, &reply);
  objstoreid_ = reply.objstoreid();
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

/*
Status ObjStoreService::DeliverObj(ServerContext* context, const DeliverObjRequest* request, AckReply* reply) {
  std::lock_guard<std::mutex> objstores_lock(objstores_lock_);
  ObjStore::Stub& stub = get_objstore_stub(request->objstore_address());
  ObjRef objref = request->objref();
  Status status = ObjStoreClient::upload_data_to(memory_[objref].ptr, objref, stub);
  return status;
}
*/

Status ObjStoreService::ObjStoreDebugInfo(ServerContext* context, const ObjStoreDebugInfoRequest* request, ObjStoreDebugInfoReply* reply) {
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  for (size_t i = 0; i < memory_.size(); ++i) {
    if (memory_[i].second) { // is the object available?
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

/*
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
*/

Status ObjStoreService::NotifyAlias(ServerContext* context, const NotifyAliasRequest* request, AckReply* reply) {
  // NotifyAlias assumes that the objstore already holds canonical_objref
  ObjRef alias_objref = request->alias_objref();
  ObjRef canonical_objref = request->canonical_objref();
  std::lock_guard<std::mutex> memory_lock(memory_lock_);
  if (canonical_objref >= memory_.size()) {
    ORCH_LOG(ORCH_FATAL, "Attempting to alias objref " << alias_objref << " with objref " << canonical_objref << ", but objref " << canonical_objref << " is not in the objstore.")
  }
  if (!memory_[canonical_objref].second) {
    ORCH_LOG(ORCH_FATAL, "Attempting to alias objref " << alias_objref << " with objref " << canonical_objref << ", but objref " << canonical_objref << " is not ready yet in the objstore.")
  }
  if (alias_objref >= memory_.size()) {
    memory_.resize(alias_objref + 1);
  }
  memory_[alias_objref].first = memory_[canonical_objref].first;
  memory_[alias_objref].second = true;

  ObjRequest done_request;
  done_request.type = ObjRequestType::ALIAS_DONE;
  done_request.objref = alias_objref;
  recv_queue_.send(&done_request);
  return Status::OK;
}

void ObjStoreService::process_objstore_request(const ObjRequest request) {
  switch (request.type) {
    case ObjRequestType::ALIAS_DONE: {
        process_pulls_for_objref(request.objref);
      }
      break;
    default: {
        ORCH_LOG(ORCH_FATAL, "Attempting to process request of type " <<  request.type << ". This code should be unreachable.");
      }
  }
}

void ObjStoreService::process_worker_request(const ObjRequest request) {
  if (request.workerid >= send_queues_.size()) {
    send_queues_.resize(request.workerid + 1);
  }
  if (!send_queues_[request.workerid].connected()) {
    std::string queue_name = std::string("queue:") + objstore_address_ + std::string(":worker:") + std::to_string(request.workerid) + std::string(":obj");
    send_queues_[request.workerid].connect(queue_name, false);
  }
  if (request.objref >= memory_.size()) {
    memory_.resize(request.objref + 1);
    memory_[request.objref].second = false;
  }
  switch (request.type) {
    case ObjRequestType::ALLOC: {
        ObjHandle reply = segmentpool_.allocate(request.size);
        send_queues_[request.workerid].send(&reply);
        if (request.objref >= memory_.size()) {
          memory_.resize(request.objref + 1);
        }
        memory_[request.objref].first = reply;
        memory_[request.objref].second = false;
      }
      break;
    case ObjRequestType::GET: {
        std::pair<ObjHandle, bool>& item = memory_[request.objref];
        if (item.second) {
          send_queues_[request.workerid].send(&item.first);
        } else {
          std::lock_guard<std::mutex> lock(pull_queue_lock_);
          pull_queue_.push_back(std::make_pair(request.workerid, request.objref));
        }
      }
      break;
    case ObjRequestType::WORKER_DONE: {
        std::pair<ObjHandle, bool>& item = memory_[request.objref];
        item.first.set_metadata_offset(request.metadata_offset);
        item.second = true;
        process_pulls_for_objref(request.objref);
        // Tell the scheduler that the object arrived
        // TODO(pcm): put this in a separate thread so we don't have to pay the latency here
        ClientContext objready_context;
        ObjReadyRequest objready_request;
        objready_request.set_objref(request.objref);
        objready_request.set_objstoreid(objstoreid_);
        AckReply objready_reply;
        scheduler_stub_->ObjReady(&objready_context, objready_request, &objready_reply);
      }
      break;
    default: {
        ORCH_LOG(ORCH_FATAL, "Attempting to process request of type " <<  request.type << ". This code should be unreachable.");
      }
  }
}

void ObjStoreService::process_requests() {
  // TODO(rkn): Should memory_lock_ be used in this method?
  ObjRequest request;
  while (true) {
    recv_queue_.receive(&request);
    switch (request.type) {
      case ObjRequestType::ALLOC: {
          process_worker_request(request);
        }
        break;
      case ObjRequestType::GET: {
          process_worker_request(request);
        }
        break;
      case ObjRequestType::WORKER_DONE: {
          process_worker_request(request);
        }
        break;
      case ObjRequestType::ALIAS_DONE: {
          process_objstore_request(request);
        }
        break;
      default: {
          ORCH_LOG(ORCH_FATAL, "Attempting to process request of type " <<  request.type << ". This code should be unreachable.");
        }
    }
  }
}

void ObjStoreService::process_pulls_for_objref(ObjRef objref) {
  std::pair<ObjHandle, bool>& item = memory_[objref];
  std::lock_guard<std::mutex> pull_queue_lock(pull_queue_lock_);
  for (size_t i = 0; i < pull_queue_.size(); ++i) {
    if (pull_queue_[i].second == objref) {
      ObjHandle& elem = memory_[objref].first;
      send_queues_[pull_queue_[i].first].send(&item.first);
      // Remove the pull task from the queue
      std::swap(pull_queue_[i], pull_queue_[pull_queue_.size() - 1]);
      pull_queue_.pop_back();
      i -= 1;
    }
  }
}

void ObjStoreService::start_objstore_service() {
  communicator_thread_ = std::thread([this]() {
    ORCH_LOG(ORCH_INFO, "started object store communicator server");
    process_requests();
  });
}

void start_objstore(const char* scheduler_addr, const char* objstore_addr) {
  auto scheduler_channel = grpc::CreateChannel(scheduler_addr, grpc::InsecureChannelCredentials());
  ORCH_LOG(ORCH_INFO, "object store " << objstore_addr << " connected to scheduler " << scheduler_addr);
  std::string objstore_address(objstore_addr);
  ObjStoreService service(objstore_address, scheduler_channel);
  service.start_objstore_service();
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
