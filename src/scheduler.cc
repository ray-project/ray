#include <random>
#include <thread>
#include <chrono>

#include "scheduler.h"

Status SchedulerService::RemoteCall(ServerContext* context, const RemoteCallRequest* request, RemoteCallReply* reply) {
  std::unique_ptr<Call> task(new Call(request->call())); // need to copy, because request is const
  fntable_lock_.lock();

  if (fntable_.find(task->name()) == fntable_.end()) {
    // TODO(rkn): In the future, this should probably not be fatal.
    ORCH_LOG(ORCH_FATAL, "The function " << task->name() << " has not been registered by any worker.");
  }

  size_t num_return_vals = fntable_[task->name()].num_return_vals();
  fntable_lock_.unlock();

  for (size_t i = 0; i < num_return_vals; ++i) {
    ObjRef result = register_new_object();
    reply->add_result(result);
    task->add_result(result);
  }

  task_queue_lock_.lock();
  task_queue_.emplace_back(std::move(task));
  task_queue_lock_.unlock();

  schedule();
  return Status::OK;
}

Status SchedulerService::PushObj(ServerContext* context, const PushObjRequest* request, PushObjReply* reply) {
  ObjRef objref = register_new_object();
  ObjStoreId objstoreid = get_store(request->workerid());
  reply->set_objref(objref);
  schedule();
  return Status::OK;
}

Status SchedulerService::PullObj(ServerContext* context, const PullObjRequest* request, AckReply* reply) {
  objtable_lock_.lock();
  size_t size = objtable_.size();
  objtable_lock_.unlock();

  ObjRef objref = request->objref();
  if (objref >= size) {
    ORCH_LOG(ORCH_FATAL, "internal error: no object with objref " << objref << " exists");
  }

  pull_queue_lock_.lock();
  pull_queue_.push_back(std::make_pair(request->workerid(), objref));
  pull_queue_lock_.unlock();

  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) {
  std::lock_guard<std::mutex> objstore_lock(objstores_lock_);
  ObjStoreId objstoreid = objstores_.size();
  auto channel = grpc::CreateChannel(request->objstore_address(), grpc::InsecureChannelCredentials());
  objstores_.push_back(ObjStoreHandle());
  objstores_[objstoreid].address = request->objstore_address();
  objstores_[objstoreid].channel = channel;
  objstores_[objstoreid].objstore_stub = ObjStore::NewStub(channel);
  reply->set_objstoreid(objstoreid);
  return Status::OK;
}

Status SchedulerService::RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) {
  WorkerId workerid = register_worker(request->worker_address(), request->objstore_address());
  ORCH_LOG(ORCH_INFO, "registered worker with workerid " << workerid);
  reply->set_workerid(workerid);
  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) {
  ORCH_LOG(ORCH_INFO, "register function " << request->fnname() <<  " from workerid " << request->workerid());
  register_function(request->fnname(), request->workerid(), request->num_return_vals());
  schedule();
  return Status::OK;
}

Status SchedulerService::ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) {
  ORCH_LOG(ORCH_VERBOSE, "object " << request->objref() << " ready on store " << request->objstoreid());
  add_location(request->objref(), request->objstoreid());
  schedule();
  return Status::OK;
}

Status SchedulerService::WorkerReady(ServerContext* context, const WorkerReadyRequest* request, AckReply* reply) {
  ORCH_LOG(ORCH_INFO, "worker " << request->workerid() << " reported back");
  {
    std::lock_guard<std::mutex> lock(avail_workers_lock_);
    avail_workers_.push_back(request->workerid());
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::SchedulerDebugInfo(ServerContext* context, const SchedulerDebugInfoRequest* request, SchedulerDebugInfoReply* reply) {
  debug_info(*request, reply);
  return Status::OK;
}

void SchedulerService::deliver_object(ObjRef objref, ObjStoreId from, ObjStoreId to) {
  if (from == to) {
    ORCH_LOG(ORCH_FATAL, "attempting to deliver objref " << objref << " from objstore " << from << " to itself.");
  }
  ClientContext context;
  AckReply reply;
  DeliverObjRequest request;
  request.set_objref(objref);
  std::lock_guard<std::mutex> lock(objstores_lock_);
  request.set_objstore_address(objstores_[to].address);
  objstores_[from].objstore_stub->DeliverObj(&context, request, &reply);
}

void SchedulerService::schedule() {
  // TODO: don't recheck if nothing changed
  {
    std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
    std::lock_guard<std::mutex> pull_queue_lock(pull_queue_lock_);
    // Complete all pull tasks that can be completed.
    for (int i = 0; i < pull_queue_.size(); ++i) {
      const std::pair<WorkerId, ObjRef>& pull = pull_queue_[i];
      WorkerId workerid = pull.first;
      ObjRef objref = pull.second;
      if (objtable_[objref].size() > 0) {
        if (!std::binary_search(objtable_[objref].begin(), objtable_[objref].end(), get_store(workerid))) {
          // The worker's local object store does not already contain objref, so ship
          // it there from an object store that does have it.
          ObjStoreId objstoreid = pick_objstore(objref);
          deliver_object(objref, objstoreid, get_store(workerid));
        }
        // Remove the pull task from the queue
        std::swap(pull_queue_[i], pull_queue_[pull_queue_.size() - 1]);
        pull_queue_.pop_back();
        i -= 1;
      }
    }
  }
  {
    std::lock_guard<std::mutex> fntable_lock(fntable_lock_);
    std::lock_guard<std::mutex> avail_workers_lock(avail_workers_lock_);
    std::lock_guard<std::mutex> task_queue_lock(task_queue_lock_);
    for (int i = 0; i < avail_workers_.size(); ++i) {
      // Submit all tasks whose arguments are ready.
      WorkerId workerid = avail_workers_[i];
      for (auto it = task_queue_.begin(); it != task_queue_.end(); ++it) {
        // The use of erase(it) below invalidates the iterator, but we
        // immediately break out of the inner loop, so the iterator is not used
        // after the erase
        const Call& task = *(*it);
        auto& workers = fntable_[task.name()].workers();
        if (std::binary_search(workers.begin(), workers.end(), workerid) && can_run(task)) {
          submit_task(std::move(*it), workerid);
          task_queue_.erase(it);
          std::swap(avail_workers_[i], avail_workers_[avail_workers_.size() - 1]);
          avail_workers_.pop_back();
          i -= 1;
          break;
        }
      }
    }
  }
}

void SchedulerService::submit_task(std::unique_ptr<Call> call, WorkerId workerid) {
  ClientContext context;
  InvokeCallRequest request;
  InvokeCallReply reply;
  ORCH_LOG(ORCH_INFO, "starting to send arguments");
  for (size_t i = 0; i < call->arg_size(); ++i) {
    if (!call->arg(i).has_obj()) {
      ObjRef objref = call->arg(i).ref();
      ORCH_LOG(ORCH_INFO, "call contains object ref " << objref);
      std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
      auto &objstores = objtable_[call->arg(i).ref()];
      std::lock_guard<std::mutex> workers_lock(workers_lock_);
      if (!std::binary_search(objstores.begin(), objstores.end(), workers_[workerid].objstoreid)) {
        deliver_object(objref, pick_objstore(objref), workers_[workerid].objstoreid);
      }
    }
  }
  request.set_allocated_call(call.release()); // protobuf object takes ownership
  Status status = workers_[workerid].worker_stub->InvokeCall(&context, request, &reply);
}

bool SchedulerService::can_run(const Call& task) {
  std::lock_guard<std::mutex> lock(objtable_lock_);
  for (int i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjRef objref = task.arg(i).ref();
      if (objref >= objtable_.size() || objtable_[objref].size() == 0) {
        return false;
      }
    }
  }
  return true;
}

WorkerId SchedulerService::register_worker(const std::string& worker_address, const std::string& objstore_address) {
  ORCH_LOG(ORCH_INFO, "registering worker " << worker_address << " connected to object store " << objstore_address);
  ObjStoreId objstoreid = std::numeric_limits<size_t>::max();
  for (int num_attempts = 0; num_attempts < 5; ++num_attempts) {
    std::lock_guard<std::mutex> lock(objstores_lock_);
    for (size_t i = 0; i < objstores_.size(); ++i) {
      if (objstores_[i].address == objstore_address) {
        objstoreid = i;
      }
    }
    if (objstoreid == std::numeric_limits<size_t>::max()) {
      std::this_thread::sleep_for (std::chrono::milliseconds(100));
    }
  }
  if (objstoreid == std::numeric_limits<size_t>::max()) {
    ORCH_LOG(ORCH_FATAL, "object store with address " << objstore_address << " not yet registered");
  }
  workers_lock_.lock();
  WorkerId workerid = workers_.size();
  workers_.push_back(WorkerHandle());
  auto channel = grpc::CreateChannel(worker_address, grpc::InsecureChannelCredentials());
  workers_[workerid].channel = channel;
  workers_[workerid].objstoreid = objstoreid;
  workers_[workerid].worker_stub = WorkerService::NewStub(channel);
  workers_lock_.unlock();
  avail_workers_lock_.lock();
  avail_workers_.push_back(workerid);
  avail_workers_lock_.unlock();
  return workerid;
}

ObjRef SchedulerService::register_new_object() {
  std::lock_guard<std::mutex> lock(objtable_lock_);
  ObjRef result = objtable_.size();
  objtable_.push_back(std::vector<ObjStoreId>());
  return result;
}

void SchedulerService::add_location(ObjRef objref, ObjStoreId objstoreid) {
  std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
  if (objref >= objtable_.size()) {
    ORCH_LOG(ORCH_FATAL, "trying to put object on object store that was not registered with the scheduler");
  }
  // do a binary search
  auto pos = std::lower_bound(objtable_[objref].begin(), objtable_[objref].end(), objstoreid);
  if (pos == objtable_[objref].end() || objstoreid < *pos) {
    objtable_[objref].insert(pos, objstoreid);
  }
}

ObjStoreId SchedulerService::get_store(WorkerId workerid) {
  std::lock_guard<std::mutex> lock(workers_lock_);
  ObjStoreId result = workers_[workerid].objstoreid;
  return result;
}

void SchedulerService::register_function(const std::string& name, WorkerId workerid, size_t num_return_vals) {
  std::lock_guard<std::mutex> lock(fntable_lock_);
  FnInfo& info = fntable_[name];
  info.set_num_return_vals(num_return_vals);
  info.add_worker(workerid);
}

void SchedulerService::debug_info(const SchedulerDebugInfoRequest& request, SchedulerDebugInfoReply* reply) {
  fntable_lock_.lock();
  auto function_table = reply->mutable_function_table();
  for (const auto& entry : fntable_) {
    (*function_table)[entry.first].set_num_return_vals(entry.second.num_return_vals());
    for (const WorkerId& worker : entry.second.workers()) {
      (*function_table)[entry.first].add_workerid(worker);
    }
  }
  fntable_lock_.unlock();
  task_queue_lock_.lock();
  for (const auto& entry : task_queue_) {
    Call* call = reply->add_task();
    call->CopyFrom(*entry);
  }
  task_queue_lock_.unlock();
  avail_workers_lock_.lock();
  for (const WorkerId& entry : avail_workers_) {
    reply->add_avail_worker(entry);
  }
  avail_workers_lock_.unlock();
}

ObjStoreId SchedulerService::pick_objstore(ObjRef objref) {
  std::mt19937 rng;
  std::uniform_int_distribution<int> uni(0, objtable_[objref].size()-1);
  return uni(rng);
}

void start_scheduler_service(const char* server_address) {
  SchedulerService service;
  ServerBuilder builder;
  builder.AddListeningPort(std::string(server_address), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 2)
    return 1;
  start_scheduler_service(argv[1]);
  return 0;
}
