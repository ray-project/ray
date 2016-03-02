#include "scheduler.h"

Status SchedulerService::RemoteCall(ServerContext* context, const RemoteCallRequest* request, RemoteCallReply* reply) {
  std::unique_ptr<Call> task(new Call(request->call())); // need to copy, because request is const
  fntable_lock_.lock();
  size_t num_return_vals = fntable_[task->name()].num_return_vals();
  fntable_lock_.unlock();

  for (size_t i = 0; i < num_return_vals; ++i) {
    ObjRef result = register_new_object();
    reply->add_result(result);
    task->add_result(result);
  }

  tasks_lock_.lock();
  tasks_.emplace_back(std::move(task));
  tasks_lock_.unlock();
  return Status::OK;
}

Status SchedulerService::PushObj(ServerContext* context, const PushObjRequest* request, PushObjReply* reply) {
  ObjRef objref = register_new_object();
  ObjStoreId objstoreid = get_store(request->workerid());
  add_location(objref, objstoreid);
  reply->set_objref(objref);
  return Status::OK;
}

Status SchedulerService::PullObj(ServerContext* context, const PullObjRequest* request, AckReply* reply) {
  return Status::OK;
}

Status SchedulerService::RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) {
  WorkerId workerid = register_worker(request->worker_address(), request->objstore_address());
  std::cout << "registered worker with workerid" << workerid << std::endl;
  reply->set_workerid(workerid);
  return Status::OK;
}

Status SchedulerService::RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) {
  std::cout << "RegisterFunction: workerid is" << request->workerid() << std::endl;
  register_function(request->fnname(), request->workerid(), request->num_return_vals());
  return Status::OK;
}

Status SchedulerService::GetDebugInfo(ServerContext* context, const GetDebugInfoRequest* request, GetDebugInfoReply* reply) {
  debug_info(*request, reply);
  return Status::OK;
}

void SchedulerService::schedule() {
  // TODO: work out a better strategy here
  WorkerId workerid = 0;
  {
    std::lock_guard<std::mutex> lock(avail_workers_lock_);
    if (avail_workers_.size() == 0)
      return;
    workerid = avail_workers_.back();
    std::cout << "got available worker" << workerid << std::endl;
    avail_workers_.pop_back();
  }
  // TODO: think about locking here
  for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
    const Call& task = *(*it);
    auto& workers = fntable_[task.name()].workers();
    if (std::binary_search(workers.begin(), workers.end(), workerid) && can_run(task)) {
      submit_task(std::move(*it), workerid);
      tasks_.erase(it);
      return;
    }
  }
}

void SchedulerService::submit_task(std::unique_ptr<Call> call, WorkerId workerid) {
  ClientContext context;
  InvokeCallRequest request;
  InvokeCallReply reply;
  std::cout << "sending arguments now" << std::endl;
  for (size_t i = 0; i < call->arg_size(); ++i) {
    if (!call->arg(i).has_obj()) {
      std::cout << "need to send object ref" << call->arg(i).ref() << std::endl;
      std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
      auto &objstores = objtable_[call->arg(i).ref()];
      std::lock_guard<std::mutex> workers_lock(workers_lock_);
      if (!std::binary_search(objstores.begin(), objstores.end(), workers_[workerid].objstoreid)) {
        std::cout << "have to send" << std::endl;
        std::exit(1);
      }
      // if (objstoreid != workers_[workerid].objstoreid) {
      //  std::lock_guard<std::mutex> objstores_lock(objstores_lock_);
      //  objstores_.
      // }
    }
  }
  request.set_allocated_call(call.release()); // protobuf object takes ownership
  Status status = workers_[workerid].worker_stub->InvokeCall(&context, request, &reply);
}

bool SchedulerService::can_run(const Call& task) {
  std::lock_guard<std::mutex> lock(objtable_lock_);
  for (int i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      if (objtable_[task.arg(i).ref()].size() == 0) {
        return false;
      }
    }
  }
  return true;
}

WorkerId SchedulerService::register_worker(const std::string& worker_address, const std::string& objstore_address) {
  ObjStoreId objstoreid = std::numeric_limits<size_t>::max();
  objstores_lock_.lock();
  for (size_t i = 0; i < objstores_.size(); ++i) {
    std::cout << "adress: " << objstores_[i].address << std::endl;
    std::cout << "my adress: " << objstore_address << std::endl;
    if (objstores_[i].address == objstore_address) {
      objstoreid = i;
    }
  }
  if (objstoreid == std::numeric_limits<size_t>::max()) {
    // register objstore
    objstoreid = objstores_.size();
    auto channel = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
    objstores_.push_back(ObjStoreHandle());
    objstores_[objstoreid].address = objstore_address;
    objstores_[objstoreid].channel = channel;
    objstores_[objstoreid].objstore_stub = ObjStore::NewStub(channel);
  }
  objstores_lock_.unlock();
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
  objtable_lock_.lock();
  ObjRef result = objtable_.size();
  objtable_.push_back(std::vector<ObjStoreId>());
  objtable_lock_.unlock();
  return result;
}

void SchedulerService::add_location(ObjRef objref, ObjStoreId objstoreid) {
  objtable_lock_.lock();
  // do a binary search
  auto pos = std::lower_bound(objtable_[objref].begin(), objtable_[objref].end(), objstoreid);
  if (pos == objtable_[objref].end() || objstoreid < *pos) {
    objtable_[objref].insert(pos, objstoreid);
  }
  objtable_lock_.unlock();
}

ObjStoreId SchedulerService::get_store(WorkerId workerid) {
  workers_lock_.lock();
  ObjStoreId result = workers_[workerid].objstoreid;
  workers_lock_.unlock();
  return result;
}

void SchedulerService::register_function(const std::string& name, WorkerId workerid, size_t num_return_vals) {
  fntable_lock_.lock();
  FnInfo& info = fntable_[name];
  info.set_num_return_vals(num_return_vals);
  info.add_worker(workerid);
  fntable_lock_.unlock();
}

void SchedulerService::debug_info(const GetDebugInfoRequest& request, GetDebugInfoReply* reply) {
  if (request.do_scheduling()) {
    schedule();
  }
  fntable_lock_.lock();
  auto function_table = reply->mutable_function_table();
  for (const auto& entry : fntable_) {
    (*function_table)[entry.first].set_num_return_vals(entry.second.num_return_vals());
    for (const WorkerId& worker : entry.second.workers()) {
      (*function_table)[entry.first].add_workerid(worker);
    }
  }
  fntable_lock_.unlock();
  tasks_lock_.lock();
  for (const auto& entry : tasks_) {
    Call* call = reply->add_task();
    call->CopyFrom(*entry);
  }
  tasks_lock_.unlock();
  avail_workers_lock_.lock();
  for (const WorkerId& entry : avail_workers_) {
    reply->add_avail_worker(entry);
  }
  avail_workers_lock_.unlock();
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
