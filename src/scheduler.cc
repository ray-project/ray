#include "scheduler.h"

size_t Scheduler::add_task(const Call& task) {
  fntable_lock_.lock();
  size_t num_return_vals = fntable_[task.name()].num_return_vals();
  fntable_lock_.unlock();
  std::unique_ptr<Call> task_ptr(new Call(task));
  tasks_lock_.lock();
  tasks_.emplace_back(std::move(task_ptr));
  tasks_lock_.unlock();
  return num_return_vals;
}

void Scheduler::schedule() {
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

void Scheduler::submit_task(std::unique_ptr<Call> call, WorkerId workerid) {
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

bool Scheduler::can_run(const Call& task) {
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

WorkerId Scheduler::register_worker(const std::string& worker_address, const std::string& objstore_address) {
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
  workers_[workerid].worker_stub = WorkerServer::NewStub(channel);
  workers_lock_.unlock();
  avail_workers_lock_.lock();
  avail_workers_.push_back(workerid);
  avail_workers_lock_.unlock();
  return workerid;
}

ObjRef Scheduler::register_new_object() {
  objtable_lock_.lock();
  ObjRef result = objtable_.size();
  objtable_.push_back(std::vector<ObjStoreId>());
  objtable_lock_.unlock();
  return result;
}

void Scheduler::add_location(ObjRef objref, ObjStoreId objstoreid) {
  objtable_lock_.lock();
  // do a binary search
  auto pos = std::lower_bound(objtable_[objref].begin(), objtable_[objref].end(), objstoreid);
  if (pos == objtable_[objref].end() || objstoreid < *pos) {
    objtable_[objref].insert(pos, objstoreid);
  }
  objtable_lock_.unlock();
}

ObjStoreId Scheduler::get_store(WorkerId workerid) {
  workers_lock_.lock();
  ObjStoreId result = workers_[workerid].objstoreid;
  workers_lock_.unlock();
  return result;
}

void Scheduler::register_function(const std::string& name, WorkerId workerid, size_t num_return_vals) {
  fntable_lock_.lock();
  FnInfo& info = fntable_[name];
  info.set_num_return_vals(num_return_vals);
  info.add_worker(workerid);
  fntable_lock_.unlock();
}

void Scheduler::debug_info(const GetDebugInfoRequest& request, GetDebugInfoReply* reply) {
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
