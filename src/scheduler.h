#ifndef ORCHESTRA_SCHEDULER_H
#define ORCHESTRA_SCHEDULER_H

#include <deque>
#include <memory>

#include <grpc++/grpc++.h>

#include "orchestra/orchestra.h"
#include "orchestra.grpc.pb.h"
#include "types.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerReader;
using grpc::ServerContext;
using grpc::Status;

using grpc::Channel;

struct WorkerHandle {
  std::shared_ptr<Channel> channel;
  ObjStoreId objstoreid;
};

struct ObjStoreHandle {
  std::shared_ptr<Channel> channel;
  std::string address;
};

class Scheduler {
  // Vector of all workers registered in the system. Their index in this vector
  // is the workerid.
  std::vector<WorkerHandle> workers_;
  std::mutex workers_lock_;
  // Vector of all workers that are currently idle.
  std::vector<WorkerId> available_workers_;
  // Vector of all object stores registered in the system. Their index in this
  // vector is the objstoreid.
  std::vector<ObjStoreHandle> objstores_;
  grpc::mutex objstores_lock_;
  // Mapping from objref to list of object stores where the object is stored.
  ObjTable objtable_;
  std::mutex objtable_lock_;
  // Hash map from function names to workers where the function is registered.
  FnTable fntable_;
  std::mutex fntable_lock_;
  // List of pending tasks.
  std::deque<std::unique_ptr<Call> > tasks_;
  std::mutex tasks_lock_;
public:
  // returns number of return values of task
  size_t add_task(const Call& task) {
    fntable_lock_.lock();
    size_t num_return_vals = fntable_[task.name()].num_return_vals();
    fntable_lock_.unlock();
    std::unique_ptr<Call> task_ptr(new Call(task)); // TODO: perform copy outside
    tasks_lock_.lock();
    tasks_.emplace_back(std::move(task_ptr));
    tasks_lock_.unlock();
    return num_return_vals;
  }
  WorkerId register_worker(const std::string& worker_address, const std::string& objstore_address) {
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
      // throw objstore_not_registered_error("objectstore not registered");
      std::cout << "bad bad bad" << std::endl;
    }
    objstores_lock_.unlock();
    workers_lock_.lock();
    WorkerId result = workers_.size();
    workers_.push_back(WorkerHandle());
    workers_[result].channel = grpc::CreateChannel(worker_address, grpc::InsecureChannelCredentials());
    workers_[result].objstoreid = objstoreid;
    workers_lock_.unlock();
    return result;
  }
  ObjStoreId register_objstore(const std::string& objstore_address) {
    // auto handle = ObjStoreHandle(objstore_address);
    // auto handlecopy = handle;
    // auto handle = ObjStoreHandle("0.0.0.0:22222");
    objstores_lock_.lock();
    std::cout << "capacity" << objstores_.capacity() << std::endl;
    ObjStoreId result = objstores_.size();
    // auto handle = ObjStoreHandle(objstore_address);
    // objstores_.emplace_back(objstore_address);
    objstores_.push_back(ObjStoreHandle());

    objstores_[result].channel = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
    objstores_[result].address = std::string(objstore_address);

    // auto handlecopy = handle;
    // auto handle = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
    // auto handlecopy = grpc::CreateChannel(objstore_address, grpc::InsecureChannelCredentials());
    objstores_lock_.unlock();
    return result;
  }
  ObjRef register_new_object() {
    objtable_lock_.lock();
    ObjRef result = objtable_.size();
    objtable_.push_back(std::vector<ObjStoreId>());
    objtable_lock_.unlock();
    return result;
  }
  void add_objstore_to_obj(ObjRef objref, ObjStoreId objstoreid) {
    objtable_lock_.lock();
    // do a binary search
    auto pos = std::lower_bound(objtable_[objref].begin(), objtable_[objref].end(), objstoreid);
    if (pos == objtable_[objref].end() || objstoreid < *pos) {
      objtable_[objref].insert(pos, objstoreid);
    }
    objtable_lock_.unlock();
  }
  ObjStoreId get_store(WorkerId workerid) {
    workers_lock_.lock();
    ObjStoreId result = workers_[workerid].objstoreid;
    workers_lock_.unlock();
    return result;
  }
  void register_function(const std::string& name, WorkerId workerid, size_t num_return_vals) {
    fntable_lock_.lock();
    FnInfo& info = fntable_[name];
    info.set_num_return_vals(num_return_vals);
    info.add_worker(workerid);
    fntable_lock_.unlock();
  }
  void debug_info(GetDebugInfoReply* debug_info) {
    fntable_lock_.lock();
    for (const auto& entry : fntable_) {
      auto function_table = debug_info->mutable_function_table();
      (*function_table)[entry.first].set_num_return_vals(entry.second.num_return_vals());
      // TODO: set workerid
    }
    fntable_lock_.unlock();
    tasks_lock_.lock();
    for (const auto& entry : tasks_) {
      Call* call = debug_info->add_task();
      call->CopyFrom(*entry);
    }
    tasks_lock_.unlock();
  }
};

#endif
