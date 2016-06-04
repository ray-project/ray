#include "scheduler.h"

#include <random>
#include <thread>
#include <chrono>

#include "utils.h"

SchedulerService::SchedulerService(SchedulingAlgorithmType scheduling_algorithm) : scheduling_algorithm_(scheduling_algorithm) {}

Status SchedulerService::SubmitTask(ServerContext* context, const SubmitTaskRequest* request, SubmitTaskReply* reply) {
  std::unique_ptr<Task> task(new Task(request->task())); // need to copy, because request is const
  fntable_lock_.lock();

  if (fntable_.find(task->name()) == fntable_.end()) {
    // TODO(rkn): In the future, this should probably not be fatal. Instead, propagate the error back to the worker.
    HALO_LOG(HALO_FATAL, "The function " << task->name() << " has not been registered by any worker.");
  }

  size_t num_return_vals = fntable_[task->name()].num_return_vals();
  fntable_lock_.unlock();

  std::vector<ObjRef> result_objrefs;
  for (size_t i = 0; i < num_return_vals; ++i) {
    ObjRef result = register_new_object();
    reply->add_result(result);
    task->add_result(result);
    result_objrefs.push_back(result);
  }
  {
    std::lock_guard<std::mutex> reference_counts_lock(reference_counts_lock_); // we grab this lock because increment_ref_count assumes it has been acquired
    increment_ref_count(result_objrefs); // We increment once so the objrefs don't go out of scope before we reply to the worker that called SubmitTask. The corresponding decrement will happen in submit_task in halolib.
    increment_ref_count(result_objrefs); // We increment once so the objrefs don't go out of scope before the task is scheduled on the worker. The corresponding decrement will happen in deserialize_task in halolib.
  }

  auto operation = std::unique_ptr<Operation>(new Operation());
  operation->set_allocated_task(task.release());
  OperationId creator_operationid = ROOT_OPERATION; // TODO(rkn): Later, this should be the ID of the task that spawned this current task.
  operation->set_creator_operationid(creator_operationid);
  computation_graph_lock_.lock();
  OperationId operationid = computation_graph_.add_operation(std::move(operation));
  computation_graph_lock_.unlock();

  task_queue_lock_.lock();
  task_queue_.push_back(operationid);
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

Status SchedulerService::RequestObj(ServerContext* context, const RequestObjRequest* request, AckReply* reply) {
  objtable_lock_.lock();
  size_t size = objtable_.size();
  objtable_lock_.unlock();

  ObjRef objref = request->objref();
  if (objref >= size) {
    HALO_LOG(HALO_FATAL, "internal error: no object with objref " << objref << " exists");
  }

  pull_queue_lock_.lock();
  pull_queue_.push_back(std::make_pair(request->workerid(), objref));
  pull_queue_lock_.unlock();
  schedule();
  return Status::OK;
}

Status SchedulerService::AliasObjRefs(ServerContext* context, const AliasObjRefsRequest* request, AckReply* reply) {
  ObjRef alias_objref = request->alias_objref();
  ObjRef target_objref = request->target_objref();
  HALO_LOG(HALO_ALIAS, "Aliasing objref " << alias_objref << " with objref " << target_objref);
  if (alias_objref == target_objref) {
    HALO_LOG(HALO_FATAL, "internal error: attempting to alias objref " << alias_objref << " with itself.");
  }
  objtable_lock_.lock();
  size_t size = objtable_.size();
  objtable_lock_.unlock();
  if (alias_objref >= size) {
    HALO_LOG(HALO_FATAL, "internal error: no object with objref " << alias_objref << " exists");
  }
  if (target_objref >= size) {
    HALO_LOG(HALO_FATAL, "internal error: no object with objref " << target_objref << " exists");
  }
  {
    std::lock_guard<std::mutex> target_objrefs_lock(target_objrefs_lock_);
    if (target_objrefs_[alias_objref] != UNITIALIZED_ALIAS) {
      HALO_LOG(HALO_FATAL, "internal error: attempting to alias objref " << alias_objref << " with objref " << target_objref << ", but objref " << alias_objref << " has already been aliased with objref " << target_objrefs_[alias_objref]);
    }
    target_objrefs_[alias_objref] = target_objref;
  }
  {
    std::lock_guard<std::mutex> reverse_target_objrefs_lock(reverse_target_objrefs_lock_);
    reverse_target_objrefs_[target_objref].push_back(alias_objref);
  }
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
  std::pair<WorkerId, ObjStoreId> info = register_worker(request->worker_address(), request->objstore_address());
  WorkerId workerid = info.first;
  ObjStoreId objstoreid = info.second;
  HALO_LOG(HALO_INFO, "registered worker with workerid " << workerid);
  reply->set_workerid(workerid);
  reply->set_objstoreid(objstoreid);
  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) {
  HALO_LOG(HALO_INFO, "register function " << request->fnname() <<  " from workerid " << request->workerid());
  register_function(request->fnname(), request->workerid(), request->num_return_vals());
  schedule();
  return Status::OK;
}

Status SchedulerService::ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) {
  ObjRef objref = request->objref();
  HALO_LOG(HALO_DEBUG, "object " << objref << " ready on store " << request->objstoreid());
  add_canonical_objref(objref);
  add_location(objref, request->objstoreid());
  schedule();
  return Status::OK;
}

Status SchedulerService::WorkerReady(ServerContext* context, const WorkerReadyRequest* request, AckReply* reply) {
  HALO_LOG(HALO_INFO, "worker " << request->workerid() << " reported back");
  {
    std::lock_guard<std::mutex> lock(avail_workers_lock_);
    avail_workers_.push_back(request->workerid());
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::IncrementRefCount(ServerContext* context, const IncrementRefCountRequest* request, AckReply* reply) {
  int num_objrefs = request->objref_size();
  if (num_objrefs == 0) {
    HALO_LOG(HALO_FATAL, "Scheduler received IncrementRefCountRequest with 0 objrefs.");
  }
  std::vector<ObjRef> objrefs;
  for (int i = 0; i < num_objrefs; ++i) {
    objrefs.push_back(request->objref(i));
  }
  std::lock_guard<std::mutex> reference_counts_lock(reference_counts_lock_); // we grab this lock because increment_ref_count assumes it has been acquired
  increment_ref_count(objrefs);
  return Status::OK;
}

Status SchedulerService::DecrementRefCount(ServerContext* context, const DecrementRefCountRequest* request, AckReply* reply) {
  int num_objrefs = request->objref_size();
  if (num_objrefs == 0) {
    HALO_LOG(HALO_FATAL, "Scheduler received DecrementRefCountRequest with 0 objrefs.");
  }
  std::vector<ObjRef> objrefs;
  for (int i = 0; i < num_objrefs; ++i) {
    objrefs.push_back(request->objref(i));
  }
  std::lock_guard<std::mutex> reference_counts_lock(reference_counts_lock_); // we grab this lock, because decrement_ref_count assumes it has been acquired
  decrement_ref_count(objrefs);
  return Status::OK;
}

Status SchedulerService::AddContainedObjRefs(ServerContext* context, const AddContainedObjRefsRequest* request, AckReply* reply) {
  ObjRef objref = request->objref();
  // if (!is_canonical(objref)) {
    // TODO(rkn): Perhaps we don't need this check. It won't work because the objstore may not have called ObjReady yet.
    // HALO_LOG(HALO_FATAL, "Attempting to add contained objrefs for non-canonical objref " << objref);
  // }
  std::lock_guard<std::mutex> contained_objrefs_lock(contained_objrefs_lock_);
  if (contained_objrefs_[objref].size() != 0) {
    HALO_LOG(HALO_FATAL, "Attempting to add contained objrefs for objref " << objref << ", but contained_objrefs_[objref].size() != 0.");
  }
  for (int i = 0; i < request->contained_objref_size(); ++i) {
    contained_objrefs_[objref].push_back(request->contained_objref(i));
  }
  return Status::OK;
}

Status SchedulerService::SchedulerInfo(ServerContext* context, const SchedulerInfoRequest* request, SchedulerInfoReply* reply) {
  get_info(*request, reply);
  return Status::OK;
}

// TODO(rkn): This could execute multiple times with the same arguments before
// the delivery finishes, but we only want it to happen once. Currently, the
// redundancy is handled by the object store, which will only execute the
// delivery once. However, we may want to handle it in the scheduler in the
// future.
//
// deliver_object assumes that the aliasing for objref has already been completed. That is, has_canonical_objref(objref) == true
void SchedulerService::deliver_object(ObjRef objref, ObjStoreId from, ObjStoreId to) {
  if (from == to) {
    HALO_LOG(HALO_FATAL, "attempting to deliver objref " << objref << " from objstore " << from << " to itself.");
  }
  if (!has_canonical_objref(objref)) {
    HALO_LOG(HALO_FATAL, "attempting to deliver objref " << objref << ", but this objref does not yet have a canonical objref.");
  }
  ClientContext context;
  AckReply reply;
  StartDeliveryRequest request;
  ObjRef canonical_objref = get_canonical_objref(objref);
  request.set_objref(canonical_objref);
  std::lock_guard<std::mutex> lock(objstores_lock_);
  request.set_objstore_address(objstores_[from].address);
  objstores_[to].objstore_stub->StartDelivery(&context, request, &reply);
}

void SchedulerService::schedule() {
  // TODO(rkn): Do this more intelligently.
  perform_pulls(); // See what we can do in pull_queue_
  if (scheduling_algorithm_ == SCHEDULING_ALGORITHM_NAIVE) {
    schedule_tasks_naively(); // See what we can do in task_queue_
  } else if (scheduling_algorithm_ == SCHEDULING_ALGORITHM_LOCALITY_AWARE) {
    schedule_tasks_location_aware(); // See what we can do in task_queue_
  } else {
    HALO_LOG(HALO_FATAL, "scheduling algorithm not known");
  }
  perform_notify_aliases(); // See what we can do in alias_notification_queue_
}

// assign_task assumes that computation_graph_lock_ has been acquired.
// assign_task assumes that the canonical objrefs for its arguments are all ready, that is has_canonical_objref() is true for all of the call's arguments
void SchedulerService::assign_task(OperationId operationid, WorkerId workerid) {
  const Task& task = computation_graph_.get_task(operationid);
  ClientContext context;
  ExecuteTaskRequest request;
  ExecuteTaskReply reply;
  HALO_LOG(HALO_INFO, "starting to send arguments");
  for (size_t i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjRef objref = task.arg(i).ref();
      ObjRef canonical_objref = get_canonical_objref(objref);
      {
        // Notify the relevant objstore about potential aliasing when it's ready
        std::lock_guard<std::mutex> alias_notification_queue_lock(alias_notification_queue_lock_);
        alias_notification_queue_.push_back(std::make_pair(get_store(workerid), std::make_pair(objref, canonical_objref)));
      }
      attempt_notify_alias(get_store(workerid), objref, canonical_objref);

      HALO_LOG(HALO_DEBUG, "task contains object ref " << canonical_objref);
      std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
      auto &objstores = objtable_[canonical_objref];
      std::lock_guard<std::mutex> workers_lock(workers_lock_);
      if (!std::binary_search(objstores.begin(), objstores.end(), workers_[workerid].objstoreid)) { // TODO(rkn): replace this with get_store
        deliver_object(canonical_objref, pick_objstore(canonical_objref), workers_[workerid].objstoreid); // TODO(rkn): replace this with get_store
      }
    }
  }
  request.mutable_task()->CopyFrom(task); // TODO(rkn): Is ownership handled properly here?
  Status status = workers_[workerid].worker_stub->ExecuteTask(&context, request, &reply);
}

bool SchedulerService::can_run(const Task& task) {
  std::lock_guard<std::mutex> lock(objtable_lock_);
  for (int i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjRef objref = task.arg(i).ref();
      if (!has_canonical_objref(objref)) {
        return false;
      }
      ObjRef canonical_objref = get_canonical_objref(objref);
      if (canonical_objref >= objtable_.size() || objtable_[canonical_objref].size() == 0) {
        return false;
      }
    }
  }
  return true;
}

std::pair<WorkerId, ObjStoreId> SchedulerService::register_worker(const std::string& worker_address, const std::string& objstore_address) {
  HALO_LOG(HALO_INFO, "registering worker " << worker_address << " connected to object store " << objstore_address);
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
    HALO_LOG(HALO_FATAL, "object store with address " << objstore_address << " not yet registered");
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
  return std::make_pair(workerid, objstoreid);
}

ObjRef SchedulerService::register_new_object() {
  // If we don't simultaneously lock objtable_ and target_objrefs_, we will probably get errors.
  // TODO(rkn): increment/decrement_reference_count also acquire reference_counts_lock_ and target_objrefs_lock_ (through has_canonical_objref()), which caused deadlock in the past
  std::lock_guard<std::mutex> reference_counts_lock(reference_counts_lock_);
  std::lock_guard<std::mutex> contained_objrefs_lock(contained_objrefs_lock_);
  std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
  std::lock_guard<std::mutex> target_objrefs_lock(target_objrefs_lock_);
  std::lock_guard<std::mutex> reverse_target_objrefs_lock(reverse_target_objrefs_lock_);
  ObjRef objtable_size = objtable_.size();
  ObjRef target_objrefs_size = target_objrefs_.size();
  ObjRef reverse_target_objrefs_size = reverse_target_objrefs_.size();
  ObjRef reference_counts_size = reference_counts_.size();
  ObjRef contained_objrefs_size = contained_objrefs_.size();
  if (objtable_size != target_objrefs_size) {
    HALO_LOG(HALO_FATAL, "objtable_ and target_objrefs_ should have the same size, but objtable_.size() = " << objtable_size << " and target_objrefs_.size() = " << target_objrefs_size);
  }
  if (objtable_size != reverse_target_objrefs_size) {
    HALO_LOG(HALO_FATAL, "objtable_ and reverse_target_objrefs_ should have the same size, but objtable_.size() = " << objtable_size << " and reverse_target_objrefs_.size() = " << reverse_target_objrefs_size);
  }
  if (objtable_size != reference_counts_size) {
    HALO_LOG(HALO_FATAL, "objtable_ and reference_counts_ should have the same size, but objtable_.size() = " << objtable_size << " and reference_counts_.size() = " << reference_counts_size);
  }
  if (objtable_size != contained_objrefs_size) {
    HALO_LOG(HALO_FATAL, "objtable_ and contained_objrefs_ should have the same size, but objtable_.size() = " << objtable_size << " and contained_objrefs_.size() = " << contained_objrefs_size);
  }
  objtable_.push_back(std::vector<ObjStoreId>());
  target_objrefs_.push_back(UNITIALIZED_ALIAS);
  reverse_target_objrefs_.push_back(std::vector<ObjRef>());
  reference_counts_.push_back(0);
  contained_objrefs_.push_back(std::vector<ObjRef>());
  return objtable_size;
}

void SchedulerService::add_location(ObjRef canonical_objref, ObjStoreId objstoreid) {
  // add_location must be called with a canonical objref
  if (!is_canonical(canonical_objref)) {
    HALO_LOG(HALO_FATAL, "Attempting to call add_location with a non-canonical objref (objref " << canonical_objref << ")");
  }
  std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
  if (canonical_objref >= objtable_.size()) {
    HALO_LOG(HALO_FATAL, "trying to put an object in the object store that was not registered with the scheduler (objref " << canonical_objref << ")");
  }
  // do a binary search
  auto pos = std::lower_bound(objtable_[canonical_objref].begin(), objtable_[canonical_objref].end(), objstoreid);
  if (pos == objtable_[canonical_objref].end() || objstoreid < *pos) {
    objtable_[canonical_objref].insert(pos, objstoreid);
  }
}

void SchedulerService::add_canonical_objref(ObjRef objref) {
  std::lock_guard<std::mutex> lock(target_objrefs_lock_);
  if (objref >= target_objrefs_.size()) {
    HALO_LOG(HALO_FATAL, "internal error: attempting to insert objref " << objref << " in target_objrefs_, but target_objrefs_.size() is " << target_objrefs_.size());
  }
  if (target_objrefs_[objref] != UNITIALIZED_ALIAS && target_objrefs_[objref] != objref) {
    HALO_LOG(HALO_FATAL, "internal error: attempting to declare objref " << objref << " as a canonical objref, but target_objrefs_[objref] is already aliased with objref " << target_objrefs_[objref]);
  }
  target_objrefs_[objref] = objref;
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

void SchedulerService::get_info(const SchedulerInfoRequest& request, SchedulerInfoReply* reply) {
  // TODO(rkn): Also grab the objstores_lock_
  // alias_notification_queue_lock_ may need to come before objtable_lock_
  std::lock_guard<std::mutex> reference_counts_lock(reference_counts_lock_);
  std::lock_guard<std::mutex> contained_objrefs_lock(contained_objrefs_lock_);
  std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
  std::lock_guard<std::mutex> pull_queue_lock(pull_queue_lock_);
  std::lock_guard<std::mutex> target_objrefs_lock(target_objrefs_lock_);
  std::lock_guard<std::mutex> reverse_target_objrefs_lock(reverse_target_objrefs_lock_);
  std::lock_guard<std::mutex> fntable_lock(fntable_lock_);
  std::lock_guard<std::mutex> avail_workers_lock(avail_workers_lock_);
  std::lock_guard<std::mutex> task_queue_lock(task_queue_lock_);
  std::lock_guard<std::mutex> alias_notification_queue_lock(alias_notification_queue_lock_);
  for (int i = 0; i < reference_counts_.size(); ++i) {
    reply->add_reference_count(reference_counts_[i]);
  }
  for (int i = 0; i < target_objrefs_.size(); ++i) {
    reply->add_target_objref(target_objrefs_[i]);
  }
  auto function_table = reply->mutable_function_table();
  for (const auto& entry : fntable_) {
    (*function_table)[entry.first].set_num_return_vals(entry.second.num_return_vals());
    for (const WorkerId& worker : entry.second.workers()) {
      (*function_table)[entry.first].add_workerid(worker);
    }
  }
  for (const auto& entry : task_queue_) {
    reply->add_operationid(entry);
  }
  for (const WorkerId& entry : avail_workers_) {
    reply->add_avail_worker(entry);
  }

}

// pick_objstore assumes that objtable_lock_ has been acquired
// pick_objstore must be called with a canonical_objref
ObjStoreId SchedulerService::pick_objstore(ObjRef canonical_objref) {
  std::mt19937 rng;
  if (!is_canonical(canonical_objref)) {
    HALO_LOG(HALO_FATAL, "Attempting to call pick_objstore with a non-canonical objref, (objref " << canonical_objref << ")");
  }
  std::uniform_int_distribution<int> uni(0, objtable_[canonical_objref].size() - 1);
  ObjStoreId objstoreid = objtable_[canonical_objref][uni(rng)];
  return objstoreid;
}

bool SchedulerService::is_canonical(ObjRef objref) {
  std::lock_guard<std::mutex> lock(target_objrefs_lock_);
  if (target_objrefs_[objref] == UNITIALIZED_ALIAS) {
    HALO_LOG(HALO_FATAL, "Attempting to call is_canonical on an objref for which aliasing is not complete or the object is not ready, target_objrefs_[objref] == UNITIALIZED_ALIAS for objref " << objref << ".");
  }
  return objref == target_objrefs_[objref];
}

void SchedulerService::perform_pulls() {
  std::lock_guard<std::mutex> pull_queue_lock(pull_queue_lock_);
  // Complete all pull tasks that can be completed.
  for (int i = 0; i < pull_queue_.size(); ++i) {
    const std::pair<WorkerId, ObjRef>& pull = pull_queue_[i];
    ObjRef objref = pull.second;
    WorkerId workerid = pull.first;
    if (!has_canonical_objref(objref)) {
      HALO_LOG(HALO_ALIAS, "objref " << objref << " does not have a canonical_objref, so continuing");
      continue;
    }
    ObjRef canonical_objref = get_canonical_objref(objref);
    HALO_LOG(HALO_DEBUG, "attempting to pull objref " << pull.second << " with canonical objref " << canonical_objref << " to objstore " << get_store(workerid));

    objtable_lock_.lock();
    int num_stores = objtable_[canonical_objref].size();
    objtable_lock_.unlock();

    if (num_stores > 0) {
      {
        std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
        if (!std::binary_search(objtable_[canonical_objref].begin(), objtable_[canonical_objref].end(), get_store(workerid))) {
          // The worker's local object store does not already contain objref, so ship
          // it there from an object store that does have it.
          ObjStoreId objstoreid = pick_objstore(canonical_objref);
          deliver_object(canonical_objref, objstoreid, get_store(workerid));
        }
      }
      {
        // Notify the relevant objstore about potential aliasing when it's ready
        std::lock_guard<std::mutex> alias_notification_queue_lock(alias_notification_queue_lock_);
        alias_notification_queue_.push_back(std::make_pair(get_store(workerid), std::make_pair(objref, canonical_objref)));
      }
      // Remove the pull task from the queue
      std::swap(pull_queue_[i], pull_queue_[pull_queue_.size() - 1]);
      pull_queue_.pop_back();
      i -= 1;
    }
  }
}

void SchedulerService::schedule_tasks_naively() {
  std::lock_guard<std::mutex> computation_graph_lock(computation_graph_lock_);
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
      const OperationId operationid = *it;
      const Task& task = computation_graph_.get_task(operationid);
      auto& workers = fntable_[task.name()].workers();
      if (std::binary_search(workers.begin(), workers.end(), workerid) && can_run(task)) {
        assign_task(operationid, workerid);
        task_queue_.erase(it);
        std::swap(avail_workers_[i], avail_workers_[avail_workers_.size() - 1]);
        avail_workers_.pop_back();
        i -= 1;
        break;
      }
    }
  }
}

void SchedulerService::schedule_tasks_location_aware() {
  std::lock_guard<std::mutex> computation_graph_lock(computation_graph_lock_);
  std::lock_guard<std::mutex> fntable_lock(fntable_lock_);
  std::lock_guard<std::mutex> avail_workers_lock(avail_workers_lock_);
  std::lock_guard<std::mutex> task_queue_lock(task_queue_lock_);
  for (int i = 0; i < avail_workers_.size(); ++i) {
    // Submit all tasks whose arguments are ready.
    WorkerId workerid = avail_workers_[i];
    ObjStoreId objstoreid = workers_[workerid].objstoreid;
    auto bestit = task_queue_.end(); // keep track of the task that fits the worker best so far
    size_t min_num_shipped_objects = std::numeric_limits<size_t>::max(); // number of objects that need to be transfered for this worker
    for (auto it = task_queue_.begin(); it != task_queue_.end(); ++it) {
      OperationId operationid = *it;
      const Task& task = computation_graph_.get_task(operationid);
      auto& workers = fntable_[task.name()].workers();
      if (std::binary_search(workers.begin(), workers.end(), workerid) && can_run(task)) {
        // determine how many objects would need to be shipped
        size_t num_shipped_objects = 0;
        for (int j = 0; j < task.arg_size(); ++j) {
          if (!task.arg(j).has_obj()) {
            ObjRef objref = task.arg(j).ref();
            if (!has_canonical_objref(objref)) {
              HALO_LOG(HALO_FATAL, "no canonical object ref found even though task is ready; that should not be possible!");
            }
            ObjRef canonical_objref = get_canonical_objref(objref);
            // check if the object is already in the local object store
            if (!std::binary_search(objtable_[canonical_objref].begin(), objtable_[canonical_objref].end(), objstoreid)) {
              num_shipped_objects += 1;
            }
          }
        }
        if (num_shipped_objects < min_num_shipped_objects) {
          min_num_shipped_objects = num_shipped_objects;
          bestit = it;
        }
      }
    }
    // if we found a suitable task
    if (bestit != task_queue_.end()) {
      assign_task(*bestit, workerid);
      task_queue_.erase(bestit);
      std::swap(avail_workers_[i], avail_workers_[avail_workers_.size() - 1]);
      avail_workers_.pop_back();
      i -= 1;
    }
  }
}

void SchedulerService::perform_notify_aliases() {
  std::lock_guard<std::mutex> alias_notification_queue_lock(alias_notification_queue_lock_);
  for (int i = 0; i < alias_notification_queue_.size(); ++i) {
    const std::pair<WorkerId, std::pair<ObjRef, ObjRef> > alias_notification = alias_notification_queue_[i];
    ObjStoreId objstoreid = alias_notification.first;
    ObjRef alias_objref = alias_notification.second.first;
    ObjRef canonical_objref = alias_notification.second.second;
    if (attempt_notify_alias(objstoreid, alias_objref, canonical_objref)) { // this locks both the objstore_ and objtable_
      // the attempt to notify the objstore of the objref aliasing succeeded, so remove the notification task from the queue
      std::swap(alias_notification_queue_[i], alias_notification_queue_[alias_notification_queue_.size() - 1]);
      alias_notification_queue_.pop_back();
      i -= 1;
    }
  }
}

bool SchedulerService::has_canonical_objref(ObjRef objref) {
  std::lock_guard<std::mutex> lock(target_objrefs_lock_);
  ObjRef objref_temp = objref;
  while (true) {
    if (objref_temp >= target_objrefs_.size()) {
      HALO_LOG(HALO_FATAL, "Attempting to index target_objrefs_ with objref " << objref_temp << ", but target_objrefs_.size() = " << target_objrefs_.size());
    }
    if (target_objrefs_[objref_temp] == UNITIALIZED_ALIAS) {
      return false;
    }
    if (target_objrefs_[objref_temp] == objref_temp) {
      return true;
    }
    objref_temp = target_objrefs_[objref_temp];
  }
}

ObjRef SchedulerService::get_canonical_objref(ObjRef objref) {
  // get_canonical_objref assumes that has_canonical_objref(objref) is true
  std::lock_guard<std::mutex> lock(target_objrefs_lock_);
  ObjRef objref_temp = objref;
  while (true) {
    if (objref_temp >= target_objrefs_.size()) {
      HALO_LOG(HALO_FATAL, "Attempting to index target_objrefs_ with objref " << objref_temp << ", but target_objrefs_.size() = " << target_objrefs_.size());
    }
    if (target_objrefs_[objref_temp] == UNITIALIZED_ALIAS) {
      HALO_LOG(HALO_FATAL, "Attempting to get canonical objref for objref " << objref << ", which aliases, objref " << objref_temp << ", but target_objrefs_[objref_temp] == UNITIALIZED_ALIAS for objref_temp = " << objref_temp << ".");
    }
    if (target_objrefs_[objref_temp] == objref_temp) {
      return objref_temp;
    }
    objref_temp = target_objrefs_[objref_temp];
    HALO_LOG(HALO_ALIAS, "Looping in get_canonical_objref.");
  }
}

bool SchedulerService::attempt_notify_alias(ObjStoreId objstoreid, ObjRef alias_objref, ObjRef canonical_objref) {
  // return true if successful and false otherwise
  if (alias_objref == canonical_objref) {
    // no need to do anything
    return true;
  }
  {
    std::lock_guard<std::mutex> lock(objtable_lock_);
    if (!std::binary_search(objtable_[canonical_objref].begin(), objtable_[canonical_objref].end(), objstoreid)) {
      // the objstore doesn't have the object for canonical_objref yet, so it's too early to notify the objstore about the alias
      return false;
    }
  }
  ClientContext context;
  AckReply reply;
  NotifyAliasRequest request;
  request.set_alias_objref(alias_objref);
  request.set_canonical_objref(canonical_objref);
  objstores_lock_.lock();
  objstores_[objstoreid].objstore_stub->NotifyAlias(&context, request, &reply);
  objstores_lock_.unlock();
  return true;
}

void SchedulerService::deallocate_object(ObjRef canonical_objref) {
  // deallocate_object should only be called from decrement_ref_count (note that
  // deallocate_object also recursively calls decrement_ref_count). Both of
  // these methods require reference_counts_lock_ to have been acquired, and
  // so the lock must before outside of these methods (it is acquired in
  // DecrementRefCount).
  HALO_LOG(HALO_REFCOUNT, "Deallocating canonical_objref " << canonical_objref << ".");
  {
    std::lock_guard<std::mutex> objtable_lock(objtable_lock_);
    auto &objstores = objtable_[canonical_objref];
    std::lock_guard<std::mutex> objstores_lock(objstores_lock_); // TODO(rkn): Should this be inside the for loop instead?
    for (int i = 0; i < objstores.size(); ++i) {
      ClientContext context;
      AckReply reply;
      DeallocateObjectRequest request;
      request.set_canonical_objref(canonical_objref);
      ObjStoreId objstoreid = objstores[i];
      HALO_LOG(HALO_REFCOUNT, "Attempting to deallocate canonical_objref " << canonical_objref << " from objstore " << objstoreid);
      objstores_[objstoreid].objstore_stub->DeallocateObject(&context, request, &reply);
    }
    objtable_[canonical_objref].clear();
  }
  decrement_ref_count(contained_objrefs_[canonical_objref]);
}

void SchedulerService::increment_ref_count(std::vector<ObjRef> &objrefs) {
  // increment_ref_count assumes that reference_counts_lock_ has been acquired already
  for (int i = 0; i < objrefs.size(); ++i) {
    ObjRef objref = objrefs[i];
    if (reference_counts_[objref] == DEALLOCATED) {
      HALO_LOG(HALO_FATAL, "Attempting to increment the reference count for objref " << objref << ", but this object appears to have been deallocated already.");
    }
    reference_counts_[objref] += 1;
    HALO_LOG(HALO_REFCOUNT, "Incremented ref count for objref " << objref <<". New reference count is " << reference_counts_[objref]);
  }
}

void SchedulerService::decrement_ref_count(std::vector<ObjRef> &objrefs) {
  // decrement_ref_count assumes that reference_counts_lock_ has been acquired already
  for (int i = 0; i < objrefs.size(); ++i) {
    ObjRef objref = objrefs[i];
    if (reference_counts_[objref] == DEALLOCATED) {
      HALO_LOG(HALO_FATAL, "Attempting to decrement the reference count for objref " << objref << ", but this object appears to have been deallocated already.");
    }
    if (reference_counts_[objref] == 0) {
      HALO_LOG(HALO_FATAL, "Attempting to decrement the reference count for objref " << objref << ", but the reference count for this object is already 0.");
    }
    reference_counts_[objref] -= 1;
    HALO_LOG(HALO_REFCOUNT, "Decremented ref count for objref " << objref << ". New reference count is " << reference_counts_[objref]);
    // See if we can deallocate the object
    std::vector<ObjRef> equivalent_objrefs;
    get_equivalent_objrefs(objref, equivalent_objrefs);
    bool can_deallocate = true;
    for (int j = 0; j < equivalent_objrefs.size(); ++j) {
      if (reference_counts_[equivalent_objrefs[j]] != 0) {
        can_deallocate = false;
        break;
      }
    }
    if (can_deallocate) {
      ObjRef canonical_objref = equivalent_objrefs[0];
      if (!is_canonical(canonical_objref)) {
        HALO_LOG(HALO_FATAL, "canonical_objref is not canonical.");
      }
      deallocate_object(canonical_objref);
      for (int j = 0; j < equivalent_objrefs.size(); ++j) {
        reference_counts_[equivalent_objrefs[j]] = DEALLOCATED;
      }
    }
  }
}

void SchedulerService::upstream_objrefs(ObjRef objref, std::vector<ObjRef> &objrefs) {
  // upstream_objrefs assumes that the lock reverse_target_objrefs_lock_ has been acquired
  objrefs.push_back(objref);
  for (int i = 0; i < reverse_target_objrefs_[objref].size(); ++i) {
    upstream_objrefs(reverse_target_objrefs_[objref][i], objrefs);
  }
}

void SchedulerService::get_equivalent_objrefs(ObjRef objref, std::vector<ObjRef> &equivalent_objrefs) {
  std::lock_guard<std::mutex> target_objrefs_lock(target_objrefs_lock_);
  ObjRef downstream_objref = objref;
  while (target_objrefs_[downstream_objref] != downstream_objref && target_objrefs_[downstream_objref] != UNITIALIZED_ALIAS) {
    HALO_LOG(HALO_ALIAS, "Looping in get_equivalent_objrefs");
    downstream_objref = target_objrefs_[downstream_objref];
  }
  std::lock_guard<std::mutex> reverse_target_objrefs_lock(reverse_target_objrefs_lock_);
  upstream_objrefs(downstream_objref, equivalent_objrefs);
}

void start_scheduler_service(const char* service_addr, SchedulingAlgorithmType scheduling_algorithm) {
  std::string service_address(service_addr);
  std::string::iterator split_point = split_ip_address(service_address);
  std::string port;
  port.assign(split_point, service_address.end());
  SchedulerService service(scheduling_algorithm);
  ServerBuilder builder;
  builder.AddListeningPort(std::string("0.0.0.0:") + port, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}

char* get_cmd_option(char** begin, char** end, const std::string& option) {
  char** it = std::find(begin, end, option);
  if (it != end && ++it != end) {
    return *it;
  }
  return 0;
}

int main(int argc, char** argv) {
  SchedulingAlgorithmType scheduling_algorithm = SCHEDULING_ALGORITHM_LOCALITY_AWARE;
  if (argc < 2) {
    HALO_LOG(HALO_FATAL, "scheduler: expected at least one argument (scheduler ip address)");
    return 1;
  }
  if (argc > 2) {
    char* scheduling_algorithm_name = get_cmd_option(argv, argv + argc, "--scheduler-algorithm");
    if (scheduling_algorithm_name) {
      if(std::string(scheduling_algorithm_name) == "naive") {
        std::cout << "using 'naive' scheduler" << std::endl;
        scheduling_algorithm = SCHEDULING_ALGORITHM_NAIVE;
      }
      if(std::string(scheduling_algorithm_name) == "locality_aware") {
        std::cout << "using 'locality aware' scheduler" << std::endl;
        scheduling_algorithm = SCHEDULING_ALGORITHM_LOCALITY_AWARE;
      }
    }
  }
  start_scheduler_service(argv[1], scheduling_algorithm);
  return 0;
}
