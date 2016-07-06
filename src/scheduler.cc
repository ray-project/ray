#include "scheduler.h"

#include <random>
#include <thread>
#include <chrono>

#include "utils.h"

SchedulerService::SchedulerService(SchedulingAlgorithmType scheduling_algorithm) : scheduling_algorithm_(scheduling_algorithm) {}

Status SchedulerService::SubmitTask(ServerContext* context, const SubmitTaskRequest* request, SubmitTaskReply* reply) {
  std::unique_ptr<Task> task(new Task(request->task())); // need to copy, because request is const
  size_t num_return_vals;
  {
    auto fntable = fntable_.get();
    FnTable::const_iterator fn = fntable->find(task->name());
    if (fn == fntable->end()) {
      num_return_vals = 0;
      reply->set_function_registered(false);
    } else {
      num_return_vals = fn->second.num_return_vals();
      reply->set_function_registered(true);
    }
  }
  if (reply->function_registered()) {
    std::vector<ObjRef> result_objrefs;
    for (size_t i = 0; i < num_return_vals; ++i) {
      ObjRef result = register_new_object();
      reply->add_result(result);
      task->add_result(result);
      result_objrefs.push_back(result);
    }
    {
      auto reference_counts = reference_counts_.get();
      increment_ref_count(result_objrefs, reference_counts); // We increment once so the objrefs don't go out of scope before we reply to the worker that called SubmitTask. The corresponding decrement will happen in submit_task in raylib.
      increment_ref_count(result_objrefs, reference_counts); // We increment once so the objrefs don't go out of scope before the task is scheduled on the worker. The corresponding decrement will happen in deserialize_task in raylib.
    }

    auto operation = std::unique_ptr<Operation>(new Operation());
    operation->set_allocated_task(task.release());
    operation->set_creator_operationid((*workers_.get())[request->workerid()].current_task);

    OperationId operationid = computation_graph_.get()->add_operation(std::move(operation));
    task_queue_.get()->push_back(operationid);
    schedule();
  }
  return Status::OK;
}

Status SchedulerService::PutObj(ServerContext* context, const PutObjRequest* request, PutObjReply* reply) {
  ObjRef objref = register_new_object();
  auto operation = std::unique_ptr<Operation>(new Operation());
  operation->mutable_put()->set_objref(objref);
  operation->set_creator_operationid((*workers_.get())[request->workerid()].current_task);
  computation_graph_.get()->add_operation(std::move(operation));
  reply->set_objref(objref);
  schedule();
  return Status::OK;
}

Status SchedulerService::RequestObj(ServerContext* context, const RequestObjRequest* request, AckReply* reply) {
  size_t size = objtable_.get()->size();
  ObjRef objref = request->objref();
  RAY_CHECK_LT(objref, size, "internal error: no object with objref " << objref << " exists");
  auto operation = std::unique_ptr<Operation>(new Operation());
  operation->mutable_get()->set_objref(objref);
  operation->set_creator_operationid((*workers_.get())[request->workerid()].current_task);
  computation_graph_.get()->add_operation(std::move(operation));
  get_queue_.get()->push_back(std::make_pair(request->workerid(), objref));
  schedule();
  return Status::OK;
}

Status SchedulerService::AliasObjRefs(ServerContext* context, const AliasObjRefsRequest* request, AckReply* reply) {
  ObjRef alias_objref = request->alias_objref();
  ObjRef target_objref = request->target_objref();
  RAY_LOG(RAY_ALIAS, "Aliasing objref " << alias_objref << " with objref " << target_objref);
  RAY_CHECK_NEQ(alias_objref, target_objref, "internal error: attempting to alias objref " << alias_objref << " with itself.");
  size_t size = objtable_.get()->size();
  RAY_CHECK_LT(alias_objref, size, "internal error: no object with objref " << alias_objref << " exists");
  RAY_CHECK_LT(target_objref, size, "internal error: no object with objref " << target_objref << " exists");
  {
    auto target_objrefs = target_objrefs_.get();
    RAY_CHECK_EQ((*target_objrefs)[alias_objref], UNITIALIZED_ALIAS, "internal error: attempting to alias objref " << alias_objref << " with objref " << target_objref << ", but objref " << alias_objref << " has already been aliased with objref " << (*target_objrefs)[alias_objref]);
    (*target_objrefs)[alias_objref] = target_objref;
  }
  (*reverse_target_objrefs_.get())[target_objref].push_back(alias_objref);
  {
    // The corresponding increment was done in register_new_object.
    auto reference_counts = reference_counts_.get(); // we grab this lock because decrement_ref_count assumes it has been acquired
    auto contained_objrefs = contained_objrefs_.get(); // we grab this lock because decrement_ref_count assumes it has been acquired
    decrement_ref_count(std::vector<ObjRef>({alias_objref}), reference_counts, contained_objrefs);
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) {
  auto objtable = objtable_.get(); // to protect objects_in_transit_
  auto objstores = objstores_.get();
  ObjStoreId objstoreid = objstores->size();
  auto channel = grpc::CreateChannel(request->objstore_address(), grpc::InsecureChannelCredentials());
  objstores->push_back(ObjStoreHandle());
  (*objstores)[objstoreid].address = request->objstore_address();
  (*objstores)[objstoreid].channel = channel;
  (*objstores)[objstoreid].objstore_stub = ObjStore::NewStub(channel);
  reply->set_objstoreid(objstoreid);
  objects_in_transit_.push_back(std::vector<ObjRef>());
  return Status::OK;
}

Status SchedulerService::RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) {
  std::pair<WorkerId, ObjStoreId> info = register_worker(request->worker_address(), request->objstore_address(), request->is_driver());
  WorkerId workerid = info.first;
  ObjStoreId objstoreid = info.second;
  RAY_LOG(RAY_INFO, "registered worker with workerid " << workerid);
  reply->set_workerid(workerid);
  reply->set_objstoreid(objstoreid);
  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) {
  RAY_LOG(RAY_INFO, "register function " << request->fnname() <<  " from workerid " << request->workerid());
  register_function(request->fnname(), request->workerid(), request->num_return_vals());
  schedule();
  return Status::OK;
}

Status SchedulerService::ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) {
  ObjRef objref = request->objref();
  RAY_LOG(RAY_DEBUG, "object " << objref << " ready on store " << request->objstoreid());
  add_canonical_objref(objref);
  add_location(objref, request->objstoreid());
  {
    // If this is the first time that ObjReady has been called for this objref,
    // the corresponding increment was done in register_new_object in the
    // scheduler. For all subsequent calls to ObjReady, the corresponding
    // increment was done in deliver_object_if_necessary in the scheduler.
    auto reference_counts = reference_counts_.get(); // we grab this lock because decrement_ref_count assumes it has been acquired
    auto contained_objrefs = contained_objrefs_.get(); // we grab this lock because decrement_ref_count assumes it has been acquired
    decrement_ref_count(std::vector<ObjRef>({objref}), reference_counts, contained_objrefs);
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::ReadyForNewTask(ServerContext* context, const ReadyForNewTaskRequest* request, AckReply* reply) {
  WorkerId workerid = request->workerid();
  OperationId operationid = (*workers_.get())[workerid].current_task;
  RAY_LOG(RAY_INFO, "worker " << workerid << " is ready for a new task");
  RAY_CHECK(operationid != ROOT_OPERATION, "A driver appears to have called ReadyForNewTask.");
  if (request->has_previous_task_info()) {
    RAY_CHECK(operationid != NO_OPERATION, "request->has_previous_task_info() should not be true if operationid == NO_OPERATION.");
    std::string task_name;
    task_name = computation_graph_.get()->get_task(operationid).name();
    TaskStatus info;
    {
      auto workers = workers_.get();
      info.set_operationid(operationid);
      info.set_function_name(task_name);
      info.set_worker_address((*workers)[workerid].worker_address);
      info.set_error_message(request->previous_task_info().error_message());
      (*workers)[workerid].current_task = NO_OPERATION; // clear operation ID
    }
    if (!request->previous_task_info().task_succeeded()) {
      RAY_LOG(RAY_INFO, "Error: Task " << info.operationid() << " executing function " << info.function_name() << " on worker " << workerid << " failed with error message:\n" << info.error_message());
      failed_tasks_.get()->push_back(info);
    } else {
      successful_tasks_.get()->push_back(info.operationid());
    }
    // TODO(rkn): Handle task failure
  }
  avail_workers_.get()->push_back(workerid);
  schedule();
  return Status::OK;
}

Status SchedulerService::IncrementRefCount(ServerContext* context, const IncrementRefCountRequest* request, AckReply* reply) {
  int num_objrefs = request->objref_size();
  RAY_CHECK_NEQ(num_objrefs, 0, "Scheduler received IncrementRefCountRequest with 0 objrefs.");
  std::vector<ObjRef> objrefs;
  for (int i = 0; i < num_objrefs; ++i) {
    objrefs.push_back(request->objref(i));
  }
  auto reference_counts = reference_counts_.get();
  increment_ref_count(objrefs, reference_counts);
  return Status::OK;
}

Status SchedulerService::DecrementRefCount(ServerContext* context, const DecrementRefCountRequest* request, AckReply* reply) {
  int num_objrefs = request->objref_size();
  RAY_CHECK_NEQ(num_objrefs, 0, "Scheduler received DecrementRefCountRequest with 0 objrefs.");
  std::vector<ObjRef> objrefs;
  for (int i = 0; i < num_objrefs; ++i) {
    objrefs.push_back(request->objref(i));
  }
  auto reference_counts = reference_counts_.get(); // we grab this lock, because decrement_ref_count assumes it has been acquired
  auto contained_objrefs = contained_objrefs_.get(); // we grab this lock because decrement_ref_count assumes it has been acquired
  decrement_ref_count(objrefs, reference_counts, contained_objrefs);
  return Status::OK;
}

Status SchedulerService::AddContainedObjRefs(ServerContext* context, const AddContainedObjRefsRequest* request, AckReply* reply) {
  ObjRef objref = request->objref();
  // if (!is_canonical(objref)) {
    // TODO(rkn): Perhaps we don't need this check. It won't work because the objstore may not have called ObjReady yet.
    // RAY_LOG(RAY_FATAL, "Attempting to add contained objrefs for non-canonical objref " << objref);
  // }
  auto contained_objrefs = contained_objrefs_.get();
  RAY_CHECK_EQ((*contained_objrefs)[objref].size(), 0, "Attempting to add contained objrefs for objref " << objref << ", but contained_objrefs_[objref].size() != 0.");
  for (int i = 0; i < request->contained_objref_size(); ++i) {
    (*contained_objrefs)[objref].push_back(request->contained_objref(i));
  }
  return Status::OK;
}

Status SchedulerService::SchedulerInfo(ServerContext* context, const SchedulerInfoRequest* request, SchedulerInfoReply* reply) {
  get_info(*request, reply);
  return Status::OK;
}

Status SchedulerService::TaskInfo(ServerContext* context, const TaskInfoRequest* request, TaskInfoReply* reply) {
  auto successful_tasks = successful_tasks_.get();
  auto failed_tasks = failed_tasks_.get();
  auto computation_graph = computation_graph_.get();
  auto workers = workers_.get();
  for (int i = 0; i < failed_tasks->size(); ++i) {
    TaskStatus* info = reply->add_failed_task();
    *info = (*failed_tasks)[i];
  }
  for (size_t i = 0; i < workers->size(); ++i) {
    OperationId operationid = (*workers)[i].current_task;
    if (operationid != NO_OPERATION && operationid != ROOT_OPERATION) {
      const Task& task = computation_graph->get_task(operationid);
      TaskStatus* info = reply->add_running_task();
      info->set_operationid(operationid);
      info->set_function_name(task.name());
      info->set_worker_address((*workers)[i].worker_address);
    }
  }
  reply->set_num_succeeded(successful_tasks->size());
  return Status::OK;
}

Status SchedulerService::KillWorkers(ServerContext* context, const KillWorkersRequest* request, KillWorkersReply* reply) {
  // TODO: Update reference counts
  auto failed_tasks = failed_tasks_.get();
  auto get_queue = get_queue_.get();
  auto computation_graph = computation_graph_.get();
  auto fntable = fntable_.get();
  auto avail_workers = avail_workers_.get();
  auto task_queue = task_queue_.get();
  auto workers = workers_.get();
  size_t busy_workers = 0;
  std::vector<WorkerHandle*> idle_workers;
  RAY_LOG(RAY_INFO, "Attempting to kill workers.");
  for (size_t i = 0; i < workers->size(); ++i) {
    WorkerHandle* worker = &(*workers)[i];
    if (worker->worker_stub) {
      if (worker->current_task == NO_OPERATION) {
        idle_workers.push_back(worker);
        RAY_CHECK(std::find(avail_workers->begin(), avail_workers->end(), i) != avail_workers->end(), "Worker with workerid " << i << " is idle, but is not in avail_workers_");
        RAY_LOG(RAY_INFO, "Worker with workerid " << i << " is idle.");
      } else if (worker->current_task == ROOT_OPERATION) {
        // Skip the driver
        RAY_LOG(RAY_INFO, "Worker with workerid " << i << " is a driver.");
      } else {
        ++busy_workers;
        RAY_LOG(RAY_INFO, "Worker with workerid " << i << " is running a task.");
      }
    }
  }
  if (task_queue->empty() && busy_workers == 0) {
    RAY_LOG(RAY_INFO, "Killing " << idle_workers.size() << " idle workers.");
    for (WorkerHandle* idle_worker : idle_workers) {
      ClientContext client_context;
      DieRequest die_request;
      DieReply die_reply;
      // TODO: Fault handling... what if a worker refuses to die? We just assume it dies here.
      idle_worker->worker_stub->Die(&client_context, die_request, &die_reply);
      idle_worker->worker_stub.reset();
    }
    avail_workers->clear();
    fntable->clear();
    reply->set_success(true);
  } else {
    RAY_LOG(RAY_INFO, "Either the task queue is not empty or there are still busy workers, so we are not killing any workers.");
    reply->set_success(false);
  }
  return Status::OK;
}

void SchedulerService::deliver_object_async_if_necessary(ObjRef canonical_objref, ObjStoreId from, ObjStoreId to) {
  bool object_present_or_in_transit;
  {
    auto objtable = objtable_.get();
    auto &locations = (*objtable)[canonical_objref];
    bool object_present = std::binary_search(locations.begin(), locations.end(), to);
    auto &objects_in_flight = objects_in_transit_[to];
    bool object_in_transit = (std::find(objects_in_flight.begin(), objects_in_flight.end(), canonical_objref) != objects_in_flight.end());
    object_present_or_in_transit = object_present || object_in_transit;
    if (!object_present_or_in_transit) {
      objects_in_flight.push_back(canonical_objref);
    }
  }
  if (!object_present_or_in_transit) {
    deliver_object_async(canonical_objref, from, to);
  }
}

// TODO(rkn): This could execute multiple times with the same arguments before
// the delivery finishes, but we only want it to happen once. Currently, the
// redundancy is handled by the object store, which will only execute the
// delivery once. However, we may want to handle it in the scheduler in the
// future.
//
// deliver_object_async assumes that the aliasing for objref has already been completed. That is, has_canonical_objref(objref) == true
void SchedulerService::deliver_object_async(ObjRef canonical_objref, ObjStoreId from, ObjStoreId to) {
  RAY_CHECK_NEQ(from, to, "attempting to deliver canonical_objref " << canonical_objref << " from objstore " << from << " to itself.");
  RAY_CHECK(is_canonical(canonical_objref), "attempting to deliver objref " << canonical_objref << ", but this objref is not a canonical objref.");
  {
    // We increment once so the objref doesn't go out of scope before the ObjReady
    // method is called. The corresponding decrement will happen in ObjReady in
    // the scheduler.
    auto reference_counts = reference_counts_.get(); // we grab this lock because increment_ref_count assumes it has been acquired
    increment_ref_count(std::vector<ObjRef>({canonical_objref}), reference_counts);
  }
  ClientContext context;
  AckReply reply;
  StartDeliveryRequest request;
  request.set_objref(canonical_objref);
  auto objstores = objstores_.get();
  request.set_objstore_address((*objstores)[from].address);
  (*objstores)[to].objstore_stub->StartDelivery(&context, request, &reply);
}

void SchedulerService::schedule() {
  // TODO(rkn): Do this more intelligently.
  perform_gets(); // See what we can do in get_queue_
  if (scheduling_algorithm_ == SCHEDULING_ALGORITHM_NAIVE) {
    schedule_tasks_naively(); // See what we can do in task_queue_
  } else if (scheduling_algorithm_ == SCHEDULING_ALGORITHM_LOCALITY_AWARE) {
    schedule_tasks_location_aware(); // See what we can do in task_queue_
  } else {
    RAY_CHECK(false, "scheduling algorithm not known");
  }
  perform_notify_aliases(); // See what we can do in alias_notification_queue_
}

// assign_task assumes that the canonical objrefs for its arguments are all ready, that is has_canonical_objref() is true for all of the call's arguments
void SchedulerService::assign_task(OperationId operationid, WorkerId workerid, const SynchronizedPtr<ComputationGraph> &computation_graph) {
  // assign_task takes computation_graph as an argument, which is obtained by
  // computation_graph_.get(), so we know that the data structure has been
  // locked.
  ObjStoreId objstoreid = get_store(workerid);
  const Task& task = computation_graph->get_task(operationid);
  ClientContext context;
  ExecuteTaskRequest request;
  ExecuteTaskReply reply;
  RAY_LOG(RAY_INFO, "starting to send arguments");
  for (size_t i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjRef objref = task.arg(i).ref();
      ObjRef canonical_objref = get_canonical_objref(objref);
      // Notify the relevant objstore about potential aliasing when it's ready
      alias_notification_queue_.get()->push_back(std::make_pair(objstoreid, std::make_pair(objref, canonical_objref)));
      attempt_notify_alias(objstoreid, objref, canonical_objref);
      RAY_LOG(RAY_DEBUG, "task contains object ref " << canonical_objref);
      deliver_object_async_if_necessary(canonical_objref, pick_objstore(canonical_objref), objstoreid);
    }
  }
  {
    auto workers = workers_.get();
    (*workers)[workerid].current_task = operationid;
    request.mutable_task()->CopyFrom(task); // TODO(rkn): Is ownership handled properly here?
    Status status = (*workers)[workerid].worker_stub->ExecuteTask(&context, request, &reply);
  }
}

bool SchedulerService::can_run(const Task& task) {
  auto objtable = objtable_.get();
  for (int i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjRef objref = task.arg(i).ref();
      if (!has_canonical_objref(objref)) {
        return false;
      }
      ObjRef canonical_objref = get_canonical_objref(objref);
      if (canonical_objref >= objtable->size() || (*objtable)[canonical_objref].size() == 0) {
        return false;
      }
    }
  }
  return true;
}

std::pair<WorkerId, ObjStoreId> SchedulerService::register_worker(const std::string& worker_address, const std::string& objstore_address, bool is_driver) {
  RAY_LOG(RAY_INFO, "registering worker " << worker_address << " connected to object store " << objstore_address);
  ObjStoreId objstoreid = std::numeric_limits<size_t>::max();
  // TODO: HACK: num_attempts is a hack
  for (int num_attempts = 0; num_attempts < 5; ++num_attempts) {
    auto objstores = objstores_.get();
    for (size_t i = 0; i < objstores->size(); ++i) {
      if ((*objstores)[i].address == objstore_address) {
        objstoreid = i;
      }
    }
    if (objstoreid == std::numeric_limits<size_t>::max()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  RAY_CHECK_NEQ(objstoreid, std::numeric_limits<size_t>::max(), "object store with address " << objstore_address << " not yet registered");
  WorkerId workerid;
  {
    auto workers = workers_.get();
    workerid = workers->size();
    workers->push_back(WorkerHandle());
    auto channel = grpc::CreateChannel(worker_address, grpc::InsecureChannelCredentials());
    (*workers)[workerid].channel = channel;
    (*workers)[workerid].objstoreid = objstoreid;
    (*workers)[workerid].worker_stub = WorkerService::NewStub(channel);
    (*workers)[workerid].worker_address = worker_address;
    if (is_driver) {
      (*workers)[workerid].current_task = ROOT_OPERATION; // We use this field to identify which workers are drivers.
    } else {
      (*workers)[workerid].current_task = NO_OPERATION;
    }
  }
  return std::make_pair(workerid, objstoreid);
}

ObjRef SchedulerService::register_new_object() {
  // If we don't simultaneously lock objtable_ and target_objrefs_, we will probably get errors.
  // TODO(rkn): increment/decrement_reference_count also acquire reference_counts_lock_ and target_objrefs_lock_ (through has_canonical_objref()), which caused deadlock in the past
  auto reference_counts = reference_counts_.get();
  auto contained_objrefs = contained_objrefs_.get();
  auto objtable = objtable_.get();
  auto target_objrefs = target_objrefs_.get();
  auto reverse_target_objrefs = reverse_target_objrefs_.get();
  ObjRef objtable_size = objtable->size();
  ObjRef target_objrefs_size = target_objrefs->size();
  ObjRef reverse_target_objrefs_size = reverse_target_objrefs->size();
  ObjRef reference_counts_size = reference_counts->size();
  ObjRef contained_objrefs_size = contained_objrefs->size();
  RAY_CHECK_EQ(objtable_size, target_objrefs_size, "objtable_ and target_objrefs_ should have the same size, but objtable_.size() = " << objtable_size << " and target_objrefs_.size() = " << target_objrefs_size);
  RAY_CHECK_EQ(objtable_size, reverse_target_objrefs_size, "objtable_ and reverse_target_objrefs_ should have the same size, but objtable_.size() = " << objtable_size << " and reverse_target_objrefs_.size() = " << reverse_target_objrefs_size);
  RAY_CHECK_EQ(objtable_size, reference_counts_size, "objtable_ and reference_counts_ should have the same size, but objtable_.size() = " << objtable_size << " and reference_counts_.size() = " << reference_counts_size);
  RAY_CHECK_EQ(objtable_size, contained_objrefs_size, "objtable_ and contained_objrefs_ should have the same size, but objtable_.size() = " << objtable_size << " and contained_objrefs_.size() = " << contained_objrefs_size);
  objtable->push_back(std::vector<ObjStoreId>());
  target_objrefs->push_back(UNITIALIZED_ALIAS);
  reverse_target_objrefs->push_back(std::vector<ObjRef>());
  reference_counts->push_back(0);
  contained_objrefs->push_back(std::vector<ObjRef>());
  {
    // We increment once so the objref doesn't go out of scope before the ObjReady
    // method is called. The corresponding decrement will happen either in
    // ObjReady in the scheduler or in AliasObjRefs in the scheduler.
    increment_ref_count(std::vector<ObjRef>({objtable_size}), reference_counts); // Note that reference_counts_lock_ is acquired above, as assumed by increment_ref_count
  }
  return objtable_size;
}

void SchedulerService::add_location(ObjRef canonical_objref, ObjStoreId objstoreid) {
  // add_location must be called with a canonical objref
  RAY_CHECK_NEQ((*reference_counts_.get())[canonical_objref], DEALLOCATED, "Calling ObjReady with canonical_objref " << canonical_objref << ", but this objref has already been deallocated");
  RAY_CHECK(is_canonical(canonical_objref), "Attempting to call add_location with a non-canonical objref (objref " << canonical_objref << ")");
  auto objtable = objtable_.get();
  RAY_CHECK_LT(canonical_objref, objtable->size(), "trying to put an object in the object store that was not registered with the scheduler (objref " << canonical_objref << ")");
  // do a binary search
  auto &locations = (*objtable)[canonical_objref];
  auto pos = std::lower_bound(locations.begin(), locations.end(), objstoreid);
  if (pos == locations.end() || objstoreid < *pos) {
    locations.insert(pos, objstoreid);
  }
  auto &objects_in_flight = objects_in_transit_[objstoreid];
  objects_in_flight.erase(std::remove(objects_in_flight.begin(), objects_in_flight.end(), canonical_objref), objects_in_flight.end());
}

void SchedulerService::add_canonical_objref(ObjRef objref) {
  auto target_objrefs = target_objrefs_.get();
  RAY_CHECK_LT(objref, target_objrefs->size(), "internal error: attempting to insert objref " << objref << " in target_objrefs_, but target_objrefs_.size() is " << target_objrefs->size());
  RAY_CHECK((*target_objrefs)[objref] == UNITIALIZED_ALIAS || (*target_objrefs)[objref] == objref, "internal error: attempting to declare objref " << objref << " as a canonical objref, but target_objrefs_[objref] is already aliased with objref " << (*target_objrefs)[objref]);
  (*target_objrefs)[objref] = objref;
}

ObjStoreId SchedulerService::get_store(WorkerId workerid) {
  auto workers = workers_.get();
  ObjStoreId result = (*workers)[workerid].objstoreid;
  return result;
}

void SchedulerService::register_function(const std::string& name, WorkerId workerid, size_t num_return_vals) {
  auto fntable = fntable_.get();
  FnInfo& info = (*fntable)[name];
  info.set_num_return_vals(num_return_vals);
  info.add_worker(workerid);
}

void SchedulerService::get_info(const SchedulerInfoRequest& request, SchedulerInfoReply* reply) {
  acquire_all_locks();
  auto reference_counts = reference_counts_.unsafe_get();
  for (int i = 0; i < reference_counts->size(); ++i) {
    reply->add_reference_count((*reference_counts)[i]);
  }
  auto target_objrefs = target_objrefs_.unsafe_get();
  for (int i = 0; i < target_objrefs->size(); ++i) {
    reply->add_target_objref((*target_objrefs)[i]);
  }
  auto function_table = reply->mutable_function_table();
  for (const auto& entry : *fntable_.unsafe_get()) {
    (*function_table)[entry.first].set_num_return_vals(entry.second.num_return_vals());
    for (const WorkerId& worker : entry.second.workers()) {
      (*function_table)[entry.first].add_workerid(worker);
    }
  }
  for (const auto& entry : *task_queue_.unsafe_get()) {
    reply->add_operationid(entry);
  }
  for (const WorkerId& entry : *avail_workers_.unsafe_get()) {
    reply->add_avail_worker(entry);
  }
  computation_graph_.unsafe_get()->to_protobuf(reply->mutable_computation_graph());
  release_all_locks();
}

// pick_objstore must be called with a canonical_objref
ObjStoreId SchedulerService::pick_objstore(ObjRef canonical_objref) {
  std::mt19937 rng;
  RAY_CHECK(is_canonical(canonical_objref), "Attempting to call pick_objstore with a non-canonical objref, (objref " << canonical_objref << ")");
  auto objtable = objtable_.get();
  std::uniform_int_distribution<int> uni(0, (*objtable)[canonical_objref].size() - 1);
  ObjStoreId objstoreid = (*objtable)[canonical_objref][uni(rng)];
  return objstoreid;
}

bool SchedulerService::is_canonical(ObjRef objref) {
  auto target_objrefs = target_objrefs_.get();
  RAY_CHECK_NEQ((*target_objrefs)[objref], UNITIALIZED_ALIAS, "Attempting to call is_canonical on an objref for which aliasing is not complete or the object is not ready, target_objrefs_[objref] == UNITIALIZED_ALIAS for objref " << objref << ".");
  return objref == (*target_objrefs)[objref];
}

void SchedulerService::perform_gets() {
  auto get_queue = get_queue_.get();
  // Complete all get tasks that can be completed.
  for (int i = 0; i < get_queue->size(); ++i) {
    const std::pair<WorkerId, ObjRef>& get = (*get_queue)[i];
    ObjRef objref = get.second;
    WorkerId workerid = get.first;
    ObjStoreId objstoreid = get_store(workerid);
    if (!has_canonical_objref(objref)) {
      RAY_LOG(RAY_ALIAS, "objref " << objref << " does not have a canonical_objref, so continuing");
      continue;
    }
    ObjRef canonical_objref = get_canonical_objref(objref);
    RAY_LOG(RAY_DEBUG, "attempting to get objref " << get.second << " with canonical objref " << canonical_objref << " to objstore " << objstoreid);
    int num_stores = (*objtable_.get())[canonical_objref].size();
    if (num_stores > 0) {
      deliver_object_async_if_necessary(canonical_objref, pick_objstore(canonical_objref), objstoreid);
      // Notify the relevant objstore about potential aliasing when it's ready
      alias_notification_queue_.get()->push_back(std::make_pair(objstoreid, std::make_pair(objref, canonical_objref)));
      // Remove the get task from the queue
      std::swap((*get_queue)[i], (*get_queue)[get_queue->size() - 1]);
      get_queue->pop_back();
      i -= 1;
    }
  }
}

void SchedulerService::schedule_tasks_naively() {
  auto computation_graph = computation_graph_.get();
  auto fntable = fntable_.get();
  auto avail_workers = avail_workers_.get();
  auto task_queue = task_queue_.get();
  for (int i = 0; i < avail_workers->size(); ++i) {
    // Submit all tasks whose arguments are ready.
    WorkerId workerid = (*avail_workers)[i];
    for (auto it = task_queue->begin(); it != task_queue->end(); ++it) {
      // The use of erase(it) below invalidates the iterator, but we
      // immediately break out of the inner loop, so the iterator is not used
      // after the erase
      const OperationId operationid = *it;
      const Task& task = computation_graph->get_task(operationid);
      auto& workers = (*fntable)[task.name()].workers();
      if (std::binary_search(workers.begin(), workers.end(), workerid) && can_run(task)) {
        assign_task(operationid, workerid, computation_graph);
        task_queue->erase(it);
        std::swap((*avail_workers)[i], (*avail_workers)[avail_workers->size() - 1]);
        avail_workers->pop_back();
        i -= 1;
        break;
      }
    }
  }
}

void SchedulerService::schedule_tasks_location_aware() {
  auto computation_graph = computation_graph_.get();
  auto fntable = fntable_.get();
  auto avail_workers = avail_workers_.get();
  auto task_queue = task_queue_.get();
  for (int i = 0; i < avail_workers->size(); ++i) {
    // Submit all tasks whose arguments are ready.
    WorkerId workerid = (*avail_workers)[i];
    ObjStoreId objstoreid = get_store(workerid);
    auto bestit = task_queue->end(); // keep track of the task that fits the worker best so far
    size_t min_num_shipped_objects = std::numeric_limits<size_t>::max(); // number of objects that need to be transfered for this worker
    for (auto it = task_queue->begin(); it != task_queue->end(); ++it) {
      OperationId operationid = *it;
      const Task& task = computation_graph->get_task(operationid);
      auto& workers = (*fntable)[task.name()].workers();
      if (std::binary_search(workers.begin(), workers.end(), workerid) && can_run(task)) {
        // determine how many objects would need to be shipped
        size_t num_shipped_objects = 0;
        for (int j = 0; j < task.arg_size(); ++j) {
          if (!task.arg(j).has_obj()) {
            ObjRef objref = task.arg(j).ref();
            RAY_CHECK(has_canonical_objref(objref), "no canonical object ref found even though task is ready; that should not be possible!");
            ObjRef canonical_objref = get_canonical_objref(objref);
            {
              // check if the object is already in the local object store
              auto objtable = objtable_.get();
              if (!std::binary_search((*objtable)[canonical_objref].begin(), (*objtable)[canonical_objref].end(), objstoreid)) {
                num_shipped_objects += 1;
              }
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
    if (bestit != task_queue->end()) {
      assign_task(*bestit, workerid, computation_graph);
      task_queue->erase(bestit);
      std::swap((*avail_workers)[i], (*avail_workers)[avail_workers->size() - 1]);
      avail_workers->pop_back();
      i -= 1;
    }
  }
}

void SchedulerService::perform_notify_aliases() {
  auto alias_notification_queue = alias_notification_queue_.get();
  for (int i = 0; i < alias_notification_queue->size(); ++i) {
    const std::pair<WorkerId, std::pair<ObjRef, ObjRef> > alias_notification = (*alias_notification_queue)[i];
    ObjStoreId objstoreid = alias_notification.first;
    ObjRef alias_objref = alias_notification.second.first;
    ObjRef canonical_objref = alias_notification.second.second;
    if (attempt_notify_alias(objstoreid, alias_objref, canonical_objref)) { // this locks both the objstore_ and objtable_
      // the attempt to notify the objstore of the objref aliasing succeeded, so remove the notification task from the queue
      std::swap((*alias_notification_queue)[i], (*alias_notification_queue)[alias_notification_queue->size() - 1]);
      alias_notification_queue->pop_back();
      i -= 1;
    }
  }
}

bool SchedulerService::has_canonical_objref(ObjRef objref) {
  auto target_objrefs = target_objrefs_.get();
  ObjRef objref_temp = objref;
  while (true) {
    RAY_CHECK_LT(objref_temp, target_objrefs->size(), "Attempting to index target_objrefs_ with objref " << objref_temp << ", but target_objrefs_.size() = " << target_objrefs->size());
    if ((*target_objrefs)[objref_temp] == UNITIALIZED_ALIAS) {
      return false;
    }
    if ((*target_objrefs)[objref_temp] == objref_temp) {
      return true;
    }
    objref_temp = (*target_objrefs)[objref_temp];
  }
}

ObjRef SchedulerService::get_canonical_objref(ObjRef objref) {
  // get_canonical_objref assumes that has_canonical_objref(objref) is true
  auto target_objrefs = target_objrefs_.get();
  ObjRef objref_temp = objref;
  while (true) {
    RAY_CHECK_LT(objref_temp, target_objrefs->size(), "Attempting to index target_objrefs_ with objref " << objref_temp << ", but target_objrefs_.size() = " << target_objrefs->size());
    RAY_CHECK_NEQ((*target_objrefs)[objref_temp], UNITIALIZED_ALIAS, "Attempting to get canonical objref for objref " << objref << ", which aliases, objref " << objref_temp << ", but target_objrefs_[objref_temp] == UNITIALIZED_ALIAS for objref_temp = " << objref_temp << ".");
    if ((*target_objrefs)[objref_temp] == objref_temp) {
      return objref_temp;
    }
    objref_temp = (*target_objrefs)[objref_temp];
    RAY_LOG(RAY_ALIAS, "Looping in get_canonical_objref.");
  }
}

bool SchedulerService::attempt_notify_alias(ObjStoreId objstoreid, ObjRef alias_objref, ObjRef canonical_objref) {
  // return true if successful and false otherwise
  if (alias_objref == canonical_objref) {
    // no need to do anything
    return true;
  }
  {
    auto objtable = objtable_.get();
    if (!std::binary_search((*objtable)[canonical_objref].begin(), (*objtable)[canonical_objref].end(), objstoreid)) {
      // the objstore doesn't have the object for canonical_objref yet, so it's too early to notify the objstore about the alias
      return false;
    }
  }
  ClientContext context;
  AckReply reply;
  NotifyAliasRequest request;
  request.set_alias_objref(alias_objref);
  request.set_canonical_objref(canonical_objref);
  (*objstores_.get())[objstoreid].objstore_stub->NotifyAlias(&context, request, &reply);
  return true;
}

void SchedulerService::deallocate_object(ObjRef canonical_objref, const SynchronizedPtr<std::vector<RefCount> > &reference_counts, const SynchronizedPtr<std::vector<std::vector<ObjRef> > > &contained_objrefs) {
  // deallocate_object should only be called from decrement_ref_count (note that
  // deallocate_object also recursively calls decrement_ref_count). Both of
  // these methods take reference_counts and contained_objrefs as argumens,
  // which are obtained by reference_counts_.get() and contained_objrefs_.get(),
  // so we know that those data structures have been locked
  RAY_LOG(RAY_REFCOUNT, "Deallocating canonical_objref " << canonical_objref << ".");
  {
    auto objtable = objtable_.get();
    auto &locations = (*objtable)[canonical_objref];
    auto objstores = objstores_.get(); // TODO(rkn): Should this be inside the for loop instead?
    for (int i = 0; i < locations.size(); ++i) {
      ClientContext context;
      AckReply reply;
      DeallocateObjectRequest request;
      request.set_canonical_objref(canonical_objref);
      ObjStoreId objstoreid = locations[i];
      RAY_LOG(RAY_REFCOUNT, "Attempting to deallocate canonical_objref " << canonical_objref << " from objstore " << objstoreid);
      (*objstores)[objstoreid].objstore_stub->DeallocateObject(&context, request, &reply);
    }
    locations.clear();
  }
  decrement_ref_count((*contained_objrefs)[canonical_objref], reference_counts, contained_objrefs);
}

void SchedulerService::increment_ref_count(const std::vector<ObjRef> &objrefs, const SynchronizedPtr<std::vector<RefCount> > &reference_counts) {
  // increment_ref_count takes reference_counts as an argument, which is
  // obtained by reference_counts_.get(), so we know that the data structure has
  // been locked
  for (int i = 0; i < objrefs.size(); ++i) {
    ObjRef objref = objrefs[i];
    RAY_CHECK_NEQ((*reference_counts)[objref], DEALLOCATED, "Attempting to increment the reference count for objref " << objref << ", but this object appears to have been deallocated already.");
    (*reference_counts)[objref] += 1;
    RAY_LOG(RAY_REFCOUNT, "Incremented ref count for objref " << objref <<". New reference count is " << (*reference_counts)[objref]);
  }
}

void SchedulerService::decrement_ref_count(const std::vector<ObjRef> &objrefs, const SynchronizedPtr<std::vector<RefCount> > &reference_counts, const SynchronizedPtr<std::vector<std::vector<ObjRef> > > &contained_objrefs) {
  // decrement_ref_count takes reference_counts and contained_objrefs as
  // arguments, which are obtained by reference_counts_.get() and
  // contained_objrefs_.get(), so we know that those data structures have been
  // locked
  for (int i = 0; i < objrefs.size(); ++i) {
    ObjRef objref = objrefs[i];
    RAY_CHECK_NEQ((*reference_counts)[objref], DEALLOCATED, "Attempting to decrement the reference count for objref " << objref << ", but this object appears to have been deallocated already.");
    RAY_CHECK_NEQ((*reference_counts)[objref], 0, "Attempting to decrement the reference count for objref " << objref << ", but the reference count for this object is already 0.");
    (*reference_counts)[objref] -= 1;
    RAY_LOG(RAY_REFCOUNT, "Decremented ref count for objref " << objref << ". New reference count is " << (*reference_counts)[objref]);
    // See if we can deallocate the object
    std::vector<ObjRef> equivalent_objrefs;
    get_equivalent_objrefs(objref, equivalent_objrefs);
    bool can_deallocate = true;
    for (int j = 0; j < equivalent_objrefs.size(); ++j) {
      if ((*reference_counts)[equivalent_objrefs[j]] != 0) {
        can_deallocate = false;
        break;
      }
    }
    if (can_deallocate) {
      ObjRef canonical_objref = equivalent_objrefs[0];
      RAY_CHECK(is_canonical(canonical_objref), "canonical_objref is not canonical.");
      deallocate_object(canonical_objref, reference_counts, contained_objrefs);
      for (int j = 0; j < equivalent_objrefs.size(); ++j) {
        (*reference_counts)[equivalent_objrefs[j]] = DEALLOCATED;
      }
    }
  }
}

void SchedulerService::upstream_objrefs(ObjRef objref, std::vector<ObjRef> &objrefs, const SynchronizedPtr<std::vector<std::vector<ObjRef> > > &reverse_target_objrefs) {
  // upstream_objrefs takes reverse_target_objrefs as an argument, which is
  // obtained by reverse_target_objrefs_.get(), so we know the data structure
  // has been locked.
  objrefs.push_back(objref);
  for (int i = 0; i < (*reverse_target_objrefs)[objref].size(); ++i) {
    upstream_objrefs((*reverse_target_objrefs)[objref][i], objrefs, reverse_target_objrefs);
  }
}

void SchedulerService::get_equivalent_objrefs(ObjRef objref, std::vector<ObjRef> &equivalent_objrefs) {
  auto target_objrefs = target_objrefs_.get();
  ObjRef downstream_objref = objref;
  while ((*target_objrefs)[downstream_objref] != downstream_objref && (*target_objrefs)[downstream_objref] != UNITIALIZED_ALIAS) {
    RAY_LOG(RAY_ALIAS, "Looping in get_equivalent_objrefs");
    downstream_objref = (*target_objrefs)[downstream_objref];
  }
  upstream_objrefs(downstream_objref, equivalent_objrefs, reverse_target_objrefs_.get());
}

// This method defines the order in which locks should be acquired.
void SchedulerService::do_on_locks(bool lock) {
  std::mutex *mutexes[] = {
    &successful_tasks_.mutex(),
    &failed_tasks_.mutex(),
    &get_queue_.mutex(),
    &computation_graph_.mutex(),
    &fntable_.mutex(),
    &avail_workers_.mutex(),
    &task_queue_.mutex(),
    &reference_counts_.mutex(),
    &contained_objrefs_.mutex(),
    &workers_.mutex(),
    &alias_notification_queue_.mutex(),
    &objtable_.mutex(),
    &objstores_.mutex(),
    &target_objrefs_.mutex(),
    &reverse_target_objrefs_.mutex()
  };
  size_t n = sizeof(mutexes) / sizeof(*mutexes);
  for (size_t i = 0; i != n; ++i) {
    if (lock) {
      mutexes[i]->lock();
    } else {
      mutexes[n - i - 1]->unlock();
    }
  }
}

void SchedulerService::acquire_all_locks() {
  do_on_locks(true);
}

void SchedulerService::release_all_locks() {
  do_on_locks(false);
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

RayConfig global_ray_config;

int main(int argc, char** argv) {
  SchedulingAlgorithmType scheduling_algorithm = SCHEDULING_ALGORITHM_LOCALITY_AWARE;
  RAY_CHECK_GE(argc, 2, "scheduler: expected at least one argument (scheduler ip address)");
  if (argc > 2) {
    const char* log_file_name = get_cmd_option(argv, argv + argc, "--log-file-name");
    if (log_file_name) {
      std::cout << "scheduler: writing to log file " << log_file_name << std::endl;
      create_log_dir_or_die(log_file_name);
      global_ray_config.log_to_file = true;
      global_ray_config.logfile.open(log_file_name);
    } else {
      std::cout << "scheduler: writing logs to stdout; you can change this by passing --log-file-name <filename> to ./scheduler" << std::endl;
      global_ray_config.log_to_file = false;
    }
    const char* scheduling_algorithm_name = get_cmd_option(argv, argv + argc, "--scheduler-algorithm");
    if (scheduling_algorithm_name) {
      if (std::string(scheduling_algorithm_name) == "naive") {
        RAY_LOG(RAY_INFO, "scheduler: using 'naive' scheduler" << std::endl);
        scheduling_algorithm = SCHEDULING_ALGORITHM_NAIVE;
      }
      if (std::string(scheduling_algorithm_name) == "locality_aware") {
        RAY_LOG(RAY_INFO, "scheduler: using 'locality aware' scheduler" << std::endl);
        scheduling_algorithm = SCHEDULING_ALGORITHM_LOCALITY_AWARE;
      }
    }
  }
  start_scheduler_service(argv[1], scheduling_algorithm);
  return 0;
}
