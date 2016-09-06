#include "scheduler.h"

#include <random>
#include <thread>
#include <chrono>
#include <sstream>

#include "utils.h"

// Macro used for acquiring locks. Required to pass along the field name and the line number without duplicating code.
#define GET(FieldName) get(FieldName, #FieldName, __LINE__)

#ifndef NDEBUG
template<>
class SchedulerService::MySynchronizedPtr<void> {
  SchedulerService* me_;  // If NULL, then no lock is being checked
  size_t order_delta_;
  const char* name_;
  unsigned int line_number_;
  // ID returned seems to always be zero on Mac.
  // I unfortunately can't find a workaround, so if the returned ID is zero, then the caller should not rely on it identifying the thread.
  static unsigned long long get_thread_id() {
    unsigned long long id = 0;
    std::stringstream ss;
    ss << std::this_thread::get_id();
    ss >> id;
    return id;
  }
protected:
  MySynchronizedPtr& operator=(MySynchronizedPtr&& other) {
    if (this != &other) {
      me_ = std::move(other.me_);
      order_delta_ = std::move(other.order_delta_);
      name_ = std::move(other.name_);
      line_number_ = std::move(other.line_number_);

      other.me_ = NULL;  // Disable lock checking logic on other now that it has been moved
    }
    return *this;
  }
  ~MySynchronizedPtr() {
    unsigned long long thread_id = get_thread_id();
    if (thread_id != 0 && me_ != NULL) {
      auto lock_orders = me_->lock_orders_.unchecked_get();
      // Look for a previous lock on this thread -- it must exist, since this thread supposedly had the lock...
      auto found = lock_orders->begin();
      while (found != lock_orders->end() && found->first != thread_id) {
        ++found;
      }
      RAY_CHECK(found != lock_orders->end() && found->second.first >= order_delta_, "Thread " << thread_id << " attempted to unlock a lock it didn't hold on line " << line_number_);
      // Subtract back the delta
      found->second.first -= order_delta_;
      found->second.second = name_;
      // If it goes to zero, then this thread no longer has locks, so remove it from the list
      if (found->second.first == 0) {
        using std::swap; swap(*found, lock_orders->back());
        lock_orders->pop_back();
      }
      me_ = NULL;
    }
  }
  MySynchronizedPtr(MySynchronizedPtr&& other) : me_() {
    *this = std::move(other);
  }
  MySynchronizedPtr(SchedulerService* me, size_t order, const char* name, unsigned int line_number) : me_(me), order_delta_(order), name_(name), line_number_(line_number) {
    unsigned long long thread_id = get_thread_id();
    if (thread_id != 0 && me_ != NULL) {
      auto lock_orders = me_->lock_orders_.unchecked_get();
      auto found = lock_orders->begin();
      // Look for a previous lock on this thread -- it shouldn't exist since these are not recursive locks
      while (found != lock_orders->end() && found->first != thread_id) {
        ++found;
      }
      if (found == lock_orders->end()) {
        found = lock_orders->insert(found, std::make_pair(thread_id, std::make_pair(0, name_)));
      } else if (thread_id != 0) {
        RAY_CHECK_GE(order, found->second.first, "Thread " << thread_id << " attempted to lock " << name_ << " on line " << line_number_ << " after " << found->second.second);
      }
      // Store the delta between the last lock and this lock (each identified by the field offset) so we can reverse it
      order_delta_ = order - found->second.first;
      // Record the fact that we locked this field in the scheduler
      found->second.first = order;
      found->second.second = name_;
    }
  }
};
template<class T>
class SchedulerService::MySynchronizedPtr : SynchronizedPtr<T>, MySynchronizedPtr<void> {
  // TODO(mniknami): release(), etc. are private here -- implementing them is extra work we don't need yet
public:
  using SynchronizedPtr<T>::operator*;
  using SynchronizedPtr<T>::operator->;
  MySynchronizedPtr(SchedulerService& me, Synchronized<T>& value, const char* name, unsigned int line_number) :
    SynchronizedPtr<T>(value.unchecked_get()),
    MySynchronizedPtr<void>(&me, static_cast<size_t>(reinterpret_cast<unsigned char const *>(&value) - reinterpret_cast<unsigned char const *>(&me)), name, line_number) {
  }
  MySynchronizedPtr(MySynchronizedPtr&& other) = default;
  MySynchronizedPtr& operator=(MySynchronizedPtr&& other) = default;
};

template<class T>
SchedulerService::MySynchronizedPtr<T> SchedulerService::get(Synchronized<T>& my_field, const char* name, unsigned int line_number) { return MySynchronizedPtr<T>(*this, my_field, name, line_number); }

template<class T>
SchedulerService::MySynchronizedPtr<const T> SchedulerService::get(const Synchronized<T>& my_field, const char* name, unsigned int line_number) const { return MySynchronizedPtr<const T>(*this, my_field, name, line_number); }
#else
template<class T>
SchedulerService::MySynchronizedPtr<T> SchedulerService::get(Synchronized<T>& my_field, const char* name, unsigned int line_number) { (void) name; (void) line_number; return my_field.unchecked_get(); }

template<class T>
SchedulerService::MySynchronizedPtr<const T> SchedulerService::get(const Synchronized<T>& my_field, const char* name, unsigned int line_number) const { (void) name; (void) line_number; return my_field.unchecked_get(); }
#endif

SchedulerService::SchedulerService(SchedulingAlgorithmType scheduling_algorithm) : scheduling_algorithm_(scheduling_algorithm) {}

Status SchedulerService::SubmitTask(ServerContext* context, const SubmitTaskRequest* request, SubmitTaskReply* reply) {
  std::unique_ptr<Task> task(new Task(request->task())); // need to copy, because request is const
  size_t num_return_vals;
  {
    auto fntable = GET(fntable_);
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
    std::vector<ObjectID> result_objectids;
    for (size_t i = 0; i < num_return_vals; ++i) {
      ObjectID result = register_new_object();
      reply->add_result(result);
      task->add_result(result);
      result_objectids.push_back(result);
    }
    {
      auto reference_counts = GET(reference_counts_);
      increment_ref_count(result_objectids, reference_counts); // We increment once so the objectids don't go out of scope before we reply to the worker that called SubmitTask. The corresponding decrement will happen in submit_task in raylib.
      increment_ref_count(result_objectids, reference_counts); // We increment once so the objectids don't go out of scope before the task is scheduled on the worker. The corresponding decrement will happen in deserialize_task in raylib.
    }

    auto operation = std::unique_ptr<Operation>(new Operation());
    operation->set_allocated_task(task.release());
    operation->set_creator_operationid((*GET(workers_))[request->workerid()].current_task);

    OperationId operationid = GET(computation_graph_)->add_operation(std::move(operation));
    GET(task_queue_)->push_back(operationid);
    schedule();
  }
  return Status::OK;
}

Status SchedulerService::PutObj(ServerContext* context, const PutObjRequest* request, PutObjReply* reply) {
  ObjectID objectid = register_new_object();
  auto operation = std::unique_ptr<Operation>(new Operation());
  operation->mutable_put()->set_objectid(objectid);
  operation->set_creator_operationid((*GET(workers_))[request->workerid()].current_task);
  GET(computation_graph_)->add_operation(std::move(operation));
  reply->set_objectid(objectid);
  schedule();
  return Status::OK;
}

Status SchedulerService::RequestObj(ServerContext* context, const RequestObjRequest* request, AckReply* reply) {
  size_t size = GET(objtable_)->size();
  ObjectID objectid = request->objectid();
  RAY_CHECK_LT(objectid, size, "internal error: no object with objectid " << objectid << " exists");
  auto operation = std::unique_ptr<Operation>(new Operation());
  operation->mutable_get()->set_objectid(objectid);
  operation->set_creator_operationid((*GET(workers_))[request->workerid()].current_task);
  GET(computation_graph_)->add_operation(std::move(operation));
  GET(get_queue_)->push_back(std::make_pair(request->workerid(), objectid));
  schedule();
  return Status::OK;
}

Status SchedulerService::AliasObjectIDs(ServerContext* context, const AliasObjectIDsRequest* request, AckReply* reply) {
  ObjectID alias_objectid = request->alias_objectid();
  ObjectID target_objectid = request->target_objectid();
  RAY_LOG(RAY_ALIAS, "Aliasing objectid " << alias_objectid << " with objectid " << target_objectid);
  RAY_CHECK_NEQ(alias_objectid, target_objectid, "internal error: attempting to alias objectid " << alias_objectid << " with itself.");
  size_t size = GET(objtable_)->size();
  RAY_CHECK_LT(alias_objectid, size, "internal error: no object with objectid " << alias_objectid << " exists");
  RAY_CHECK_LT(target_objectid, size, "internal error: no object with objectid " << target_objectid << " exists");
  {
    auto target_objectids = GET(target_objectids_);
    RAY_CHECK_EQ((*target_objectids)[alias_objectid], UNITIALIZED_ALIAS, "internal error: attempting to alias objectid " << alias_objectid << " with objectid " << target_objectid << ", but objectid " << alias_objectid << " has already been aliased with objectid " << (*target_objectids)[alias_objectid]);
    (*target_objectids)[alias_objectid] = target_objectid;
  }
  (*GET(reverse_target_objectids_))[target_objectid].push_back(alias_objectid);
  {
    // The corresponding increment was done in register_new_object.
    auto reference_counts = GET(reference_counts_); // we grab this lock because decrement_ref_count assumes it has been acquired
    auto contained_objectids = GET(contained_objectids_); // we grab this lock because decrement_ref_count assumes it has been acquired
    decrement_ref_count(std::vector<ObjectID>({alias_objectid}), reference_counts, contained_objectids);
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) {
  auto objtable = GET(objtable_); // to protect objects_in_transit_
  auto objstores = GET(objstores_);
  ObjStoreId objstoreid = objstores->size();
  auto channel = grpc::CreateChannel(request->objstore_address(), grpc::InsecureChannelCredentials());
  objstores->push_back(ObjStoreHandle());
  (*objstores)[objstoreid].address = request->objstore_address();
  (*objstores)[objstoreid].channel = channel;
  (*objstores)[objstoreid].objstore_stub = ObjStore::NewStub(channel);
  reply->set_objstoreid(objstoreid);
  objects_in_transit_.push_back(std::vector<ObjectID>());
  return Status::OK;
}

Status SchedulerService::RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) {
  std::string worker_address = request->worker_address();
  std::string objstore_address = request->objstore_address();
  std::string node_ip_address = request->node_ip_address();
  bool is_driver = request->is_driver();
  RAY_LOG(RAY_INFO, "Registering a worker from node with IP address " << node_ip_address);
  // Find the object store to connect to. We use the max size to indicate that
  // the object store for this worker has not been found.
  ObjStoreId objstoreid = std::numeric_limits<size_t>::max();
  // TODO: HACK: num_attempts is a hack
  for (int num_attempts = 0; num_attempts < 30; ++num_attempts) {
    auto objstores = GET(objstores_);
    for (size_t i = 0; i < objstores->size(); ++i) {
      if (objstore_address != "" && (*objstores)[i].address == objstore_address) {
        // This object store address is the same as the provided object store
        // address.
        objstoreid = i;
      }
      if ((*objstores)[i].address.compare(0, node_ip_address.size(), node_ip_address) == 0) {
        // The object store address was not provided and this object store
        // address has node_ip_address as a prefix, so it is on the same machine
        // as the worker that is registering.
        objstoreid = i;
        objstore_address = (*objstores)[i].address;
      }
    }
    if (objstoreid == std::numeric_limits<size_t>::max()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } else {
      break;
    }
  }
  if (objstore_address.empty()) {
    RAY_CHECK_NEQ(objstoreid, std::numeric_limits<size_t>::max(), "No object store with IP address " << node_ip_address << " has registered.");
  } else {
    RAY_CHECK_NEQ(objstoreid, std::numeric_limits<size_t>::max(), "Object store with address " << objstore_address << " not yet registered.");
  }
  // Populate the worker information.
  WorkerId workerid;
  {
    auto workers = GET(workers_);
    workerid = workers->size();
    workers->push_back(WorkerHandle());
    auto channel = grpc::CreateChannel(worker_address, grpc::InsecureChannelCredentials());
    (*workers)[workerid].channel = channel;
    (*workers)[workerid].objstoreid = objstoreid;
    (*workers)[workerid].worker_stub = WorkerService::NewStub(channel);
    (*workers)[workerid].worker_address = worker_address;
    (*workers)[workerid].initialized = false;
    if (is_driver) {
      (*workers)[workerid].current_task = ROOT_OPERATION; // We use this field to identify which workers are drivers.
    } else {
      (*workers)[workerid].current_task = NO_OPERATION;
    }
  }
  RAY_LOG(RAY_INFO, "Finished registering worker with workerid " << workerid << ", worker address " << worker_address << " on node with IP address " << node_ip_address << ", is_driver = " << is_driver << ", assigned to object store with id " << objstoreid << " and address " << objstore_address);
  reply->set_workerid(workerid);
  reply->set_objstoreid(objstoreid);
  reply->set_objstore_address(objstore_address);
  schedule();
  return Status::OK;
}

Status SchedulerService::RegisterRemoteFunction(ServerContext* context, const RegisterRemoteFunctionRequest* request, AckReply* reply) {
  RAY_LOG(RAY_INFO, "register function " << request->function_name() <<  " from workerid " << request->workerid());
  register_function(request->function_name(), request->workerid(), request->num_return_vals());
  schedule();
  return Status::OK;
}

Status SchedulerService::NotifyFailure(ServerContext* context, const NotifyFailureRequest* request, AckReply* reply) {
  const Failure failure = request->failure();
  WorkerId workerid = failure.workerid();
  if (failure.type() == FailedType::FailedTask) {
    // A task threw an exception while executing.
    TaskStatus failed_task_info;
    {
      auto workers = GET(workers_);
      failed_task_info.set_operationid((*workers)[workerid].current_task);
      failed_task_info.set_function_name(failure.name());
      failed_task_info.set_worker_address((*workers)[workerid].worker_address);
      failed_task_info.set_error_message(failure.error_message());
    }
    GET(failed_tasks_)->push_back(failed_task_info);
    RAY_LOG(RAY_INFO, "Error: Task " << failed_task_info.operationid() << " executing function " << failed_task_info.function_name() << " on worker " << workerid << " failed with error message:\n" << failed_task_info.error_message());
  } else if (failure.type() == FailedType::FailedRemoteFunctionImport) {
    // An exception was thrown while a remote function was being imported.
    GET(failed_remote_function_imports_)->push_back(failure);
    RAY_LOG(RAY_INFO, "Error: Worker " << workerid << " failed to import remote function " << failure.name() << ", failed with error message:\n" << failure.error_message());
  } else if (failure.type() == FailedType::FailedReusableVariableImport) {
    // An exception was thrown while a reusable variable was being imported.
    GET(failed_reusable_variable_imports_)->push_back(failure);
    RAY_LOG(RAY_INFO, "Error: Worker " << workerid << " failed to import reusable variable " << failure.name() << ", failed with error message:\n" << failure.error_message());
  } else if (failure.type() == FailedType::FailedReinitializeReusableVariable) {
    // An exception was thrown while a reusable variable was being imported.
    GET(failed_reinitialize_reusable_variables_)->push_back(failure);
    RAY_LOG(RAY_INFO, "Error: Worker " << workerid << " failed to reinitialize a reusable variable after running remote function " << failure.name() << ", failed with error message:\n" << failure.error_message());
  } else if (failure.type() == FailedType::FailedFunctionToRun) {
    // An exception was thrown while a function was being run on all workers.
    GET(failed_function_to_runs_)->push_back(failure);
    RAY_LOG(RAY_INFO, "Error: Worker " << workerid << " failed to run function " << failure.name() << " on all workers, failed with error message:\n" << failure.error_message());
  } else {
    RAY_CHECK(false, "This code should be unreachable.")
  }
  // Print the failure on the relevant driver. TODO(rkn): At the moment, this
  // prints the failure on all of the drivers. It should probably only print it
  // on the driver that caused the problem.
  auto workers = GET(workers_);
  for (size_t i = 0; i < workers->size(); ++i) {
    WorkerHandle* worker = &(*workers)[i];
    // Check if the worker is still connected.
    if (worker->worker_stub) {
      // Check if this is a driver.
      if (worker->current_task == ROOT_OPERATION) {
        ClientContext client_context;
        PrintErrorMessageRequest print_request;
        print_request.mutable_failure()->CopyFrom(request->failure());
        AckReply print_reply;
        // RAY_CHECK_GRPC(worker->worker_stub->PrintErrorMessage(&client_context, print_request, &print_reply));
      }
    }
  }
  return Status::OK;
}

Status SchedulerService::ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) {
  ObjectID objectid = request->objectid();
  RAY_LOG(RAY_DEBUG, "object " << objectid << " ready on store " << request->objstoreid());
  add_canonical_objectid(objectid);
  add_location(objectid, request->objstoreid());
  {
    // If this is the first time that ObjReady has been called for this objectid,
    // the corresponding increment was done in register_new_object in the
    // scheduler. For all subsequent calls to ObjReady, the corresponding
    // increment was done in deliver_object_if_necessary in the scheduler.
    auto reference_counts = GET(reference_counts_); // we grab this lock because decrement_ref_count assumes it has been acquired
    auto contained_objectids = GET(contained_objectids_); // we grab this lock because decrement_ref_count assumes it has been acquired
    decrement_ref_count(std::vector<ObjectID>({objectid}), reference_counts, contained_objectids);
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::ReadyForNewTask(ServerContext* context, const ReadyForNewTaskRequest* request, AckReply* reply) {
  WorkerId workerid = request->workerid();
  {
    auto workers = GET(workers_);
    OperationId operationid = (*workers)[workerid].current_task;
    RAY_LOG(RAY_INFO, "worker " << workerid << " is ready for a new task");
    RAY_CHECK(operationid != ROOT_OPERATION, "A driver appears to have called ReadyForNewTask.");
    {
      // Check if the worker has been initialized yet, and if not, then give it
      // all of the exported functions and all of the exported reusable variables.
      if (!(*workers)[workerid].initialized) {
        // This should only happen once.
        // Queue up all functions to run on the worker.
        add_all_functions_to_run_to_worker_queue(workerid);
        // Queue up all remote functions to be imported on the worker.
        add_all_remote_functions_to_worker_export_queue(workerid);
        // Queue up all reusable variables to be imported on the worker.
        add_all_reusable_variables_to_worker_export_queue(workerid);
        // Mark the worker as initialized.
        (*workers)[workerid].initialized = true;
      }
    }
    (*workers)[workerid].current_task = NO_OPERATION; // clear operation ID
  }
  GET(avail_workers_)->push_back(workerid);
  schedule();
  return Status::OK;
}

Status SchedulerService::IncrementRefCount(ServerContext* context, const IncrementRefCountRequest* request, AckReply* reply) {
  int num_objectids = request->objectid_size();
  RAY_CHECK_NEQ(num_objectids, 0, "Scheduler received IncrementRefCountRequest with 0 objectids.");
  std::vector<ObjectID> objectids;
  for (int i = 0; i < num_objectids; ++i) {
    objectids.push_back(request->objectid(i));
  }
  auto reference_counts = GET(reference_counts_);
  increment_ref_count(objectids, reference_counts);
  return Status::OK;
}

Status SchedulerService::DecrementRefCount(ServerContext* context, const DecrementRefCountRequest* request, AckReply* reply) {
  int num_objectids = request->objectid_size();
  RAY_CHECK_NEQ(num_objectids, 0, "Scheduler received DecrementRefCountRequest with 0 objectids.");
  std::vector<ObjectID> objectids;
  for (int i = 0; i < num_objectids; ++i) {
    objectids.push_back(request->objectid(i));
  }
  auto reference_counts = GET(reference_counts_); // we grab this lock, because decrement_ref_count assumes it has been acquired
  auto contained_objectids = GET(contained_objectids_); // we grab this lock because decrement_ref_count assumes it has been acquired
  decrement_ref_count(objectids, reference_counts, contained_objectids);
  return Status::OK;
}

Status SchedulerService::AddContainedObjectIDs(ServerContext* context, const AddContainedObjectIDsRequest* request, AckReply* reply) {
  ObjectID objectid = request->objectid();
  // if (!is_canonical(objectid)) {
    // TODO(rkn): Perhaps we don't need this check. It won't work because the objstore may not have called ObjReady yet.
    // RAY_LOG(RAY_FATAL, "Attempting to add contained objectids for non-canonical objectid " << objectid);
  // }
  auto contained_objectids = GET(contained_objectids_);
  RAY_CHECK_EQ((*contained_objectids)[objectid].size(), 0, "Attempting to add contained objectids for objectid " << objectid << ", but contained_objectids_[objectid].size() != 0.");
  for (int i = 0; i < request->contained_objectid_size(); ++i) {
    (*contained_objectids)[objectid].push_back(request->contained_objectid(i));
  }
  return Status::OK;
}

Status SchedulerService::SchedulerInfo(ServerContext* context, const SchedulerInfoRequest* request, SchedulerInfoReply* reply) {
  get_info(*request, reply);
  return Status::OK;
}

Status SchedulerService::TaskInfo(ServerContext* context, const TaskInfoRequest* request, TaskInfoReply* reply) {
  auto failed_tasks = GET(failed_tasks_);
  auto failed_remote_function_imports = GET(failed_remote_function_imports_);
  auto failed_reusable_variable_imports = GET(failed_reusable_variable_imports_);
  auto failed_reinitialize_reusable_variables = GET(failed_reinitialize_reusable_variables_);
  auto failed_function_to_runs = GET(failed_function_to_runs_);
  auto computation_graph = GET(computation_graph_);
  auto workers = GET(workers_);
  // Return information about the failed tasks.
  for (int i = 0; i < failed_tasks->size(); ++i) {
    TaskStatus* info = reply->add_failed_task();
    *info = (*failed_tasks)[i];
  }
  // Return information about currently running tasks.
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
  // Return information about failed remote function imports.
  for (size_t i = 0; i < failed_remote_function_imports->size(); ++i) {
    Failure* failure = reply->add_failed_remote_function_import();
    *failure = (*failed_remote_function_imports)[i];
  }
  // Return information about failed reusable variable imports.
  for (size_t i = 0; i < failed_reusable_variable_imports->size(); ++i) {
    Failure* failure = reply->add_failed_reusable_variable_import();
    *failure = (*failed_reusable_variable_imports)[i];
  }
  // Return information about failed reusable variable reinitializations.
  for (size_t i = 0; i < failed_reinitialize_reusable_variables->size(); ++i) {
    Failure* failure = reply->add_failed_reinitialize_reusable_variable();
    *failure = (*failed_reinitialize_reusable_variables)[i];
  }
  // Return information about functions that failed to run on all workers.
  for (size_t i = 0; i < failed_function_to_runs->size(); ++i) {
    Failure* failure = reply->add_failed_function_to_run();
    *failure = (*failed_function_to_runs)[i];
  }
  return Status::OK;
}

Status SchedulerService::KillWorkers(ServerContext* context, const KillWorkersRequest* request, KillWorkersReply* reply) {
  // TODO: Update reference counts
  auto failed_tasks = GET(failed_tasks_);
  auto get_queue = GET(get_queue_);
  auto computation_graph = GET(computation_graph_);
  auto fntable = GET(fntable_);
  auto avail_workers = GET(avail_workers_);
  auto task_queue = GET(task_queue_);
  auto workers = GET(workers_);
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
      AckReply die_reply;
      // TODO: Fault handling... what if a worker refuses to die? We just assume it dies here.
      RAY_CHECK_GRPC(idle_worker->worker_stub->Die(&client_context, die_request, &die_reply));
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

Status SchedulerService::RunFunctionOnAllWorkers(ServerContext* context, const RunFunctionOnAllWorkersRequest* request, AckReply* reply) {
  {
    auto workers = GET(workers_);
    auto function_to_run_queue = GET(function_to_run_queue_);
    auto exported_functions_to_run = GET(exported_functions_to_run_);
    exported_functions_to_run->push_back(std::unique_ptr<Function>(new Function(request->function())));
    for (WorkerId workerid = 0; workerid < workers->size(); ++workerid) {
      if ((*workers)[workerid].current_task != ROOT_OPERATION) {
        function_to_run_queue->push(std::make_pair(workerid, exported_functions_to_run->size() - 1));
      }
    }
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::ExportRemoteFunction(ServerContext* context, const ExportRemoteFunctionRequest* request, AckReply* reply) {
  {
    auto workers = GET(workers_);
    auto remote_function_export_queue = GET(remote_function_export_queue_);
    auto exported_functions = GET(exported_functions_);
    // TODO(rkn): Does this do a deep copy?
    exported_functions->push_back(std::unique_ptr<Function>(new Function(request->function())));
    for (WorkerId workerid = 0; workerid < workers->size(); ++workerid) {
      if ((*workers)[workerid].current_task != ROOT_OPERATION) {
        // Add this workerid and remote function pair to the export queue.
        remote_function_export_queue->push(std::make_pair(workerid, exported_functions->size() - 1));
      }
    }
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::ExportReusableVariable(ServerContext* context, const ExportReusableVariableRequest* request, AckReply* reply) {
  {
    auto workers = GET(workers_);
    auto reusable_variable_export_queue = GET(reusable_variable_export_queue_);
    auto exported_reusable_variables = GET(exported_reusable_variables_);
    // TODO(rkn): Does this do a deep copy?
    exported_reusable_variables->push_back(std::unique_ptr<ReusableVar>(new ReusableVar(request->reusable_variable())));
    for (WorkerId workerid = 0; workerid < workers->size(); ++workerid) {
      if ((*workers)[workerid].current_task != ROOT_OPERATION) {
        // Add this workerid and reusable variable pair to the export queue.
        reusable_variable_export_queue->push(std::make_pair(workerid, exported_reusable_variables->size() - 1));
      }
    }
  }
  schedule();
  return Status::OK;
}

Status SchedulerService::Select(ServerContext* context, const SelectRequest* request, SelectReply* reply) {
  auto objtable = GET(objtable_);
  for (int i = 0; i < request->objectids_size(); ++i) {
    ObjectID objectid = request->objectids(i);
    if (has_canonical_objectid(objectid)) {
      ObjectID canonical_objectid = get_canonical_objectid(objectid);
      RAY_CHECK_LT(canonical_objectid, objtable->size(), "Canonical_objectid is outside object table.");
      if ((*objtable)[canonical_objectid].size() != 0) {
        reply->add_indices(i);
      }
    }
  }
  return Status::OK;
}

void SchedulerService::deliver_object_async_if_necessary(ObjectID canonical_objectid, ObjStoreId from, ObjStoreId to) {
  bool object_present_or_in_transit;
  {
    auto objtable = GET(objtable_);
    auto &locations = (*objtable)[canonical_objectid];
    bool object_present = std::binary_search(locations.begin(), locations.end(), to);
    auto &objects_in_flight = objects_in_transit_[to];
    bool object_in_transit = (std::find(objects_in_flight.begin(), objects_in_flight.end(), canonical_objectid) != objects_in_flight.end());
    object_present_or_in_transit = object_present || object_in_transit;
    if (!object_present_or_in_transit) {
      objects_in_flight.push_back(canonical_objectid);
    }
  }
  if (!object_present_or_in_transit) {
    deliver_object_async(canonical_objectid, from, to);
  }
}

// TODO(rkn): This could execute multiple times with the same arguments before
// the delivery finishes, but we only want it to happen once. Currently, the
// redundancy is handled by the object store, which will only execute the
// delivery once. However, we may want to handle it in the scheduler in the
// future.
//
// deliver_object_async assumes that the aliasing for objectid has already been completed. That is, has_canonical_objectid(objectid) == true
void SchedulerService::deliver_object_async(ObjectID canonical_objectid, ObjStoreId from, ObjStoreId to) {
  RAY_CHECK_NEQ(from, to, "attempting to deliver canonical_objectid " << canonical_objectid << " from objstore " << from << " to itself.");
  RAY_CHECK(is_canonical(canonical_objectid), "attempting to deliver objectid " << canonical_objectid << ", but this objectid is not a canonical objectid.");
  {
    // We increment once so the objectid doesn't go out of scope before the ObjReady
    // method is called. The corresponding decrement will happen in ObjReady in
    // the scheduler.
    auto reference_counts = GET(reference_counts_); // we grab this lock because increment_ref_count assumes it has been acquired
    increment_ref_count(std::vector<ObjectID>({canonical_objectid}), reference_counts);
  }
  ClientContext context;
  AckReply reply;
  StartDeliveryRequest request;
  request.set_objectid(canonical_objectid);
  auto objstores = GET(objstores_);
  request.set_objstore_address((*objstores)[from].address);
  RAY_CHECK_GRPC((*objstores)[to].objstore_stub->StartDelivery(&context, request, &reply));
}

void SchedulerService::schedule() {
  // Run functions on workers. This must happen before we schedule tasks in
  // order to guarantee that remote function calls use the most up to date
  // environment.
  perform_functions_to_run();
  // Export remote functions to the workers. This must happen before we schedule
  // tasks in order to guarantee that remote function calls use the most up to
  // date definitions.
  perform_remote_function_exports();
  // Export reusable variables to the workers. This must happen before we
  // schedule tasks in order to guarantee that the workers have the definitions
  // they need.
  perform_reusable_variable_exports();
  // See what we can do in get_queue_
  perform_gets();
  if (scheduling_algorithm_ == SCHEDULING_ALGORITHM_NAIVE) {
    schedule_tasks_naively(); // See what we can do in task_queue_
  } else if (scheduling_algorithm_ == SCHEDULING_ALGORITHM_LOCALITY_AWARE) {
    schedule_tasks_location_aware(); // See what we can do in task_queue_
  } else {
    RAY_CHECK(false, "scheduling algorithm not known");
  }
  perform_notify_aliases(); // See what we can do in alias_notification_queue_
}

// assign_task assumes that the canonical objectids for its arguments are all ready, that is has_canonical_objectid() is true for all of the call's arguments
void SchedulerService::assign_task(OperationId operationid, WorkerId workerid, const MySynchronizedPtr<ComputationGraph> &computation_graph) {
  // assign_task takes computation_graph as an argument, which is obtained by
  // GET(computation_graph_), so we know that the data structure has been
  // locked.
  ObjStoreId objstoreid = get_store(workerid);
  const Task& task = computation_graph->get_task(operationid);
  ClientContext context;
  ExecuteTaskRequest request;
  AckReply reply;
  RAY_LOG(RAY_INFO, "starting to send arguments");
  for (size_t i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjectID objectid = task.arg(i).id();
      ObjectID canonical_objectid = get_canonical_objectid(objectid);
      // Notify the relevant objstore about potential aliasing when it's ready
      GET(alias_notification_queue_)->push_back(std::make_pair(objstoreid, std::make_pair(objectid, canonical_objectid)));
      attempt_notify_alias(objstoreid, objectid, canonical_objectid);
      RAY_LOG(RAY_DEBUG, "task contains object ref " << canonical_objectid);
      deliver_object_async_if_necessary(canonical_objectid, pick_objstore(canonical_objectid), objstoreid);
    }
  }
  {
    auto workers = GET(workers_);
    (*workers)[workerid].current_task = operationid;
    request.mutable_task()->CopyFrom(task); // TODO(rkn): Is ownership handled properly here?
    RAY_CHECK_GRPC((*workers)[workerid].worker_stub->ExecuteTask(&context, request, &reply));
  }
}

bool SchedulerService::can_run(const Task& task) {
  auto objtable = GET(objtable_);
  for (int i = 0; i < task.arg_size(); ++i) {
    if (!task.arg(i).has_obj()) {
      ObjectID objectid = task.arg(i).id();
      if (!has_canonical_objectid(objectid)) {
        return false;
      }
      ObjectID canonical_objectid = get_canonical_objectid(objectid);
      if (canonical_objectid >= objtable->size() || (*objtable)[canonical_objectid].size() == 0) {
        return false;
      }
    }
  }
  return true;
}

ObjectID SchedulerService::register_new_object() {
  // If we don't simultaneously lock objtable_ and target_objectids_, we will probably get errors.
  // TODO(rkn): increment/decrement_reference_count also acquire reference_counts_lock_ and target_objectids_lock_ (through has_canonical_objectid()), which caused deadlock in the past
  auto reference_counts = GET(reference_counts_);
  auto contained_objectids = GET(contained_objectids_);
  auto objtable = GET(objtable_);
  auto target_objectids = GET(target_objectids_);
  auto reverse_target_objectids = GET(reverse_target_objectids_);
  ObjectID objtable_size = objtable->size();
  ObjectID target_objectids_size = target_objectids->size();
  ObjectID reverse_target_objectids_size = reverse_target_objectids->size();
  ObjectID reference_counts_size = reference_counts->size();
  ObjectID contained_objectids_size = contained_objectids->size();
  RAY_CHECK_EQ(objtable_size, target_objectids_size, "objtable_ and target_objectids_ should have the same size, but objtable_.size() = " << objtable_size << " and target_objectids_.size() = " << target_objectids_size);
  RAY_CHECK_EQ(objtable_size, reverse_target_objectids_size, "objtable_ and reverse_target_objectids_ should have the same size, but objtable_.size() = " << objtable_size << " and reverse_target_objectids_.size() = " << reverse_target_objectids_size);
  RAY_CHECK_EQ(objtable_size, reference_counts_size, "objtable_ and reference_counts_ should have the same size, but objtable_.size() = " << objtable_size << " and reference_counts_.size() = " << reference_counts_size);
  RAY_CHECK_EQ(objtable_size, contained_objectids_size, "objtable_ and contained_objectids_ should have the same size, but objtable_.size() = " << objtable_size << " and contained_objectids_.size() = " << contained_objectids_size);
  objtable->push_back(std::vector<ObjStoreId>());
  target_objectids->push_back(UNITIALIZED_ALIAS);
  reverse_target_objectids->push_back(std::vector<ObjectID>());
  reference_counts->push_back(0);
  contained_objectids->push_back(std::vector<ObjectID>());
  {
    // We increment once so the objectid doesn't go out of scope before the ObjReady
    // method is called. The corresponding decrement will happen either in
    // ObjReady in the scheduler or in AliasObjectIDs in the scheduler.
    increment_ref_count(std::vector<ObjectID>({objtable_size}), reference_counts); // Note that reference_counts_lock_ is acquired above, as assumed by increment_ref_count
  }
  return objtable_size;
}

void SchedulerService::add_location(ObjectID canonical_objectid, ObjStoreId objstoreid) {
  // add_location must be called with a canonical objectid
  RAY_CHECK_NEQ((*GET(reference_counts_))[canonical_objectid], DEALLOCATED, "Calling ObjReady with canonical_objectid " << canonical_objectid << ", but this objectid has already been deallocated");
  RAY_CHECK(is_canonical(canonical_objectid), "Attempting to call add_location with a non-canonical objectid (objectid " << canonical_objectid << ")");
  auto objtable = GET(objtable_);
  RAY_CHECK_LT(canonical_objectid, objtable->size(), "trying to put an object in the object store that was not registered with the scheduler (objectid " << canonical_objectid << ")");
  // do a binary search
  auto &locations = (*objtable)[canonical_objectid];
  auto pos = std::lower_bound(locations.begin(), locations.end(), objstoreid);
  if (pos == locations.end() || objstoreid < *pos) {
    locations.insert(pos, objstoreid);
  }
  auto &objects_in_flight = objects_in_transit_[objstoreid];
  objects_in_flight.erase(std::remove(objects_in_flight.begin(), objects_in_flight.end(), canonical_objectid), objects_in_flight.end());
}

void SchedulerService::add_canonical_objectid(ObjectID objectid) {
  auto target_objectids = GET(target_objectids_);
  RAY_CHECK_LT(objectid, target_objectids->size(), "internal error: attempting to insert objectid " << objectid << " in target_objectids_, but target_objectids_.size() is " << target_objectids->size());
  RAY_CHECK((*target_objectids)[objectid] == UNITIALIZED_ALIAS || (*target_objectids)[objectid] == objectid, "internal error: attempting to declare objectid " << objectid << " as a canonical objectid, but target_objectids_[objectid] is already aliased with objectid " << (*target_objectids)[objectid]);
  (*target_objectids)[objectid] = objectid;
}

ObjStoreId SchedulerService::get_store(WorkerId workerid) {
  auto workers = GET(workers_);
  ObjStoreId result = (*workers)[workerid].objstoreid;
  return result;
}

void SchedulerService::register_function(const std::string& name, WorkerId workerid, size_t num_return_vals) {
  auto fntable = GET(fntable_);
  FnInfo& info = (*fntable)[name];
  info.set_num_return_vals(num_return_vals);
  info.add_worker(workerid);
}

void SchedulerService::get_info(const SchedulerInfoRequest& request, SchedulerInfoReply* reply) {
  auto computation_graph = GET(computation_graph_);
  auto fntable = GET(fntable_);
  auto avail_workers = GET(avail_workers_);
  auto task_queue = GET(task_queue_);
  auto reference_counts = GET(reference_counts_);
  auto objstores = GET(objstores_);
  auto target_objectids = GET(target_objectids_);
  auto function_table = reply->mutable_function_table();
  // Return info about the reference counts.
  for (int i = 0; i < reference_counts->size(); ++i) {
    reply->add_reference_count((*reference_counts)[i]);
  }
  // Return info about the target objectids.
  for (int i = 0; i < target_objectids->size(); ++i) {
    reply->add_target_objectid((*target_objectids)[i]);
  }
  // Return info about the function table.
  for (const auto& entry : *fntable) {
    (*function_table)[entry.first].set_num_return_vals(entry.second.num_return_vals());
    for (const WorkerId& worker : entry.second.workers()) {
      (*function_table)[entry.first].add_workerid(worker);
    }
  }
  // Return info about the task queue.
  for (const auto& entry : *task_queue) {
    reply->add_operationid(entry);
  }
  // Return info about the available workers.
  for (const WorkerId& entry : *avail_workers) {
    reply->add_avail_worker(entry);
  }
  // Return info about the computation graph.
  computation_graph->to_protobuf(reply->mutable_computation_graph());
  // Return info about the object stores.
  for (int i = 0; i < objstores->size(); ++i) {
    ObjstoreData* objstore_data = reply->add_objstore();
    objstore_data->set_objstoreid(i);
    objstore_data->set_address((*objstores)[i].address);
  }
}

// pick_objstore must be called with a canonical_objectid
ObjStoreId SchedulerService::pick_objstore(ObjectID canonical_objectid) {
  std::mt19937 rng;
  RAY_CHECK(is_canonical(canonical_objectid), "Attempting to call pick_objstore with a non-canonical objectid, (objectid " << canonical_objectid << ")");
  auto objtable = GET(objtable_);
  std::uniform_int_distribution<int> uni(0, (*objtable)[canonical_objectid].size() - 1);
  ObjStoreId objstoreid = (*objtable)[canonical_objectid][uni(rng)];
  return objstoreid;
}

bool SchedulerService::is_canonical(ObjectID objectid) {
  auto target_objectids = GET(target_objectids_);
  RAY_CHECK_NEQ((*target_objectids)[objectid], UNITIALIZED_ALIAS, "Attempting to call is_canonical on an objectid for which aliasing is not complete or the object is not ready, target_objectids_[objectid] == UNITIALIZED_ALIAS for objectid " << objectid << ".");
  return objectid == (*target_objectids)[objectid];
}

void SchedulerService::perform_functions_to_run() {
  auto workers = GET(workers_);
  auto function_to_run_queue = GET(function_to_run_queue_);
  auto exported_functions_to_run = GET(exported_functions_to_run_);
  while (!function_to_run_queue->empty()) {
    std::pair<WorkerId, int> workerid_functionid_pair = function_to_run_queue->front();
    export_function_to_run_to_worker(workerid_functionid_pair.first, workerid_functionid_pair.second, workers, exported_functions_to_run);
    function_to_run_queue->pop();
  }
}

void SchedulerService::perform_remote_function_exports() {
  auto workers = GET(workers_);
  auto remote_function_export_queue = GET(remote_function_export_queue_);
  auto exported_functions = GET(exported_functions_);
  while (!remote_function_export_queue->empty()) {
    std::pair<WorkerId, int> workerid_functionid_pair = remote_function_export_queue->front();
    export_function_to_worker(workerid_functionid_pair.first, workerid_functionid_pair.second, workers, exported_functions);
    remote_function_export_queue->pop();
  }
}

void SchedulerService::perform_reusable_variable_exports() {
  auto workers = GET(workers_);
  auto reusable_variable_export_queue = GET(reusable_variable_export_queue_);
  auto exported_reusable_variables = GET(exported_reusable_variables_);
  while (!reusable_variable_export_queue->empty()) {
    std::pair<WorkerId, int> workerid_variableid_pair = reusable_variable_export_queue->front();
    export_reusable_variable_to_worker(workerid_variableid_pair.first, workerid_variableid_pair.second, workers, exported_reusable_variables);
    reusable_variable_export_queue->pop();
  }
}

void SchedulerService::perform_gets() {
  auto get_queue = GET(get_queue_);
  // Complete all get tasks that can be completed.
  for (int i = 0; i < get_queue->size(); ++i) {
    const std::pair<WorkerId, ObjectID>& get_request = (*get_queue)[i];
    ObjectID objectid = get_request.second;
    WorkerId workerid = get_request.first;
    ObjStoreId objstoreid = get_store(workerid);
    if (!has_canonical_objectid(objectid)) {
      RAY_LOG(RAY_ALIAS, "objectid " << objectid << " does not have a canonical_objectid, so continuing");
      continue;
    }
    ObjectID canonical_objectid = get_canonical_objectid(objectid);
    RAY_LOG(RAY_DEBUG, "attempting to get objectid " << get_request.second << " with canonical objectid " << canonical_objectid << " to objstore " << objstoreid);
    int num_stores = (*GET(objtable_))[canonical_objectid].size();
    if (num_stores > 0) {
      deliver_object_async_if_necessary(canonical_objectid, pick_objstore(canonical_objectid), objstoreid);
      // Notify the relevant objstore about potential aliasing when it's ready
      GET(alias_notification_queue_)->push_back(std::make_pair(objstoreid, std::make_pair(objectid, canonical_objectid)));
      // Remove the get task from the queue
      std::swap((*get_queue)[i], (*get_queue)[get_queue->size() - 1]);
      get_queue->pop_back();
      i -= 1;
    }
  }
}

void SchedulerService::schedule_tasks_naively() {
  auto computation_graph = GET(computation_graph_);
  auto fntable = GET(fntable_);
  auto avail_workers = GET(avail_workers_);
  auto task_queue = GET(task_queue_);
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
  auto computation_graph = GET(computation_graph_);
  auto fntable = GET(fntable_);
  auto avail_workers = GET(avail_workers_);
  auto task_queue = GET(task_queue_);
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
            ObjectID objectid = task.arg(j).id();
            RAY_CHECK(has_canonical_objectid(objectid), "no canonical object ref found even though task is ready; that should not be possible!");
            ObjectID canonical_objectid = get_canonical_objectid(objectid);
            {
              // check if the object is already in the local object store
              auto objtable = GET(objtable_);
              if (!std::binary_search((*objtable)[canonical_objectid].begin(), (*objtable)[canonical_objectid].end(), objstoreid)) {
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
  auto alias_notification_queue = GET(alias_notification_queue_);
  for (int i = 0; i < alias_notification_queue->size(); ++i) {
    const std::pair<WorkerId, std::pair<ObjectID, ObjectID> > alias_notification = (*alias_notification_queue)[i];
    ObjStoreId objstoreid = alias_notification.first;
    ObjectID alias_objectid = alias_notification.second.first;
    ObjectID canonical_objectid = alias_notification.second.second;
    if (attempt_notify_alias(objstoreid, alias_objectid, canonical_objectid)) { // this locks both the objstore_ and objtable_
      // the attempt to notify the objstore of the objectid aliasing succeeded, so remove the notification task from the queue
      std::swap((*alias_notification_queue)[i], (*alias_notification_queue)[alias_notification_queue->size() - 1]);
      alias_notification_queue->pop_back();
      i -= 1;
    }
  }
}

bool SchedulerService::has_canonical_objectid(ObjectID objectid) {
  auto target_objectids = GET(target_objectids_);
  ObjectID objectid_temp = objectid;
  while (true) {
    RAY_CHECK_LT(objectid_temp, target_objectids->size(), "Attempting to index target_objectids_ with objectid " << objectid_temp << ", but target_objectids_.size() = " << target_objectids->size());
    if ((*target_objectids)[objectid_temp] == UNITIALIZED_ALIAS) {
      return false;
    }
    if ((*target_objectids)[objectid_temp] == objectid_temp) {
      return true;
    }
    objectid_temp = (*target_objectids)[objectid_temp];
  }
}

ObjectID SchedulerService::get_canonical_objectid(ObjectID objectid) {
  // get_canonical_objectid assumes that has_canonical_objectid(objectid) is true
  auto target_objectids = GET(target_objectids_);
  ObjectID objectid_temp = objectid;
  while (true) {
    RAY_CHECK_LT(objectid_temp, target_objectids->size(), "Attempting to index target_objectids_ with objectid " << objectid_temp << ", but target_objectids_.size() = " << target_objectids->size());
    RAY_CHECK_NEQ((*target_objectids)[objectid_temp], UNITIALIZED_ALIAS, "Attempting to get canonical objectid for objectid " << objectid << ", which aliases, objectid " << objectid_temp << ", but target_objectids_[objectid_temp] == UNITIALIZED_ALIAS for objectid_temp = " << objectid_temp << ".");
    if ((*target_objectids)[objectid_temp] == objectid_temp) {
      return objectid_temp;
    }
    objectid_temp = (*target_objectids)[objectid_temp];
    RAY_LOG(RAY_ALIAS, "Looping in get_canonical_objectid.");
  }
}

bool SchedulerService::attempt_notify_alias(ObjStoreId objstoreid, ObjectID alias_objectid, ObjectID canonical_objectid) {
  // return true if successful and false otherwise
  if (alias_objectid == canonical_objectid) {
    // no need to do anything
    return true;
  }
  {
    auto objtable = GET(objtable_);
    if (!std::binary_search((*objtable)[canonical_objectid].begin(), (*objtable)[canonical_objectid].end(), objstoreid)) {
      // the objstore doesn't have the object for canonical_objectid yet, so it's too early to notify the objstore about the alias
      return false;
    }
  }
  ClientContext context;
  AckReply reply;
  NotifyAliasRequest request;
  request.set_alias_objectid(alias_objectid);
  request.set_canonical_objectid(canonical_objectid);
  RAY_CHECK_GRPC((*GET(objstores_))[objstoreid].objstore_stub->NotifyAlias(&context, request, &reply));
  return true;
}

void SchedulerService::deallocate_object(ObjectID canonical_objectid, const MySynchronizedPtr<std::vector<RefCount> > &reference_counts, const MySynchronizedPtr<std::vector<std::vector<ObjectID> > > &contained_objectids) {
  // deallocate_object should only be called from decrement_ref_count (note that
  // deallocate_object also recursively calls decrement_ref_count). Both of
  // these methods take reference_counts and contained_objectids as argumens,
  // which are obtained by GET(reference_counts) and GET(contained_objectids_),
  // so we know that those data structures have been locked
  RAY_LOG(RAY_REFCOUNT, "Deallocating canonical_objectid " << canonical_objectid << ".");
  {
    auto objtable = GET(objtable_);
    auto &locations = (*objtable)[canonical_objectid];
    auto objstores = GET(objstores_); // TODO(rkn): Should this be inside the for loop instead?
    for (int i = 0; i < locations.size(); ++i) {
      ClientContext context;
      AckReply reply;
      DeallocateObjectRequest request;
      request.set_canonical_objectid(canonical_objectid);
      ObjStoreId objstoreid = locations[i];
      RAY_LOG(RAY_REFCOUNT, "Attempting to deallocate canonical_objectid " << canonical_objectid << " from objstore " << objstoreid);
      RAY_CHECK_GRPC((*objstores)[objstoreid].objstore_stub->DeallocateObject(&context, request, &reply));
    }
    locations.clear();
  }
  decrement_ref_count((*contained_objectids)[canonical_objectid], reference_counts, contained_objectids);
}

void SchedulerService::increment_ref_count(const std::vector<ObjectID> &objectids, const MySynchronizedPtr<std::vector<RefCount> > &reference_counts) {
  // increment_ref_count takes reference_counts as an argument, which is
  // obtained by GET(reference_counts_), so we know that the data structure has
  // been locked
  for (int i = 0; i < objectids.size(); ++i) {
    ObjectID objectid = objectids[i];
    RAY_CHECK_NEQ((*reference_counts)[objectid], DEALLOCATED, "Attempting to increment the reference count for objectid " << objectid << ", but this object appears to have been deallocated already.");
    (*reference_counts)[objectid] += 1;
    RAY_LOG(RAY_REFCOUNT, "Incremented ref count for objectid " << objectid <<". New reference count is " << (*reference_counts)[objectid]);
  }
}

void SchedulerService::decrement_ref_count(const std::vector<ObjectID> &objectids, const MySynchronizedPtr<std::vector<RefCount> > &reference_counts, const MySynchronizedPtr<std::vector<std::vector<ObjectID> > > &contained_objectids) {
  // decrement_ref_count takes reference_counts and contained_objectids as
  // arguments, which are obtained by GET(reference_counts_) and
  // GET(contained_objectids_), so we know that those data structures have been
  // locked
  for (int i = 0; i < objectids.size(); ++i) {
    ObjectID objectid = objectids[i];
    RAY_CHECK_NEQ((*reference_counts)[objectid], DEALLOCATED, "Attempting to decrement the reference count for objectid " << objectid << ", but this object appears to have been deallocated already.");
    RAY_CHECK_NEQ((*reference_counts)[objectid], 0, "Attempting to decrement the reference count for objectid " << objectid << ", but the reference count for this object is already 0.");
    (*reference_counts)[objectid] -= 1;
    RAY_LOG(RAY_REFCOUNT, "Decremented ref count for objectid " << objectid << ". New reference count is " << (*reference_counts)[objectid]);
    // See if we can deallocate the object
    std::vector<ObjectID> equivalent_objectids;
    get_equivalent_objectids(objectid, equivalent_objectids);
    bool can_deallocate = true;
    for (int j = 0; j < equivalent_objectids.size(); ++j) {
      if ((*reference_counts)[equivalent_objectids[j]] != 0) {
        can_deallocate = false;
        break;
      }
    }
    if (can_deallocate) {
      ObjectID canonical_objectid = equivalent_objectids[0];
      RAY_CHECK(is_canonical(canonical_objectid), "canonical_objectid is not canonical.");
      deallocate_object(canonical_objectid, reference_counts, contained_objectids);
      for (int j = 0; j < equivalent_objectids.size(); ++j) {
        (*reference_counts)[equivalent_objectids[j]] = DEALLOCATED;
      }
    }
  }
}

void SchedulerService::upstream_objectids(ObjectID objectid, std::vector<ObjectID> &objectids, const MySynchronizedPtr<std::vector<std::vector<ObjectID> > > &reverse_target_objectids) {
  // upstream_objectids takes reverse_target_objectids as an argument, which is
  // obtained by GET(reverse_target_objectids_), so we know the data structure
  // has been locked.
  objectids.push_back(objectid);
  for (int i = 0; i < (*reverse_target_objectids)[objectid].size(); ++i) {
    upstream_objectids((*reverse_target_objectids)[objectid][i], objectids, reverse_target_objectids);
  }
}

void SchedulerService::get_equivalent_objectids(ObjectID objectid, std::vector<ObjectID> &equivalent_objectids) {
  auto target_objectids = GET(target_objectids_);
  ObjectID downstream_objectid = objectid;
  while ((*target_objectids)[downstream_objectid] != downstream_objectid && (*target_objectids)[downstream_objectid] != UNITIALIZED_ALIAS) {
    RAY_LOG(RAY_ALIAS, "Looping in get_equivalent_objectids");
    downstream_objectid = (*target_objectids)[downstream_objectid];
  }
  upstream_objectids(downstream_objectid, equivalent_objectids, GET(reverse_target_objectids_));
}


void SchedulerService::export_function_to_run_to_worker(WorkerId workerid, int function_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<Function> > > &exported_functions_to_run) {
  RAY_LOG(RAY_INFO, "exporting function to run with index " << function_index << " to worker " << workerid);
  ClientContext context;
  RunFunctionOnWorkerRequest request;
  request.mutable_function()->CopyFrom(*(*exported_functions_to_run)[function_index].get());
  AckReply reply;
  RAY_CHECK_GRPC((*workers)[workerid].worker_stub->RunFunctionOnWorker(&context, request, &reply));
}

void SchedulerService::export_function_to_worker(WorkerId workerid, int function_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<Function> > > &exported_functions) {
  RAY_LOG(RAY_INFO, "exporting remote function with index " << function_index << " to worker " << workerid);
  ClientContext context;
  ImportRemoteFunctionRequest request;
  request.mutable_function()->CopyFrom(*(*exported_functions)[function_index].get());
  AckReply reply;
  RAY_CHECK_GRPC((*workers)[workerid].worker_stub->ImportRemoteFunction(&context, request, &reply));
}

void SchedulerService::export_reusable_variable_to_worker(WorkerId workerid, int reusable_variable_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<ReusableVar> > > &exported_reusable_variables) {
  RAY_LOG(RAY_INFO, "exporting reusable variable with index " << reusable_variable_index << " to worker " << workerid);
  ClientContext context;
  ImportReusableVariableRequest request;
  request.mutable_reusable_variable()->CopyFrom(*(*exported_reusable_variables)[reusable_variable_index].get());
  AckReply reply;
  RAY_CHECK_GRPC((*workers)[workerid].worker_stub->ImportReusableVariable(&context, request, &reply));
}

void SchedulerService::add_all_functions_to_run_to_worker_queue(WorkerId workerid) {
  auto function_to_run_queue = GET(function_to_run_queue_);
  auto exported_functions_to_run = GET(exported_functions_to_run_);
  for (int i = 0; i < exported_functions_to_run->size(); ++i) {
    function_to_run_queue->push(std::make_pair(workerid, i));
  }
}

void SchedulerService::add_all_remote_functions_to_worker_export_queue(WorkerId workerid) {
  auto remote_function_export_queue = GET(remote_function_export_queue_);
  auto exported_functions = GET(exported_functions_);
  for (int i = 0; i < exported_functions->size(); ++i) {
    remote_function_export_queue->push(std::make_pair(workerid, i));
  }
}

void SchedulerService::add_all_reusable_variables_to_worker_export_queue(WorkerId workerid) {
  auto reusable_variable_export_queue = GET(reusable_variable_export_queue_);
  auto exported_reusable_variables = GET(exported_reusable_variables_);
  for (int i = 0; i < exported_reusable_variables->size(); ++i) {
    reusable_variable_export_queue->push(std::make_pair(workerid, i));
  }
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
  if (server == nullptr) {
    RAY_CHECK(false, "Failed to create the scheduler service.");
  }
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
        RAY_LOG(RAY_INFO, "scheduler: using 'naive' scheduler");
        scheduling_algorithm = SCHEDULING_ALGORITHM_NAIVE;
      }
      if (std::string(scheduling_algorithm_name) == "locality_aware") {
        RAY_LOG(RAY_INFO, "scheduler: using 'locality aware' scheduler");
        scheduling_algorithm = SCHEDULING_ALGORITHM_LOCALITY_AWARE;
      }
    }
  }
  start_scheduler_service(argv[1], scheduling_algorithm);
  return 0;
}
