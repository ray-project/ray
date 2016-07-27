#ifndef RAY_SCHEDULER_H
#define RAY_SCHEDULER_H


#include <deque>
#include <memory>
#include <algorithm>
#include <iostream>
#include <limits>

#include <grpc++/grpc++.h>

#include "ray/ray.h"
#include "ray.grpc.pb.h"
#include "types.pb.h"

#include "utils.h"
#include "computation_graph.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerReader;
using grpc::ServerContext;
using grpc::Status;

using grpc::ClientContext;

using grpc::Channel;

typedef size_t RefCount;

const ObjRef UNITIALIZED_ALIAS = std::numeric_limits<ObjRef>::max();
const RefCount DEALLOCATED = std::numeric_limits<RefCount>::max();

struct WorkerHandle {
  std::shared_ptr<Channel> channel;
  std::unique_ptr<WorkerService::Stub> worker_stub; // If null, the worker has died
  ObjStoreId objstoreid;
  std::string worker_address;
  // This field is initialized to false, and it is set to true after all of the
  // exported functions and exported reusable variables have been shipped to
  // this worker.
  bool initialized;
  OperationId current_task;
};

struct ObjStoreHandle {
  std::shared_ptr<Channel> channel;
  std::unique_ptr<ObjStore::Stub> objstore_stub;
  std::string address;
};

enum SchedulingAlgorithmType {
  SCHEDULING_ALGORITHM_NAIVE = 0,
  SCHEDULING_ALGORITHM_LOCALITY_AWARE = 1
};

class SchedulerService : public Scheduler::Service {
public:
  SchedulerService(SchedulingAlgorithmType scheduling_algorithm);

  Status SubmitTask(ServerContext* context, const SubmitTaskRequest* request, SubmitTaskReply* reply) override;
  Status PutObj(ServerContext* context, const PutObjRequest* request, PutObjReply* reply) override;
  Status RequestObj(ServerContext* context, const RequestObjRequest* request, AckReply* reply) override;
  Status AliasObjRefs(ServerContext* context, const AliasObjRefsRequest* request, AckReply* reply) override;
  Status RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) override;
  Status RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) override;
  Status RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) override;
  Status ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) override;
  Status ReadyForNewTask(ServerContext* context, const ReadyForNewTaskRequest* request, AckReply* reply) override;
  Status IncrementRefCount(ServerContext* context, const IncrementRefCountRequest* request, AckReply* reply) override;
  Status DecrementRefCount(ServerContext* context, const DecrementRefCountRequest* request, AckReply* reply) override;
  Status AddContainedObjRefs(ServerContext* context, const AddContainedObjRefsRequest* request, AckReply* reply) override;
  Status SchedulerInfo(ServerContext* context, const SchedulerInfoRequest* request, SchedulerInfoReply* reply) override;
  Status TaskInfo(ServerContext* context, const TaskInfoRequest* request, TaskInfoReply* reply) override;
  Status KillWorkers(ServerContext* context, const KillWorkersRequest* request, KillWorkersReply* reply) override;
  Status ExportFunction(ServerContext* context, const ExportFunctionRequest* request, ExportFunctionReply* reply) override;
  Status ExportReusableVariable(ServerContext* context, const ExportReusableVariableRequest* request, AckReply* reply) override;

#ifdef NDEBUG
  // If we've disabled assertions, then just use regular SynchronizedPtr to skip lock checking.
  template<class T>
  using MySynchronizedPtr = SynchronizedPtr<T>;
#else
  // A SynchronizedPtr specialized for this class to dynamically check that locks are obtained in the correct order (in the order of field declarations).
  template<class T>
  class MySynchronizedPtr;
#endif

  // This will ask an object store to send an object to another object store if
  // the object is not already present in that object store and is not already
  // being transmitted.
  void deliver_object_async_if_necessary(ObjRef objref, ObjStoreId from, ObjStoreId to);
  // ask an object store to send object to another object store
  void deliver_object_async(ObjRef objref, ObjStoreId from, ObjStoreId to);
  // assign a task to a worker
  void schedule();
  // execute a task on a worker and ship required object references
  void assign_task(OperationId operationid, WorkerId workerid, const MySynchronizedPtr<ComputationGraph> &computation_graph);
  // checks if the dependencies of the task are met
  bool can_run(const Task& task);
  // register a worker and its object store (if it has not been registered yet)
  std::pair<WorkerId, ObjStoreId> register_worker(const std::string& worker_address, const std::string& objstore_address, bool is_driver);
  // register a new object with the scheduler and return its object reference
  ObjRef register_new_object();
  // register the location of the object reference in the object table
  void add_location(ObjRef objref, ObjStoreId objstoreid);
  // indicate that objref is a canonical objref
  void add_canonical_objref(ObjRef objref);
  // get object store associated with a workerid
  ObjStoreId get_store(WorkerId workerid);
  // register a function with the scheduler
  void register_function(const std::string& name, WorkerId workerid, size_t num_return_vals);
  // get information about the scheduler state
  void get_info(const SchedulerInfoRequest& request, SchedulerInfoReply* reply);
private:
  // pick an objectstore that holds a given object (needs protection by objects_lock_)
  ObjStoreId pick_objstore(ObjRef objref);
  // checks if objref is a canonical objref
  bool is_canonical(ObjRef objref);
  void perform_gets();
  // schedule tasks using the naive algorithm
  void schedule_tasks_naively();
  // schedule tasks using a scheduling algorithm that takes into account data locality
  void schedule_tasks_location_aware();
  void perform_notify_aliases();
  // checks if aliasing for objref has been completed
  bool has_canonical_objref(ObjRef objref);
  // get the canonical objref for an objref
  ObjRef get_canonical_objref(ObjRef objref);
  // attempt to notify the objstore about potential objref aliasing, returns true if successful, if false then retry later
  bool attempt_notify_alias(ObjStoreId objstoreid, ObjRef alias_objref, ObjRef canonical_objref);
  // tell all of the objstores holding canonical_objref to deallocate it, the
  // data structures are passed into ensure that the appropriate locks are held.
  void deallocate_object(ObjRef canonical_objref, const MySynchronizedPtr<std::vector<RefCount> > &reference_counts, const MySynchronizedPtr<std::vector<std::vector<ObjRef> > > &contained_objrefs);
  // increment the ref counts for the object references in objrefs, the data
  // structures are passed into ensure that the appropriate locks are held.
  void increment_ref_count(const std::vector<ObjRef> &objrefs, const MySynchronizedPtr<std::vector<RefCount> > &reference_count);
  // decrement the ref counts for the object references in objrefs, the data
  // structures are passed into ensure that the appropriate locks are held.
  void decrement_ref_count(const std::vector<ObjRef> &objrefs, const MySynchronizedPtr<std::vector<RefCount> > &reference_count, const MySynchronizedPtr<std::vector<std::vector<ObjRef> > > &contained_objrefs);
  // Find all of the object references which are upstream of objref (including objref itself). That is, you can get from everything in objrefs to objref by repeatedly indexing in target_objrefs_.
  void upstream_objrefs(ObjRef objref, std::vector<ObjRef> &objrefs, const MySynchronizedPtr<std::vector<std::vector<ObjRef> > > &reverse_target_objrefs);
  // Find all of the object references that refer to the same object as objref (as best as we can determine at the moment). The information may be incomplete because not all of the aliases may be known.
  void get_equivalent_objrefs(ObjRef objref, std::vector<ObjRef> &equivalent_objrefs);
  // Export a remote function to a worker.
  void export_function_to_worker(WorkerId workerid, int function_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<Function> > > &exported_functions);
  // Export a reusable variable to a worker
  void export_reusable_variable_to_worker(WorkerId workerid, int reusable_variable_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<ReusableVar> > > &exported_reusable_variables);
  // Export all reusable variables to a worker. This is used when a new worker
  // registers and is protected by the workers lock (which is passed in) to
  // ensure that no other reusable variables are exported to the worker while
  // this method is being called.
  void export_all_functions_to_worker(WorkerId workerid, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<Function> > > &exported_functions);
  // Export all remote functions to a worker. This is used when a new worker
  // registers and is protected by the workers lock (which is passed in) to
  // ensure that no other remote functions are exported to the worker while this
  // method is being called.
  void export_all_reusable_variables_to_worker(WorkerId workerid, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<ReusableVar> > > &exported_reusable_variables);

  template<class T>
  MySynchronizedPtr<T> get(Synchronized<T>& my_field, const char* name,unsigned int line_number);
  template<class T>
  MySynchronizedPtr<const T> get(const Synchronized<T>& my_field, const char* name,unsigned int line_number) const;

  // Preferably keep this as the first field to distinguish it from the rest
  // Maps every thread to an identifier of a lock it is holding, as well the name of the lock.
  // Internally, the identifier for each lock is the offset of the field being locked.
  // When we lock, we set the field offset and store the difference; the difference should always be positive. If not, we throw.
  // When we unlock, we subtract back the field offset to restore it to the previous field that was locked.
  mutable Synchronized<std::vector<std::pair<unsigned long long, std::pair<size_t, const char*> > > > lock_orders_;

  // List of the IDs of successful tasks
  Synchronized<std::vector<OperationId> > successful_tasks_; // Right now, we only use this information in the TaskInfo call.
  // List of failed tasks
  Synchronized<std::vector<TaskStatus> > failed_tasks_;
  // List of pending get calls.
  Synchronized<std::vector<std::pair<WorkerId, ObjRef> > > get_queue_;
  // The computation graph tracks the operations that have been submitted to the
  // scheduler and is mostly used for fault tolerance.
  Synchronized<ComputationGraph> computation_graph_;
  // Hash map from function names to workers where the function is registered.
  Synchronized<FnTable> fntable_;
  // Vector of all workers that are currently idle.
  Synchronized<std::vector<WorkerId> > avail_workers_;
  // List of pending tasks.
  Synchronized<std::deque<OperationId> > task_queue_;
  // Reference counts. Currently, reference_counts_[objref] is the number of
  // existing references held to objref. This is done for all objrefs, not just
  // canonical_objrefs. This data structure completely ignores aliasing. If the
  // object corresponding to objref has been deallocated, then
  // reference_counts[objref] will equal DEALLOCATED.
  Synchronized<std::vector<RefCount> > reference_counts_;
  // contained_objrefs_[objref] is a vector of all of the objrefs contained inside the object referred to by objref
  Synchronized<std::vector<std::vector<ObjRef> > > contained_objrefs_;
  // Vector of all workers registered in the system. Their index in this vector
  // is the workerid.
  Synchronized<std::vector<WorkerHandle> > workers_;
  // List of pending alias notifications. Each element consists of (objstoreid, (alias_objref, canonical_objref)).
  Synchronized<std::vector<std::pair<ObjStoreId, std::pair<ObjRef, ObjRef> > > > alias_notification_queue_;
  // Mapping from canonical objref to list of object stores where the object is stored. Non-canonical (aliased) objrefs should not be used to index objtable_.
  Synchronized<ObjTable> objtable_; // This lock protects objtable_ and objects_in_transit_
  // Vector of all object stores registered in the system. Their index in this
  // vector is the objstoreid.
  Synchronized<std::vector<ObjStoreHandle> > objstores_;
  // Mapping from an aliased objref to the objref it is aliased with. If an
  // objref is a canonical objref (meaning it is not aliased), then
  // target_objrefs_[objref] == objref. For each objref, target_objrefs_[objref]
  // is initialized to UNITIALIZED_ALIAS and the correct value is filled later
  // when it is known.
  Synchronized<std::vector<ObjRef> > target_objrefs_;
  // This data structure maps an objref to all of the objrefs that alias it (there could be multiple such objrefs).
  Synchronized<std::vector<std::vector<ObjRef> > > reverse_target_objrefs_;
  // For each object store objstoreid, objects_in_transit_[objstoreid] is a
  // vector of the canonical object references that are being streamed to that
  // object store but are not yet present. Object references are added to this
  // in deliver_object_async_if_necessary (to ensure that we do not attempt to deliver
  // the same object to a given object store twice), and object references are
  // removed when add_location is called (from ObjReady), and they are moved to
  // the objtable_. Note that objects_in_transit_ and objtable_ share the same
  // lock (objects_lock_). // TODO(rkn): Consider making this part of the
  // objtable data structure.
  std::vector<std::vector<ObjRef> > objects_in_transit_;
  // All of the remote functions that have been exported to the workers.
  Synchronized<std::vector<std::unique_ptr<Function> > > exported_functions_;
  // All of the reusable variables that have been exported to the workers.
  Synchronized<std::vector<std::unique_ptr<ReusableVar> > > exported_reusable_variables_;
  // the scheduling algorithm that will be used
  SchedulingAlgorithmType scheduling_algorithm_;
};

#endif
