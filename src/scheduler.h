#ifndef RAY_SCHEDULER_H
#define RAY_SCHEDULER_H


#include <queue>
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

const ObjectID UNITIALIZED_ALIAS = std::numeric_limits<ObjectID>::max();
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
  Status AliasObjectIDs(ServerContext* context, const AliasObjectIDsRequest* request, AckReply* reply) override;
  Status RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) override;
  Status RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) override;
  Status RegisterRemoteFunction(ServerContext* context, const RegisterRemoteFunctionRequest* request, AckReply* reply) override;
  Status ObjReady(ServerContext* context, const ObjReadyRequest* request, AckReply* reply) override;
  Status ReadyForNewTask(ServerContext* context, const ReadyForNewTaskRequest* request, AckReply* reply) override;
  Status IncrementRefCount(ServerContext* context, const IncrementRefCountRequest* request, AckReply* reply) override;
  Status DecrementRefCount(ServerContext* context, const DecrementRefCountRequest* request, AckReply* reply) override;
  Status AddContainedObjectIDs(ServerContext* context, const AddContainedObjectIDsRequest* request, AckReply* reply) override;
  Status SchedulerInfo(ServerContext* context, const SchedulerInfoRequest* request, SchedulerInfoReply* reply) override;
  Status TaskInfo(ServerContext* context, const TaskInfoRequest* request, TaskInfoReply* reply) override;
  Status KillWorkers(ServerContext* context, const KillWorkersRequest* request, KillWorkersReply* reply) override;
  Status ExportRemoteFunction(ServerContext* context, const ExportRemoteFunctionRequest* request, AckReply* reply) override;
  Status ExportReusableVariable(ServerContext* context, const ExportReusableVariableRequest* request, AckReply* reply) override;
  Status NotifyFailure(ServerContext*, const NotifyFailureRequest* request, AckReply* reply) override;
  Status Select(ServerContext*, const SelectRequest* request, SelectReply* reply) override;

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
  void deliver_object_async_if_necessary(ObjectID objectid, ObjStoreId from, ObjStoreId to);
  // ask an object store to send object to another object store
  void deliver_object_async(ObjectID objectid, ObjStoreId from, ObjStoreId to);
  // assign a task to a worker
  void schedule();
  // execute a task on a worker and ship required object IDs
  void assign_task(OperationId operationid, WorkerId workerid, const MySynchronizedPtr<ComputationGraph> &computation_graph);
  // checks if the dependencies of the task are met
  bool can_run(const Task& task);
  // register a new object with the scheduler and return its object ID
  ObjectID register_new_object();
  // register the location of the object ID in the object table
  void add_location(ObjectID objectid, ObjStoreId objstoreid);
  // indicate that objectid is a canonical objectid
  void add_canonical_objectid(ObjectID objectid);
  // get object store associated with a workerid
  ObjStoreId get_store(WorkerId workerid);
  // register a function with the scheduler
  void register_function(const std::string& name, WorkerId workerid, size_t num_return_vals);
  // get information about the scheduler state
  void get_info(const SchedulerInfoRequest& request, SchedulerInfoReply* reply);
private:
  // pick an objectstore that holds a given object (needs protection by objects_lock_)
  ObjStoreId pick_objstore(ObjectID objectid);
  // checks if objectid is a canonical objectid
  bool is_canonical(ObjectID objectid);
  // Export all queued up remote functions.
  void perform_remote_function_exports();
  // Export all queued up reusable variables.
  void perform_reusable_variable_exports();
  void perform_gets();
  // schedule tasks using the naive algorithm
  void schedule_tasks_naively();
  // schedule tasks using a scheduling algorithm that takes into account data locality
  void schedule_tasks_location_aware();
  void perform_notify_aliases();
  // checks if aliasing for objectid has been completed
  bool has_canonical_objectid(ObjectID objectid);
  // get the canonical objectid for an objectid
  ObjectID get_canonical_objectid(ObjectID objectid);
  // attempt to notify the objstore about potential objectid aliasing, returns true if successful, if false then retry later
  bool attempt_notify_alias(ObjStoreId objstoreid, ObjectID alias_objectid, ObjectID canonical_objectid);
  // tell all of the objstores holding canonical_objectid to deallocate it, the
  // data structures are passed into ensure that the appropriate locks are held.
  void deallocate_object(ObjectID canonical_objectid, const MySynchronizedPtr<std::vector<RefCount> > &reference_counts, const MySynchronizedPtr<std::vector<std::vector<ObjectID> > > &contained_objectids);
  // increment the ref counts for the object IDs in objectids, the data
  // structures are passed into ensure that the appropriate locks are held.
  void increment_ref_count(const std::vector<ObjectID> &objectids, const MySynchronizedPtr<std::vector<RefCount> > &reference_count);
  // decrement the ref counts for the object IDs in objectids, the data
  // structures are passed into ensure that the appropriate locks are held.
  void decrement_ref_count(const std::vector<ObjectID> &objectids, const MySynchronizedPtr<std::vector<RefCount> > &reference_count, const MySynchronizedPtr<std::vector<std::vector<ObjectID> > > &contained_objectids);
  // Find all of the object IDs which are upstream of objectid (including objectid itself). That is, you can get from everything in objectids to objectid by repeatedly indexing in target_objectids_.
  void upstream_objectids(ObjectID objectid, std::vector<ObjectID> &objectids, const MySynchronizedPtr<std::vector<std::vector<ObjectID> > > &reverse_target_objectids);
  // Find all of the object IDs that refer to the same object as objectid (as best as we can determine at the moment). The information may be incomplete because not all of the aliases may be known.
  void get_equivalent_objectids(ObjectID objectid, std::vector<ObjectID> &equivalent_objectids);
  // Export a remote function to a worker.
  void export_function_to_worker(WorkerId workerid, int function_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<Function> > > &exported_functions);
  // Export a reusable variable to a worker
  void export_reusable_variable_to_worker(WorkerId workerid, int reusable_variable_index, MySynchronizedPtr<std::vector<WorkerHandle> > &workers, const MySynchronizedPtr<std::vector<std::unique_ptr<ReusableVar> > > &exported_reusable_variables);
  // Add to the remote function export queue the job of exporting all remote
  // functions to the given worker. This is used when a new worker registers.
  void add_all_remote_functions_to_worker_export_queue(WorkerId workerid);
  // Add to the reusable variable export queue the job of exporting all reusable
  // variables to the given worker. This is used when a new worker registers.
  void add_all_reusable_variables_to_worker_export_queue(WorkerId workerid);

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

  // List of failed tasks
  Synchronized<std::vector<TaskStatus> > failed_tasks_;
  // A list of remote functions import failures.
  Synchronized<std::vector<Failure> > failed_remote_function_imports_;
  // A list of reusable variables import failures.
  Synchronized<std::vector<Failure> > failed_reusable_variable_imports_;
  // A list of reusable variables reinitialization failures.
  Synchronized<std::vector<Failure> > failed_reinitialize_reusable_variables_;
  // List of pending get calls.
  Synchronized<std::vector<std::pair<WorkerId, ObjectID> > > get_queue_;
  // The computation graph tracks the operations that have been submitted to the
  // scheduler and is mostly used for fault tolerance.
  Synchronized<ComputationGraph> computation_graph_;
  // Hash map from function names to workers where the function is registered.
  Synchronized<FnTable> fntable_;
  // Vector of all workers that are currently idle.
  Synchronized<std::vector<WorkerId> > avail_workers_;
  // List of pending tasks.
  Synchronized<std::deque<OperationId> > task_queue_;
  // Reference counts. Currently, reference_counts_[objectid] is the number of
  // existing references held to objectid. This is done for all objectids, not just
  // canonical_objectids. This data structure completely ignores aliasing. If the
  // object corresponding to objectid has been deallocated, then
  // reference_counts[objectid] will equal DEALLOCATED.
  Synchronized<std::vector<RefCount> > reference_counts_;
  // contained_objectids_[objectid] is a vector of all of the objectids contained inside the object referred to by objectid
  Synchronized<std::vector<std::vector<ObjectID> > > contained_objectids_;
  // Vector of all workers registered in the system. Their index in this vector
  // is the workerid.
  Synchronized<std::vector<WorkerHandle> > workers_;
  // List of pending alias notifications. Each element consists of (objstoreid, (alias_objectid, canonical_objectid)).
  Synchronized<std::vector<std::pair<ObjStoreId, std::pair<ObjectID, ObjectID> > > > alias_notification_queue_;
  // Mapping from canonical objectid to list of object stores where the object is stored. Non-canonical (aliased) objectids should not be used to index objtable_.
  Synchronized<ObjTable> objtable_; // This lock protects objtable_ and objects_in_transit_
  // Vector of all object stores registered in the system. Their index in this
  // vector is the objstoreid.
  Synchronized<std::vector<ObjStoreHandle> > objstores_;
  // Mapping from an aliased objectid to the objectid it is aliased with. If an
  // objectid is a canonical objectid (meaning it is not aliased), then
  // target_objectids_[objectid] == objectid. For each objectid, target_objectids_[objectid]
  // is initialized to UNITIALIZED_ALIAS and the correct value is filled later
  // when it is known.
  Synchronized<std::vector<ObjectID> > target_objectids_;
  // This data structure maps an objectid to all of the objectids that alias it (there could be multiple such objectids).
  Synchronized<std::vector<std::vector<ObjectID> > > reverse_target_objectids_;
  // For each object store objstoreid, objects_in_transit_[objstoreid] is a
  // vector of the canonical object IDs that are being streamed to that
  // object store but are not yet present. object IDs are added to this
  // in deliver_object_async_if_necessary (to ensure that we do not attempt to deliver
  // the same object to a given object store twice), and object IDs are
  // removed when add_location is called (from ObjReady), and they are moved to
  // the objtable_. Note that objects_in_transit_ and objtable_ share the same
  // lock (objects_lock_). // TODO(rkn): Consider making this part of the
  // objtable data structure.
  std::vector<std::vector<ObjectID> > objects_in_transit_;
  // List of pending remote function exports. These should be processed in a
  // first in first out manner. The first element of each pair is the ID of the
  // worker to export the remote function to, and the second element of each
  // pair is the index of the function to export.
  Synchronized<std::queue<std::pair<WorkerId, int> > > remote_function_export_queue_;
  // List of pending reusable variable exports. These should be processed in a
  // first in first out manner. The first element of each pair is the ID of the
  // worker to export the reusable variable to, and the second element of each
  // pair is the index of the reusable variable to export.
  Synchronized<std::queue<std::pair<WorkerId, int> > > reusable_variable_export_queue_;
  // All of the remote functions that have been exported to the workers.
  Synchronized<std::vector<std::unique_ptr<Function> > > exported_functions_;
  // All of the reusable variables that have been exported to the workers.
  Synchronized<std::vector<std::unique_ptr<ReusableVar> > > exported_reusable_variables_;
  // the scheduling algorithm that will be used
  SchedulingAlgorithmType scheduling_algorithm_;
};

#endif
