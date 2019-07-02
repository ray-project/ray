#ifndef RAY_RPC_RAYLET_CLIENT_H
#define RAY_RPC_RAYLET_CLIENT_H

#include <unistd.h>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/raylet/task_spec.h"
#include "src/ray/protobuf/raylet.grpc.pb.h"
#include "src/ray/protobuf/raylet.pb.h"
#include "src/ray/rpc/client_call.h"

namespace ray {
namespace rpc {

using ray::ActorCheckpointID;
using ray::ActorID;
using ray::JobID;
using ray::ObjectID;
using ray::TaskID;
using ray::WorkerID;

using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;
using WaitResultPair = std::pair<std::vector<ObjectID>, std::vector<ObjectID>>;
/// TODO(jzh): At persent, we use promise and future to wrap around the async callback
///            to implement synchronized call to raylet server. It's just a workaround and
///             we would change it to actually
///            async mode once we finish the refactor of the worker to a server
///            programming mode.
/// Client used for communicating with a local node manager server.
class RayletClient {
 public:
  /// Constructor for raylet client.
  /// TODO(jzh): At present, client call manager and reply handler service are generated
  /// in raylet client. Should add parameters to the constructor and pass them in.
  /// Change them as input parameters once we changed the worker into a server.
  ///
  /// \param[in] raylet_socket Unix domain socket of the raylet server.
  RayletClient(const std::string &raylet_socket, const WorkerID &worker_id,
               bool is_worker, const JobID &job_id, const ::Language &language);

  ~RayletClient();

  /// Send disconnect request to local raylet.
  ray::Status Disconnect();

  /// Submit task to local raylet
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  ray::Status SubmitTask(const std::vector<ObjectID> &execution_dependencies,
                         const ray::raylet::TaskSpecification &task_spec);

  /// Get next task for this client. This will block until the scheduler assigns
  /// a task to this worker. The caller takes ownership of the returned task
  /// specification and must free it.
  ///
  /// \param task_spec The assigned task.
  /// \return void.
  ray::Status GetTask(std::unique_ptr<ray::raylet::TaskSpecification> *task_spec);

  /// Tell the raylet to reconstruct or fetch objects.
  ///
  /// \param object_ids The IDs of the objects to reconstruct.
  /// \param fetch_only Only fetch objects, do not reconstruct them.
  /// \param current_task_id The task that needs the objects.
  /// \return int 0 means correct, other numbers mean error.
  ray::Status FetchOrReconstruct(const std::vector<ObjectID> &object_ids, bool fetch_only,
                                 const TaskID &current_task_id);
  /// Notify the raylet that this client (worker) is no longer blocked.
  ///
  /// \param current_task_id The task that is no longer blocked.
  /// \return void.
  ray::Status NotifyUnblocked(const TaskID &current_task_id);

  /// Wait for the given objects until timeout expires or num_return objects are
  /// found.
  ///
  /// \param object_ids The objects to wait for.
  /// \param num_returns The number of objects to wait for.
  /// \param timeout_milliseconds Duration, in milliseconds, to wait before returning.
  /// \param wait_local Whether to wait for objects to appear on this node.
  /// \param current_task_id The task that called wait.
  /// \param result A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  /// \return void.
  ray::Status Wait(const std::vector<ObjectID> &object_ids, int num_returns,
                   int64_t timeout_milliseconds, bool wait_local,
                   const TaskID &current_task_id, WaitResultPair *result);

  /// Push an error to the relevant driver.
  ///
  /// \param The ID of the job_id that the error is for.
  /// \param The type of the error.
  /// \param The error message.
  /// \param The timestamp of the error.
  /// \return void.
  ray::Status PushError(const ray::JobID &job_id, const std::string &type,
                        const std::string &error_message, double timestamp);

  /// Store some profile events in the GCS.
  ///
  /// \param profile_events A batch of profiling event information.
  /// \return void.
  ray::Status PushProfileEvents(const ProfileTableData &profile_events);

  /// Free a list of objects from object stores.
  ///
  /// \param object_ids A list of ObjectsIDs to be deleted.
  /// \param local_only Whether keep this request with local object store
  /// or send it to all the object stores.
  /// \param delete_creating_tasks Whether also delete objects' creating tasks from GCS.
  /// \return void.
  ray::Status FreeObjects(const std::vector<ray::ObjectID> &object_ids, bool local_only,
                          bool deleteCreatingTasks);

  /// Request raylet backend to prepare a checkpoint for an actor.
  ///
  /// \param actor_id ID of the actor.
  /// \param checkpoint_id ID of the new checkpoint (output parameter).
  /// \return void.
  ray::Status PrepareActorCheckpoint(const ActorID &actor_id,
                                     ActorCheckpointID &checkpoint_id);

  /// Notify raylet backend that an actor was resumed from a checkpoint.
  ///
  /// \param actor_id ID of the actor.
  /// \param checkpoint_id ID of the checkpoint from which the actor was resumed.
  /// \return void.
  ray::Status NotifyActorResumedFromCheckpoint(const ActorID &actor_id,
                                               const ActorCheckpointID &checkpoint_id);

  /// Sets a resource with the specified capacity and client id
  /// \param resource_name Name of the resource to be set
  /// \param capacity Capacity of the resource
  /// \param client_Id ClientID where the resource is to be set
  /// \return void
  ray::Status SetResource(const std::string &resource_name, const double capacity,
                          const ray::ClientID &client_Id);

  ::Language GetLanguage() const { return language_; }

  WorkerID GetWorkerID() const { return worker_id_; }

  JobID GetJobID() const { return job_id_; }

  bool IsWorker() const { return is_worker_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

 private:
  ray::Status RegisterClient();

  void Heartbeat();
  /// Id of the worker to which this raylet client belongs.
  const WorkerID worker_id_;
  /// Indicates whether this worker is a driver worker.
  /// Driver is treated as a special worker.
  const bool is_worker_;
  const JobID job_id_;
  const ::Language language_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;

  /// The gRPC-generated stub.
  std::unique_ptr<RayletService::Stub> stub_;

  /// Service for handling reply.
  boost::asio::io_service main_service_;

  /// Asio work for main service.
  boost::asio::io_service::work work_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager client_call_manager_;

  /// The thread used to handle reply.
  std::thread rpc_thread_;

  /// Heartbeat timer.
  boost::asio::deadline_timer heartbeat_timer_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_RAYLET_CLIENT_H
