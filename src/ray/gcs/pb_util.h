#ifndef RAY_GCS_PB_UTIL_H
#define RAY_GCS_PB_UTIL_H

#include <memory>
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// Helper function to produce job table data (for newly created job or updated job).
///
/// \param job_id The ID of job that need to be registered or updated.
/// \param is_dead Whether the driver of this job is dead.
/// \param timestamp The UNIX timestamp of corresponding to this event.
/// \param node_manager_address Address of the node this job was started on.
/// \param driver_pid Process ID of the driver running this job.
/// \return The job table data created by this method.
inline std::shared_ptr<ray::rpc::JobTableData> CreateJobTableData(
    const ray::JobID &job_id, bool is_dead, int64_t timestamp,
    const std::string &node_manager_address, int64_t driver_pid) {
  auto job_info_ptr = std::make_shared<ray::rpc::JobTableData>();
  job_info_ptr->set_job_id(job_id.Binary());
  job_info_ptr->set_is_dead(is_dead);
  job_info_ptr->set_timestamp(timestamp);
  job_info_ptr->set_node_manager_address(node_manager_address);
  job_info_ptr->set_driver_pid(driver_pid);
  return job_info_ptr;
}

/// Helper function to produce error table data.
inline std::shared_ptr<ray::rpc::ErrorTableData> CreateErrorTableData(
    const std::string &error_type, const std::string &error_msg, double timestamp,
    const JobID &job_id = JobID::Nil()) {
  auto error_info_ptr = std::make_shared<ray::rpc::ErrorTableData>();
  error_info_ptr->set_type(error_type);
  error_info_ptr->set_error_message(error_msg);
  error_info_ptr->set_timestamp(timestamp);
  error_info_ptr->set_job_id(job_id.Binary());
  return error_info_ptr;
}

/// Helper function to produce actor table data.
inline std::shared_ptr<ray::rpc::ActorTableData> CreateActorTableData(
    const TaskSpecification &task_spec, const ray::rpc::Address &address,
    ray::rpc::ActorTableData::ActorState state, uint64_t remaining_reconstructions) {
  RAY_CHECK(task_spec.IsActorCreationTask());
  auto actor_id = task_spec.ActorCreationId();
  auto actor_info_ptr = std::make_shared<ray::rpc::ActorTableData>();
  // Set all of the static fields for the actor. These fields will not change
  // even if the actor fails or is reconstructed.
  actor_info_ptr->set_actor_id(actor_id.Binary());
  actor_info_ptr->set_parent_id(task_spec.CallerId().Binary());
  actor_info_ptr->set_actor_creation_dummy_object_id(
      task_spec.ActorDummyObject().Binary());
  actor_info_ptr->set_job_id(task_spec.JobId().Binary());
  actor_info_ptr->set_max_reconstructions(task_spec.MaxActorReconstructions());
  actor_info_ptr->set_is_detached(task_spec.IsDetachedActor());
  // Set the fields that change when the actor is restarted.
  actor_info_ptr->set_remaining_reconstructions(remaining_reconstructions);
  actor_info_ptr->set_is_direct_call(task_spec.IsDirectCall());
  actor_info_ptr->mutable_address()->CopyFrom(address);
  actor_info_ptr->mutable_owner_address()->CopyFrom(
      task_spec.GetMessage().caller_address());
  actor_info_ptr->set_state(state);
  return actor_info_ptr;
}

/// Helper function to produce worker failure data.
inline std::shared_ptr<ray::rpc::WorkerFailureData> CreateWorkerFailureData(
    const ClientID &raylet_id, const WorkerID &worker_id, const std::string &address,
    int32_t port, int64_t timestamp = std::time(nullptr)) {
  auto worker_failure_info_ptr = std::make_shared<ray::rpc::WorkerFailureData>();
  worker_failure_info_ptr->mutable_worker_address()->set_raylet_id(raylet_id.Binary());
  worker_failure_info_ptr->mutable_worker_address()->set_worker_id(worker_id.Binary());
  worker_failure_info_ptr->mutable_worker_address()->set_ip_address(address);
  worker_failure_info_ptr->mutable_worker_address()->set_port(port);
  worker_failure_info_ptr->set_timestamp(timestamp);
  return worker_failure_info_ptr;
}

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_PB_UTIL_H
