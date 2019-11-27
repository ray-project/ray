#ifndef RAY_GCS_PB_UTIL_H
#define RAY_GCS_PB_UTIL_H

#include <memory>
#include "ray/common/id.h"
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

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_PB_UTIL_H
