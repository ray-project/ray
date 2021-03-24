#include "ray/raylet/job_manager.h"

namespace ray {
namespace raylet {
bool JobManager::OnJobSubmitted(std::shared_ptr<JobTableData> job_table_data) {
  RAY_CHECK(job_table_data && job_table_data->state() == rpc::JobTableData::SUBMITTED);
  auto job_id = JobID::FromBinary(job_table_data->job_id());
  return jobs_.emplace(job_id, job_table_data).second;
}

bool JobManager::OnJobStarted(std::shared_ptr<JobTableData> job_table_data) {
  RAY_CHECK(job_table_data && job_table_data->state() == rpc::JobTableData::RUNNING);
  auto job_id = JobID::FromBinary(job_table_data->job_id());
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    jobs_.emplace(job_id, std::move(job_table_data));
    return true;
  }
  return iter->second->state() == rpc::JobTableData::SUBMITTED;
}

bool JobManager::OnJobFailedOrFinished(const JobID &job_id) {
  return jobs_.erase(job_id) != 0;
}

std::shared_ptr<JobTableData> JobManager::GetJobData(const JobID &job_id) const {
  auto iter = jobs_.find(job_id);
  return iter == jobs_.end() ? nullptr : iter->second;
}

}  // namespace raylet
}  // namespace ray
