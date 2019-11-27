#include "ray/gcs/job_state_accessor.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

JobStateAccessor::JobStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl), job_sub_executor_(client_impl.job_table()) {}

Status JobStateAccessor::AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                                  const StatusCallback &callback) {
  return DoAsyncAppend(data_ptr, callback);
}

Status JobStateAccessor::AsyncMarkFinished(const JobID &job_id,
                                           const StatusCallback &callback) {
  std::shared_ptr<JobTableData> data_ptr =
      CreateJobTableData(job_id, /*is_dead*/ true, /*time_stamp*/ std::time(nullptr),
                         /*node_manager_address*/ "", /*driver_pid*/ -1);
  return DoAsyncAppend(data_ptr, callback);
}

Status JobStateAccessor::DoAsyncAppend(const std::shared_ptr<JobTableData> &data_ptr,
                                       const StatusCallback &callback) {
  JobTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const JobID &job_id,
                         const JobTableData &data) { callback(Status::OK()); };
  }

  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  JobTable &job_table = client_impl_.job_table();
  return job_table.Append(job_id, job_id, data_ptr, on_done);
}

Status JobStateAccessor::AsyncSubscribeToFinishedJobs(
    const SubscribeCallback<JobID, JobTableData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const JobID &job_id, const JobTableData &job_data) {
    if (job_data.is_dead()) {
      subscribe(job_id, job_data);
    }
  };
  return job_sub_executor_.AsyncSubscribeAll(ClientID::Nil(), on_subscribe, done);
}

}  // namespace gcs

}  // namespace ray
