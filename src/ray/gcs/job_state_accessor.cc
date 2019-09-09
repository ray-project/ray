#include "ray/gcs/job_state_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

JobStateAccessor::JobStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl), job_sub_executor_(client_impl.job_table()) {}

Status JobStateAccessor::AsyncRegister(const std::shared_ptr<JobTableData> &data_ptr,
                                       const StatusCallback &callback) {
  return DoAsyncRegisterOrUpdate(data_ptr, callback);
}

Status JobStateAccessor::AsyncUpdate(const std::shared_ptr<JobTableData> &data_ptr,
                                     const StatusCallback &callback) {
  return DoAsyncRegisterOrUpdate(data_ptr, callback);
}

Status JobStateAccessor::DoAsyncRegisterOrUpdate(
    const std::shared_ptr<JobTableData> &data_ptr, const StatusCallback &callback) {
  JobTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const JobID &job_id,
                         const JobTableData &data) { callback(Status::OK()); };
  }

  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  JobTable &job_table = client_impl_.job_table();
  return job_table.Append(job_id, job_id, data_ptr, on_done);
}

Status JobStateAccessor::AsyncSubscribeAll(
    const SubscribeCallback<std::vector<JobTableData>> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](const JobID &job_id,
                                  const std::vector<JobTableData> &data) {
    subscribe(data);
  };
  return job_sub_executor_.AsyncSubscribe(ClientID::Nil(), on_subscribe, done);
}

}  // namespace gcs

}  // namespace ray
