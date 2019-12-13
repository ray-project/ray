#ifndef RAY_GCS_REDIS_JOB_INFO_ACCESSOR_H
#define RAY_GCS_REDIS_JOB_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/job_info_accessor.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class RedisJobInfoAccessor
/// RedisJobInfoAccessor is an implementation of `JobInfoAccessor`
/// that uses Redis as the backend storage.
class RedisJobInfoAccessor : public JobInfoAccessor {
 public:
  explicit RedisJobInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisJobInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, JobTableData> &subscribe,
      const StatusCallback &done) override;

 private:
  /// Append job information to GCS asynchronously.
  ///
  /// \param data_ptr The job information that will be appended to GCS.
  /// \param callback Callback that will be called after append done.
  /// \return Status
  Status DoAsyncAppend(const std::shared_ptr<JobTableData> &data_ptr,
                       const StatusCallback &callback);

  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_JOB_INFO_ACCESSOR_H
