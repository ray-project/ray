#ifndef RAY_GCS_JOB_STATE_ACCESSOR_H
#define RAY_GCS_JOB_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class JobStateAccessor
/// JobStateAccessor class encapsulates the implementation details of
/// reading or writing or subscribing of job's information (immutable fields which
/// determined at submission time, and mutable fields which determined at runtime).
class JobStateAccessor {
 public:
  explicit JobStateAccessor(RedisGcsClient &client_impl);

  ~JobStateAccessor() {}

  /// Add a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be add to GCS.
  /// \param callback Callback that will be called after job has been registered
  /// to GCS.
  /// \return Status
  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback);

  /// Mark job as finished in GCS asynchronously.
  ///
  /// \param job_id ID of the job that will be make finished to GCS.
  /// \param callback Callback that will be called after update finishes.
  /// \return Status
  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback);

  /// Subscribe to finished jobs.
  ///
  /// \param subscribe Callback that will be called each time when a job finished.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, JobTableData> &subscribe,
      const StatusCallback &done);

 private:
  /// Append job information to GCS asynchronously.
  ///
  /// \param data_ptr The job information that will be appended to GCS.
  /// \param callback Callback that will be called after append done.
  /// \return Status
  Status DoAsyncAppend(const std::shared_ptr<JobTableData> &data_ptr,
                       const StatusCallback &callback);

  RedisGcsClient &client_impl_;

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_JOB_STATE_ACCESSOR_H
