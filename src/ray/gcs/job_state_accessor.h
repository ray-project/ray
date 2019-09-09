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

  /// Register a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be registered to GCS.
  /// \param callback Callback that will be called after job has been registered
  /// to GCS.
  /// \return Status
  Status AsyncRegister(const std::shared_ptr<JobTableData> &data_ptr,
                       const StatusCallback &callback);

  /// Update dynamic states of job in GCS asynchronously.
  ///
  /// \param data_ptr The job that will be updated to GCS.
  /// \param callback Callback that will be called after update finishes.
  /// \return Status
  Status AsyncUpdate(const std::shared_ptr<JobTableData> &data_ptr,
                     const StatusCallback &callback);

  /// Subscribe to any update operations of jobs.
  ///
  /// \param subscribe Callback that will be called each time when an job is registered
  /// or updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribeAll(const SubscribeCallback<JobID, JobTableData> &subscribe,
                           const StatusCallback &done);

 private:
  /// Register or update job information to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be registered or updated to GCS.
  /// \param callback Callback that will be called after job has been registered
  /// or updated to GCS.
  /// \return Status
  Status DoAsyncRegisterOrUpdate(const std::shared_ptr<JobTableData> &data_ptr,
                                 const StatusCallback &callback);

  RedisGcsClient &client_impl_;

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_JOB_STATE_ACCESSOR_H
