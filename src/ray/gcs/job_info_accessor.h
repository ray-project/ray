#ifndef RAY_GCS_JOB_INFO_ACCESSOR_H
#define RAY_GCS_JOB_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class JobInfoAccessor
/// `JobInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// job information in the GCS.
class JobInfoAccessor {
 public:
  virtual ~JobInfoAccessor() = default;

  /// Add a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be add to GCS.
  /// \param callback Callback that will be called after job has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Mark job as finished in GCS asynchronously.
  ///
  /// \param job_id ID of the job that will be make finished to GCS.
  /// \param callback Callback that will be called after update finished.
  /// \return Status
  virtual Status AsyncMarkFinished(const JobID &job_id,
                                   const StatusCallback &callback) = 0;

  /// Subscribe to finished jobs.
  ///
  /// \param subscribe Callback that will be called each time when a job finishes.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const StatusCallback &done) = 0;

 protected:
  JobInfoAccessor() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_JOB_INFO_ACCESSOR_H
