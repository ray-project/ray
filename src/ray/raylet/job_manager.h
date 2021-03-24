#pragma once
#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
using rpc::JobTableData;

namespace raylet {
class JobManager {
 public:
  /// Add the job table data to JobManager if the job id of job table data
  /// does not exist in the JobManager. The state of job table data should
  /// be rpc::JobTableData::SUBMITTED.
  ///
  /// \param job_table_data The job table data.
  /// \return True if the job table data is added else false.
  bool OnJobSubmitted(std::shared_ptr<JobTableData> job_table_data);

  /// Add the job table data to JobManager if the job id of job table data
  /// does not exist in the JobManager. The state of job table data should
  /// be rpc::JobTableData::RUNNING.
  ///
  /// \param job_table_data The job table data.
  /// \return True if the job table data is added or the existing job table
  /// data has a rpc::JobTableData::SUBMITTED state else false.
  bool OnJobStarted(std::shared_ptr<JobTableData> job_table_data);

  /// Erase the job table data by job id.
  ///
  /// \param job_id The job id.
  /// \return True if at least one job table data is erased else false.
  bool OnJobFailedOrFinished(const JobID &job_id);

  /// Get the job table data by job id.
  ///
  /// \param job_id The job id.
  /// \return A null shared ptr if the job table data does not exists else
  /// the job table data contains the input job id.
  std::shared_ptr<JobTableData> GetJobData(const JobID &job_id) const;

 private:
  absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>> jobs_;
};
}  // namespace raylet
}  // namespace ray
