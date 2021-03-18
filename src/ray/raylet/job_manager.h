#pragma once
#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
using rpc::JobTableData;

namespace raylet {
class JobManager {
 public:
  bool OnJobSubmitted(std::shared_ptr<JobTableData> job_table_data);

  bool OnJobStarted(std::shared_ptr<JobTableData> job_table_data);

  bool OnJobFailedOrFinished(const JobID &job_id);

  std::shared_ptr<JobTableData> GetJobData(const JobID &job_id) const;

  const absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>> &GetAllJobData() const;

 private:
  absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>> jobs_;
};
}  // namespace raylet
}  // namespace ray
