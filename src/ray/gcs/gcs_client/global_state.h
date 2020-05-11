// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RAY_GCS_GLOBAL_STATE_H
#define RAY_GCS_GLOBAL_STATE_H

#include "service_based_gcs_client.h"

namespace ray {
namespace gcs {

class GlobalState {
 public:
  explicit GlobalState(const std::string &redis_address,
                       const std::string &redis_password, bool is_test = false);

  Status Connect();

  void Disconnect();

  std::vector<gcs::JobTableData> GetJobTable();

 private:
  std::unique_ptr<ServiceBasedGcsClient> gcs_client_;

  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<boost::asio::io_service> io_service_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GLOBAL_STATE_H
