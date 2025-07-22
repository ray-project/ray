// Copyright 2024 The Ray Authors.
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

// TODO(hjiang): Move all functions from `pb_utils.h` to this implementation file.
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/strings/str_format.h"
#include "ray/gcs/pb_util.h"

namespace ray::gcs {

std::shared_ptr<ray::rpc::ErrorTableData> CreateErrorTableData(
    const std::string &error_type,
    const std::string &error_msg,
    absl::Time timestamp,
    const JobID &job_id) {
  uint32_t max_error_msg_size_bytes = RayConfig::instance().max_error_msg_size_bytes();
  auto error_info_ptr = std::make_shared<ray::rpc::ErrorTableData>();
  error_info_ptr->set_type(error_type);
  if (error_msg.length() > max_error_msg_size_bytes) {
    std::string formatted_error_message = absl::StrFormat(
        "The message size exceeds %d bytes. Find the full log from the log files. Here "
        "is abstract: %s",
        max_error_msg_size_bytes,
        std::string_view{error_msg}.substr(0, max_error_msg_size_bytes));
    error_info_ptr->set_error_message(std::move(formatted_error_message));
  } else {
    error_info_ptr->set_error_message(error_msg);
  }
  error_info_ptr->set_timestamp(absl::ToUnixMillis(timestamp));
  error_info_ptr->set_job_id(job_id.Binary());
  return error_info_ptr;
}

}  // namespace ray::gcs
