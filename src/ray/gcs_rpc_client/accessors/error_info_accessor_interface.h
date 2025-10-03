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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// \class ErrorInfoAccessorInterface
/// Interface for ErrorInfo operations.
class ErrorInfoAccessorInterface {
 public:
  virtual ~ErrorInfoAccessorInterface() = default;

  /// Report a job error to GCS asynchronously.
  /// The error message will be pushed to the driver of a specific if it is
  /// a job internal error, or broadcast to all drivers if it is a system error.
  ///
  /// TODO(rkn): We need to make sure that the errors are unique because
  /// duplicate messages currently cause failures (the GCS doesn't allow it). A
  /// natural way to do this is to have finer-grained time stamps.
  ///
  /// \param data The error message that will be reported to GCS.
  virtual void AsyncReportJobError(rpc::ErrorTableData data) = 0;
};

}  // namespace gcs
}  // namespace ray
