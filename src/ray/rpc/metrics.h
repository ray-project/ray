// Copyright 2025 The Ray Authors.
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

#include "ray/stats/metric.h"

namespace ray {
namespace rpc {

inline ray::stats::Histogram GetGrpcServerReqProcessTimeMsHistogramMetric() {
  return ray::stats::Histogram(
      /*name=*/"grpc_server_req_process_time_ms",
      /*description=*/"Request latency in grpc server",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{"Method"});
}

inline ray::stats::Count GetGrpcServerReqNewCounterMetric() {
  return ray::stats::Count(
      /*name=*/"grpc_server_req_new",
      /*description=*/"New request number in grpc server",
      /*unit=*/"",
      /*tag_keys=*/{"Method"});
}

inline ray::stats::Count GetGrpcServerReqHandlingCounterMetric() {
  return ray::stats::Count(
      /*name=*/"grpc_server_req_handling",
      /*description=*/"Request number are handling in grpc server",
      /*unit=*/"",
      /*tag_keys=*/{"Method"});
}

inline ray::stats::Count GetGrpcServerReqFinishedCounterMetric() {
  return ray::stats::Count(
      /*name=*/"grpc_server_req_finished",
      /*description=*/"Finished request number in grpc server",
      /*unit=*/"",
      /*tag_keys=*/{"Method"});
}

inline ray::stats::Count GetGrpcServerReqSucceededCounterMetric() {
  return ray::stats::Count(
      /*name=*/"grpc_server_req_succeeded",
      /*description=*/"Succeeded request count in grpc server",
      /*unit=*/"",
      /*tag_keys=*/{"Method"});
}

inline ray::stats::Count GetGrpcServerReqFailedCounterMetric() {
  return ray::stats::Count(
      /*name=*/"grpc_server_req_failed",
      /*description=*/"Failed request count in grpc server",
      /*unit=*/"",
      /*tag_keys=*/{"Method"});
}

inline ray::stats::Count GetGrpcClientReqFailedCounterMetric() {
  return ray::stats::Count(
      /*name=*/"grpc_client_req_failed",
      /*description=*/"Number of gRPC client failures (non-OK response statuses).",
      /*unit=*/"",
      /*tag_keys=*/{"Method"});
}

}  // namespace rpc
}  // namespace ray
