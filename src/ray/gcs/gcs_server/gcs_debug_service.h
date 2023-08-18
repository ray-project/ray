// Copyright 2023 The Ray Authors.
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

#include <grpcpp/grpcpp.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace gcs {

class GcsDebugService : public ray::rpc::DebugService::CallbackService {
 public:
  GcsDebugService(instrumented_io_context &io_service);
  /// Collect memory stats.
  grpc::ServerUnaryReactor *CollectMemoryStats(
      grpc::CallbackServerContext *context,
      const ray::rpc::CollectMemoryStatsRequest *request,
      ray::rpc::CollectMemoryStatsReply *reply) override;

  grpc::ServerUnaryReactor *StartMemoryProfile(
      grpc::CallbackServerContext *context,
      const ray::rpc::StartMemoryProfileRequest *request,
      ray::rpc::StartMemoryProfileReply *reply) override;

  grpc::ServerUnaryReactor *StopMemoryProfile(
      grpc::CallbackServerContext *context,
      const ray::rpc::StopMemoryProfileRequest *request,
      ray::rpc::StopMemoryProfileReply *reply) override;

 private:
  instrumented_io_context &io_service_;
};

}  // namespace gcs
}  // namespace ray
