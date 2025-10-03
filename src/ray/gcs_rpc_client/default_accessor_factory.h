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

#include "ray/gcs_rpc_client/accessor_factory_interface.h"

namespace ray {
namespace gcs {

/// \class DefaultAccessorFactory
/// Default implementation of the AccessorFactoryInterface.
/// Creates the standard implementation of each accessor.
class DefaultAccessorFactory : public AccessorFactoryInterface {
 public:
  DefaultAccessorFactory() = default;
  ~DefaultAccessorFactory() override = default;

  std::unique_ptr<ActorInfoAccessorInterface> CreateActorInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<JobInfoAccessorInterface> CreateJobInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<NodeInfoAccessorInterface> CreateNodeInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<NodeResourceInfoAccessorInterface> CreateNodeResourceInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<ErrorInfoAccessorInterface> CreateErrorInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<TaskInfoAccessorInterface> CreateTaskInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<WorkerInfoAccessorInterface> CreateWorkerInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<PlacementGroupInfoAccessorInterface> CreatePlacementGroupInfoAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<InternalKVAccessorInterface> CreateInternalKVAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<RuntimeEnvAccessorInterface> CreateRuntimeEnvAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<AutoscalerStateAccessorInterface> CreateAutoscalerStateAccessor(
      GcsClientContext *context) override;

  std::unique_ptr<PublisherAccessorInterface> CreatePublisherAccessor(
      GcsClientContext *context) override;
};

}  // namespace gcs
}  // namespace ray
