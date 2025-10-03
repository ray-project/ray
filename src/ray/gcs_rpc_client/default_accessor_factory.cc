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

#include "ray/gcs_rpc_client/default_accessor_factory.h"

#include "ray/gcs_rpc_client/accessors/actor_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/autoscaler_state_accessor.h"
#include "ray/gcs_rpc_client/accessors/error_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/internal_kv_accessor.h"
#include "ray/gcs_rpc_client/accessors/job_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/node_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/node_resource_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/placement_group_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/publisher_accessor.h"
#include "ray/gcs_rpc_client/accessors/runtime_env_accessor.h"
#include "ray/gcs_rpc_client/accessors/task_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/worker_info_accessor.h"

namespace ray {
namespace gcs {

std::unique_ptr<ActorInfoAccessorInterface>
DefaultAccessorFactory::CreateActorInfoAccessor(GcsClientContext *context) {
  return std::make_unique<ActorInfoAccessor>(context);
}

std::unique_ptr<JobInfoAccessorInterface> DefaultAccessorFactory::CreateJobInfoAccessor(
    GcsClientContext *context) {
  // For now, return the concrete implementation from accessor.h
  // These will be replaced with the new split implementations as they are created
  return std::make_unique<JobInfoAccessor>(context);
}

std::unique_ptr<NodeInfoAccessorInterface> DefaultAccessorFactory::CreateNodeInfoAccessor(
    GcsClientContext *context) {
  return std::make_unique<NodeInfoAccessor>(context);
}

std::unique_ptr<NodeResourceInfoAccessorInterface>
DefaultAccessorFactory::CreateNodeResourceInfoAccessor(GcsClientContext *context) {
  return std::make_unique<NodeResourceInfoAccessor>(context);
}

std::unique_ptr<ErrorInfoAccessorInterface>
DefaultAccessorFactory::CreateErrorInfoAccessor(GcsClientContext *context) {
  return std::make_unique<ErrorInfoAccessor>(context);
}

std::unique_ptr<TaskInfoAccessorInterface> DefaultAccessorFactory::CreateTaskInfoAccessor(
    GcsClientContext *context) {
  return std::make_unique<TaskInfoAccessor>(context);
}

std::unique_ptr<WorkerInfoAccessorInterface>
DefaultAccessorFactory::CreateWorkerInfoAccessor(GcsClientContext *context) {
  return std::make_unique<WorkerInfoAccessor>(context);
}

std::unique_ptr<PlacementGroupInfoAccessorInterface>
DefaultAccessorFactory::CreatePlacementGroupInfoAccessor(GcsClientContext *context) {
  return std::make_unique<PlacementGroupInfoAccessor>(context);
}

std::unique_ptr<InternalKVAccessorInterface>
DefaultAccessorFactory::CreateInternalKVAccessor(GcsClientContext *context) {
  return std::make_unique<InternalKVAccessor>(context);
}

std::unique_ptr<RuntimeEnvAccessorInterface>
DefaultAccessorFactory::CreateRuntimeEnvAccessor(GcsClientContext *context) {
  return std::make_unique<RuntimeEnvAccessor>(context);
}

std::unique_ptr<AutoscalerStateAccessorInterface>
DefaultAccessorFactory::CreateAutoscalerStateAccessor(GcsClientContext *context) {
  return std::make_unique<AutoscalerStateAccessor>(context);
}

std::unique_ptr<PublisherAccessorInterface>
DefaultAccessorFactory::CreatePublisherAccessor(GcsClientContext *context) {
  return std::make_unique<PublisherAccessor>(context);
}

}  // namespace gcs
}  // namespace ray
