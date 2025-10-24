// Copyright  The Ray Authors.
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

#include <gmock/gmock.h>

#include "ray/gcs/gcs_actor_manager.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"

namespace ray {
namespace gcs {

class MockGcsActorManager : public GcsActorManager {
 public:
  MockGcsActorManager(RuntimeEnvManager &runtime_env_manager,
                      GCSFunctionManager &function_manager,
                      rpc::RayletClientPool &raylet_client_pool,
                      rpc::CoreWorkerClientPool &worker_client_pool)
      : GcsActorManager(
            /*scheduler=*/
            nullptr,
            /*gcs_table_storage=*/nullptr,
            /*io_context=*/mock_io_context_do_not_use_,
            /*gcs_publisher=*/nullptr,
            runtime_env_manager,
            function_manager,
            [](const ActorID &) {},
            raylet_client_pool,
            worker_client_pool,
            /*ray_event_recorder=*/fake_ray_event_recorder_,
            /*session_name=*/"",
            /*actor_by_state_gauge=*/fake_actor_by_state_gauge_,
            /*gcs_actor_by_state_gauge=*/fake_gcs_actor_by_state_gauge_) {}

  MOCK_METHOD(void,
              HandleRegisterActor,
              (rpc::RegisterActorRequest request,
               rpc::RegisterActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCreateActor,
              (rpc::CreateActorRequest request,
               rpc::CreateActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetActorInfo,
              (rpc::GetActorInfoRequest request,
               rpc::GetActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetNamedActorInfo,
              (rpc::GetNamedActorInfoRequest request,
               rpc::GetNamedActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleListNamedActors,
              (rpc::ListNamedActorsRequest request,
               rpc::ListNamedActorsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllActorInfo,
              (rpc::GetAllActorInfoRequest request,
               rpc::GetAllActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleKillActorViaGcs,
              (rpc::KillActorViaGcsRequest request,
               rpc::KillActorViaGcsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));

  instrumented_io_context mock_io_context_do_not_use_;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
  observability::FakeGauge fake_actor_by_state_gauge_;
  observability::FakeGauge fake_gcs_actor_by_state_gauge_;
};

}  // namespace gcs
}  // namespace ray
