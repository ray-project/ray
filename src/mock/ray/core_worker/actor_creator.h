// Copyright 2021 The Ray Authors.
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

#include "gmock/gmock.h"
namespace ray {
namespace core {

class MockActorCreatorInterface : public ActorCreatorInterface {
 public:
  MOCK_METHOD(Status,
              RegisterActor,
              (const TaskSpecification &task_spec),
              (const, override));
  MOCK_METHOD(Status,
              AsyncRegisterActor,
              (const TaskSpecification &task_spec, gcs::StatusCallback callback),
              (override));
  MOCK_METHOD(Status,
              AsyncCreateActor,
              (const TaskSpecification &task_spec,
               const rpc::ClientCallback<rpc::CreateActorReply> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncWaitForActorRegisterFinish,
              (const ActorID &actor_id, gcs::StatusCallback callback),
              (override));
  MOCK_METHOD(bool, IsActorInRegistering, (const ActorID &actor_id), (const, override));
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockDefaultActorCreator : public DefaultActorCreator {
 public:
  MOCK_METHOD(Status,
              RegisterActor,
              (const TaskSpecification &task_spec),
              (const, override));
  MOCK_METHOD(Status,
              AsyncRegisterActor,
              (const TaskSpecification &task_spec, gcs::StatusCallback callback),
              (override));
  MOCK_METHOD(bool, IsActorInRegistering, (const ActorID &actor_id), (const, override));
  MOCK_METHOD(Status,
              AsyncCreateActor,
              (const TaskSpecification &task_spec,
               const rpc::ClientCallback<rpc::CreateActorReply> &callback),
              (override));
};

}  // namespace core
}  // namespace ray
