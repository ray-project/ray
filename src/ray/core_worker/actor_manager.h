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

#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "ray/core_worker/actor_handle.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

// Interface for testing.
class ActorManagerInterface {
 public:
  virtual void PublishTerminatedActor(const TaskSpecification &actor_creation_task) = 0;

  virtual ~ActorManagerInterface() {}
};

/// Class to manage lifetimes of actors that we create (actor children).
/// Currently this class is only used to publish actor DEAD event
/// for actor creation task failures. All other cases are managed
/// by raylet.
class ActorManager : public ActorManagerInterface {
 public:
  ActorManager(gcs::ActorInfoAccessor &actor_accessor)
      : actor_accessor_(actor_accessor) {}

  /// Called when an actor that we own can no longer be restarted.
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override;

 private:
  /// Global database of actors.
  gcs::ActorInfoAccessor &actor_accessor_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
