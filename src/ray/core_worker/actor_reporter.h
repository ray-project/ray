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

#include "ray/gcs/redis_gcs_client.h"

namespace ray {

// TODO(SANG): This class won't be needed once GCS actor mangement becomes the default.
// Interface for testing.
class ActorReporterInterface {
 public:
  virtual void PublishTerminatedActor(const TaskSpecification &actor_creation_task) = 0;

  virtual ~ActorReporterInterface() {}
};

class ActorReporter : public ActorReporterInterface {
 public:
  ActorReporter(std::shared_ptr<gcs::GcsClient> gcs_client) : gcs_client_(gcs_client) {}

  ~ActorReporter() {}

  /// Called when an actor that we own can no longer be restarted.
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override;

 private:
  /// GCS client
  std::shared_ptr<gcs::GcsClient> gcs_client_;
};

}  // namespace ray
