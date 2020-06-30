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

#include <string>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"

namespace ray {

using WorkerType = rpc::WorkerType;

// Return a string representation of the worker type.
std::string WorkerTypeString(WorkerType type);

// Return a string representation of the language.
std::string LanguageString(Language language);

/// Information about a remote function.
class RayFunction {
 public:
  RayFunction() {}
  RayFunction(Language language, const ray::FunctionDescriptor &function_descriptor)
      : language_(language), function_descriptor_(function_descriptor) {}

  Language GetLanguage() const { return language_; }

  const ray::FunctionDescriptor &GetFunctionDescriptor() const {
    return function_descriptor_;
  }

 private:
  Language language_;
  ray::FunctionDescriptor function_descriptor_;
};

/// Argument of a task.
class TaskArg {
 public:
  virtual void ToProto(rpc::TaskArg *arg_proto) const = 0;
  virtual ~TaskArg(){};
};

class TaskArgByReference : public TaskArg {
 public:
  /// Create a pass-by-reference task argument.
  ///
  /// \param[in] object_id Id of the argument.
  /// \return The task argument.
  TaskArgByReference(const ObjectID &object_id, const rpc::Address &owner_address)
      : id_(object_id), owner_address_(owner_address) {}

  void ToProto(rpc::TaskArg *arg_proto) const {
    auto ref = arg_proto->mutable_object_ref();
    ref->set_object_id(id_.Binary());
    ref->mutable_owner_address()->CopyFrom(owner_address_);
  }

 private:
  /// Id of the argument if passed by reference, otherwise nullptr.
  const ObjectID id_;
  const rpc::Address owner_address_;
};

class TaskArgByValue : public TaskArg {
 public:
  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  TaskArgByValue(const std::shared_ptr<RayObject> &value) : value_(value) {
    RAY_CHECK(value) << "Value can't be null.";
  }

  void ToProto(rpc::TaskArg *arg_proto) const {
    if (value_->HasData()) {
      const auto &data = value_->GetData();
      arg_proto->set_data(data->Data(), data->Size());
    }
    if (value_->HasMetadata()) {
      const auto &metadata = value_->GetMetadata();
      arg_proto->set_metadata(metadata->Data(), metadata->Size());
    }
    for (const auto &nested_id : value_->GetNestedIds()) {
      arg_proto->add_nested_inlined_ids(nested_id.Binary());
    }
  }

 private:
  /// Value of the argument.
  const std::shared_ptr<RayObject> value_;
};

/// Options for all tasks (actor and non-actor) except for actor creation.
struct TaskOptions {
  TaskOptions() {}
  TaskOptions(int num_returns, std::unordered_map<std::string, double> &resources)
      : num_returns(num_returns), resources(resources) {}

  /// Number of returns of this task.
  int num_returns = 1;
  /// Resources required by this task.
  std::unordered_map<std::string, double> resources;
};

/// Options for actor creation tasks.
struct ActorCreationOptions {
  ActorCreationOptions() {}
  ActorCreationOptions(int64_t max_restarts, int64_t max_task_retries,
                       int max_concurrency,
                       const std::unordered_map<std::string, double> &resources,
                       const std::unordered_map<std::string, double> &placement_resources,
                       const std::vector<std::string> &dynamic_worker_options,
                       bool is_detached, std::string &name, bool is_asyncio)
      : max_restarts(max_restarts),
        max_task_retries(max_task_retries),
        max_concurrency(max_concurrency),
        resources(resources),
        placement_resources(placement_resources),
        dynamic_worker_options(dynamic_worker_options),
        is_detached(is_detached),
        name(name),
        is_asyncio(is_asyncio){};

  /// Maximum number of times that the actor should be restarted if it dies
  /// unexpectedly. A value of -1 indicates infinite restarts. If it's 0, the
  /// actor won't be restarted.
  const int64_t max_restarts = 0;
  /// Maximum number of times that individual tasks can be retried at the
  /// actor, if the actor dies unexpectedly. If -1, then the task may be
  /// retried infinitely many times.
  const int64_t max_task_retries = 0;
  /// The max number of concurrent tasks to run on this direct call actor.
  const int max_concurrency = 1;
  /// Resources required by the whole lifetime of this actor.
  const std::unordered_map<std::string, double> resources;
  /// Resources required to place this actor.
  const std::unordered_map<std::string, double> placement_resources;
  /// The dynamic options used in the worker command when starting a worker process for
  /// an actor creation task.
  const std::vector<std::string> dynamic_worker_options;
  /// Whether to keep the actor persistent after driver exit. If true, this will set
  /// the worker to not be destroyed after the driver shutdown.
  const bool is_detached = false;
  /// The name to give this detached actor that can be used to get a handle to it from
  /// other drivers. This must be globally unique across the cluster.
  /// This should set if and only if is_detached is true.
  const std::string name;
  /// Whether to use async mode of direct actor call.
  const bool is_asyncio = false;
};

}  // namespace ray
