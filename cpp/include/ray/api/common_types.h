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
#include "ray/core.h"

namespace ray {
namespace api {

enum ErrorType : int {
  WORKER_DIED = 0,
  ACTOR_DIED = 1,
  OBJECT_UNRECONSTRUCTABLE = 2,
  TASK_EXECUTION_EXCEPTION = 3,
  OBJECT_IN_PLASMA = 4,
  TASK_CANCELLED = 5,
  ACTOR_CREATION_FAILED = 6,
};

enum WorkerType : int {
  WORKER = 0,
  DRIVER = 1,
  SPILL_WORKER = 2,
  RESTORE_WORKER = 3,
};

enum Language : int {
  PYTHON = 0,
  JAVA = 1,
  CPP = 2,
};

enum TaskType : int {
  NORMAL_TASK = 0,
  ACTOR_CREATION_TASK = 1,
  ACTOR_TASK = 2,
  DRIVER_TASK = 3,
};

struct Address {
  std::string raylet_id;
  std::string ip_address;
  int32_t port;
  // Optional unique id for the worker.
  std::string worker_id;
};

enum class ArgType { ArgByRef, ArgByValue };

/// Argument of a task.
class TaskArg {
 public:
  TaskArg() = default;
  TaskArg(ArgType arg_type) : arg_type_(arg_type) {}
  virtual ~TaskArg(){};
  ArgType GetArgType() { return arg_type_; }

  virtual const ObjectID GetObjectID() { return {}; }
  virtual const Address GetAddress() { return {}; }
  virtual std::shared_ptr<RayObject> Getvalue() { return nullptr; }

 protected:
  ArgType arg_type_;
};

class TaskArgByReference : public TaskArg {
 public:
  /// Create a pass-by-reference task argument.
  ///
  /// \param[in] object_id Id of the argument.
  /// \return The task argument.
  TaskArgByReference(const ObjectID &object_id, const Address &owner_address)
      : id_(object_id), owner_address_(owner_address) {
    arg_type_ = ArgType::ArgByRef;
  }
  const ObjectID GetObjectID() { return id_; }
  const Address GetAddress() { return owner_address_; }

 private:
  /// Id of the argument if passed by reference, otherwise nullptr.
  const ObjectID id_;
  const Address owner_address_;
};

class TaskArgByValue : public TaskArg {
 public:
  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  explicit TaskArgByValue(const std::shared_ptr<RayObject> &value) : value_(value) {
    // RAY_CHECK(value) << "Value can't be null.";
    arg_type_ = ArgType::ArgByValue;
  }

  std::shared_ptr<RayObject> Getvalue() { return value_; }

 private:
  /// Value of the argument.
  const std::shared_ptr<RayObject> value_;
};

}  // namespace api
}  // namespace ray