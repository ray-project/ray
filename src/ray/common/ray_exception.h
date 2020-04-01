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

#ifndef RAY_CORE_WORKER_EXCEPTION_H
#define RAY_CORE_WORKER_EXCEPTION_H

#include <string>

#include "ray/common/grpc_util.h"
#include "ray/protobuf/common.pb.h"

namespace ray {
/// Wrap a protobuf message.
class RayError {
 public:
  /// Construct an empty message.
  explicit RayError() : ray_exception_(std::make_shared<rpc::RayException>()) {}

  /// Construct from protobuf-serialized binary.
  ///
  /// \param serialized_binary Protobuf-serialized binary.
  explicit RayError(const std::string &serialized_binary)
      : ray_exception_(std::make_shared<rpc::RayException>()) {
    ray_exception_->ParseFromString(serialized_binary);
  }

  virtual ~RayError(){};

  const rpc::ErrorType Type() const { return ray_exception_->error_type(); }

  /// Serialize the message to a string.
  const std::string Serialize() const { return ray_exception_->SerializeAsString(); }

 protected:
  std::shared_ptr<rpc::RayException> ray_exception_;
};

class RayTaskError : public RayError {
 public:
  RayTaskError(const std::string &error_message, Language language, JobID job_id,
               WorkerID worker_id, TaskID task_id, ActorID actor_id,
               const std::string &ip, pid_t pid, const std::string &proc_title,
               const std::string &file, uint64_t lineno, const std::string &function,
               const std::string &traceback, const std::string &data,
               std::shared_ptr<rpc::RayException> cause) {
    ray_exception_->set_error_type(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
    ray_exception_->set_error_message(error_message);
    ray_exception_->set_language(language);
    ray_exception_->set_job_id(job_id.Binary());
    ray_exception_->set_worker_id(worker_id.Binary());
    ray_exception_->set_task_id(task_id.Binary());
    ray_exception_->set_actor_id(actor_id.Binary());
    ray_exception_->set_ip(ip);
    ray_exception_->set_pid(pid);
    ray_exception_->set_proc_title(proc_title);
    ray_exception_->set_file(file);
    ray_exception_->set_lineno(lineno);
    ray_exception_->set_function(function);
    ray_exception_->set_traceback(traceback);
    ray_exception_->set_data(data);
    *ray_exception_->mutable_cause() = *cause.get();
  }
};

class RayActorError : public RayError {
 public:
  RayActorError(const std::string &error_message, Language language, JobID job_id,
                WorkerID worker_id, TaskID task_id, ActorID actor_id,
                const std::string &ip, const std::string &file, uint64_t lineno,
                const std::string &function) {
    ray_exception_->set_error_type(rpc::ErrorType::ACTOR_DIED);
    ray_exception_->set_error_message(error_message);
    ray_exception_->set_language(language);
    ray_exception_->set_job_id(job_id.Binary());
    ray_exception_->set_worker_id(worker_id.Binary());
    ray_exception_->set_task_id(task_id.Binary());
    ray_exception_->set_actor_id(actor_id.Binary());
    ray_exception_->set_ip(ip);
    ray_exception_->set_file(file);
    ray_exception_->set_lineno(lineno);
    ray_exception_->set_function(function);
  }
};

class RayWorkerError : public RayError {
 public:
  RayWorkerError(const std::string &error_message, Language language, JobID job_id,
                 WorkerID worker_id, TaskID task_id, ActorID actor_id,
                 const std::string &ip, pid_t pid, const std::string &file,
                 uint64_t lineno, const std::string &function) {
    ray_exception_->set_error_type(rpc::ErrorType::WORKER_DIED);
    ray_exception_->set_error_message(error_message);
    ray_exception_->set_language(language);
    ray_exception_->set_job_id(job_id.Binary());
    ray_exception_->set_worker_id(worker_id.Binary());
    ray_exception_->set_task_id(task_id.Binary());
    ray_exception_->set_actor_id(actor_id.Binary());
    ray_exception_->set_ip(ip);
    ray_exception_->set_pid(pid);
    ray_exception_->set_file(file);
    ray_exception_->set_lineno(lineno);
    ray_exception_->set_function(function);
  }
};

class RayObjectError : public RayError {
 public:
  RayObjectError(const std::string &error_message, Language language, JobID job_id,
                 TaskID task_id, ActorID actor_id, ObjectID object_id,
                 const std::string &file, uint64_t lineno, const std::string &function) {
    ray_exception_->set_error_type(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE);
    ray_exception_->set_error_message(error_message);
    ray_exception_->set_language(language);
    ray_exception_->set_job_id(job_id.Binary());
    ray_exception_->set_task_id(task_id.Binary());
    ray_exception_->set_actor_id(actor_id.Binary());
    ray_exception_->set_object_id(object_id.Binary());
    ray_exception_->set_file(file);
    ray_exception_->set_lineno(lineno);
    ray_exception_->set_function(function);
  }
};
}  // namespace ray
#endif
