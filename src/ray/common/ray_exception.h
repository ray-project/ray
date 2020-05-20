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
#include "ray/common/id.h"
#include "ray/common/task/task_common.h"
#include "ray/protobuf/common.pb.h"

namespace ray {
/// Wrap a protobuf message.
class RayException : public MessageWrapper<rpc::RayException> {
  friend class RayErrorBuilder;

 public:
  const rpc::ErrorType ErrorType() const { return message_->error_type(); }

  // Forbid construction of this class.
 protected:
  /// Construct an empty message.
  RayException() : MessageWrapper() {}

  /// Construct from a protobuf message object.
  /// The input message will be **moved** into this object.
  ///
  /// \param message The protobuf message.
  explicit RayException(rpc::RayException &&message) : MessageWrapper(message) {}

 private:
  RayException(const RayException &);
  RayException &operator=(const RayException &);
};

class RayTaskException : public RayException {
  using RayException::RayException;

 public:
  RayTaskException(const std::string &error_message, Language language, JobID job_id,
                   WorkerID worker_id, TaskID task_id, ActorID actor_id,
                   const std::string &ip, pid_t pid, const std::string &proc_title,
                   const std::string &file, uint64_t lineno, const std::string &function,
                   const std::string &traceback, const std::string &data,
                   std::shared_ptr<rpc::RayException> cause) {
    message_->set_error_type(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
    message_->set_error_message(error_message);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_worker_id(worker_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_actor_id(actor_id.Binary());
    message_->set_ip(ip);
    message_->set_pid(pid);
    message_->set_proc_title(proc_title);
    message_->set_file(file);
    message_->set_lineno(lineno);
    message_->set_function(function);
    message_->set_traceback(traceback);
    message_->set_data(data);
    *message_->mutable_cause() = *cause.get();
  }
};

class RayActorException : public RayException {
  using RayException::RayException;

 public:
  RayActorException(const std::string &error_message, Language language, JobID job_id,
                    WorkerID worker_id, TaskID task_id, ActorID actor_id,
                    const std::string &ip, const std::string &file, uint64_t lineno,
                    const std::string &function) {
    message_->set_error_type(rpc::ErrorType::ACTOR_DIED);
    message_->set_error_message(error_message);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_worker_id(worker_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_actor_id(actor_id.Binary());
    message_->set_ip(ip);
    message_->set_file(file);
    message_->set_lineno(lineno);
    message_->set_function(function);
  }
};

class RayWorkerException : public RayException {
  using RayException::RayException;

 public:
  RayWorkerException(const std::string &error_message, Language language, JobID job_id,
                     WorkerID worker_id, TaskID task_id, ActorID actor_id,
                     const std::string &ip, pid_t pid, const std::string &file,
                     uint64_t lineno, const std::string &function) {
    message_->set_error_type(rpc::ErrorType::WORKER_DIED);
    message_->set_error_message(error_message);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_worker_id(worker_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_actor_id(actor_id.Binary());
    message_->set_ip(ip);
    message_->set_pid(pid);
    message_->set_file(file);
    message_->set_lineno(lineno);
    message_->set_function(function);
  }
};

class RayObjectException : public RayException {
  using RayException::RayException;

 public:
  RayObjectException(const std::string &error_message, Language language, JobID job_id,
                     TaskID task_id, ActorID actor_id, ObjectID object_id,
                     const std::string &file, uint64_t lineno,
                     const std::string &function) {
    message_->set_error_type(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE);
    message_->set_error_message(error_message);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_actor_id(actor_id.Binary());
    message_->set_object_id(object_id.Binary());
    message_->set_file(file);
    message_->set_lineno(lineno);
    message_->set_function(function);
  }
};

/// Helper class for building a `RayException` object.
class RayErrorBuilder {
 public:
  /// Build a ray::RayException according to input message.
  ///
  /// \return new shared pointer of ray::RayException
  static std::shared_ptr<RayException> FromProto(rpc::RayException message);

  /// Build a ray::RayException from serialized binary.
  ///
  /// \return new shared pointer of ray::RayException
  static std::shared_ptr<RayException> Deserialize(const std::string &serialized_binary);
};
}  // namespace ray
#endif
