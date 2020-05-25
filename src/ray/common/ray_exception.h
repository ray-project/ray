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

#include <sstream>
#include <string>

#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_common.h"
#include "ray/protobuf/common.pb.h"

namespace ray {
/// Wrap a protobuf message.
class RayException : public MessageWrapper<rpc::RayException> {
 public:
  RayException(rpc::ErrorType error_type, const std::string &error_message,
               Language language, JobID job_id, WorkerID worker_id, TaskID task_id,
               ActorID actor_id, ObjectID object_id, const std::string &ip, pid_t pid,
               const std::string &proc_title, const std::string &file, uint64_t lineno,
               const std::string &function, const std::string &traceback,
               const std::string &data, std::shared_ptr<RayException> cause) {
    message_->set_error_type(error_type);
    message_->set_error_message(error_message);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_worker_id(worker_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_actor_id(actor_id.Binary());
    message_->set_object_id(object_id.Binary());
    message_->set_ip(ip);
    message_->set_pid(pid);
    message_->set_proc_title(proc_title);
    message_->set_file(file);
    message_->set_lineno(lineno);
    message_->set_function(function);
    message_->set_traceback(traceback);
    message_->set_data(data);
    if (cause) {
      *message_->mutable_cause() = *cause->message_;
    }
  }

  /// Construct from protobuf-serialized binary.
  ///
  /// \param serialized_binary Protobuf-serialized binary.
  explicit RayException(const std::string &serialized_binary)
      : MessageWrapper<rpc::RayException>(serialized_binary) {}

  const rpc::ErrorType ErrorType() const { return message_->error_type(); }

  std::string ErrorMessage() const { return message_->error_message(); }

  Language Language() const { return message_->language(); }

  JobID JobId() const { return JobID::FromBinary(message_->job_id()); }

  WorkerID WorkerId() const { return WorkerID::FromBinary(message_->worker_id()); }

  TaskID TaskId() const { return TaskID::FromBinary(message_->task_id()); }

  ActorID ActorId() const { return ActorID::FromBinary(message_->actor_id()); }

  ObjectID ObjectId() const { return ObjectID::FromBinary(message_->object_id()); }

  std::string Ip() const { return message_->ip(); }

  pid_t Pid() const { return static_cast<pid_t>(message_->pid()); }

  std::string ProcTitle() const { return message_->proc_title(); }

  std::string File() const { return message_->file(); }

  uint64_t LineNo() const { return message_->lineno(); }

  std::string Function() const { return message_->function(); }

  std::string Traceback() const { return message_->traceback(); }

  std::string Data() const { return message_->data(); }

  std::shared_ptr<RayException> Cause() const {
    if (message_->has_cause()) {
      return std::make_shared<RayException>(RayException(message_->cause()));
    } else {
      return nullptr;
    }
  }

  RayObject ToRayObject() const {
    auto serialized = message_->SerializeAsString();
    auto data =
        const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(serialized.data()));
    auto data_buffer =
        std::make_shared<LocalMemoryBuffer>(data, serialized.size(), /*copy_data=*/true);
    auto error_type = message_->error_type();
    std::string meta = std::to_string(static_cast<int>(error_type));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer =
        std::make_shared<LocalMemoryBuffer>(metadata, meta.size(), /*copy_data=*/true);
    return RayObject(data_buffer, meta_buffer, {});
  }

  std::string ToString() const {
    std::string from = "";
    if (!message_->ip().empty()) {
      std::stringstream ss;
      ss << message_->ip();
      if (message_->pid()) {
        ss << "(pid=" << message_->pid() << ")";
      }
      from = ss.str();
    }

    std::string at = "";
    if (!message_->file().empty()) {
      std::stringstream ss;
      ss << "[" << message_->file() << ":" << message_->lineno();
      if (message_->function().size()) {
        ss << " in function " << message_->function();
      }
      ss << "]";
      at = ss.str();
    }

    std::string details = "";
    {
      std::stringstream ss;
      ss << "  Languge: " << Language_Name(message_->language()) << "\n"
         << "  Job ID: " << JobID::FromBinary(message_->job_id()).Hex() << "\n"
         << "  Worker ID: " << WorkerID::FromBinary(message_->worker_id()).Hex() << "\n"
         << "  Task ID: " << TaskID::FromBinary(message_->task_id()).Hex() << "\n"
         << "  Actor ID: " << ActorID::FromBinary(message_->actor_id()).Hex() << "\n"
         << "  Object ID: " << ObjectID::FromBinary(message_->object_id()).Hex() << "\n";
      details = ss.str();
    }

    std::stringstream result;
    result << ErrorType_Name(message_->error_type());
    if (!from.empty()) {
      result << " from " << from;
    }
    if (!at.empty()) {
      result << " " << at;
    }
    result << ":";
    if (!message_->error_message().empty()) {
      result << "\n\n" << message_->error_message() << "\n";
    }
    if (!details.empty()) {
      result << "\n" << details;
    }
    if (!message_->traceback().empty()) {
      result << "\n" << message_->traceback();
    }
    return result.str();
  }

 private:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit RayException(const rpc::RayException &message)
      : MessageWrapper<rpc::RayException>(std::make_shared<rpc::RayException>(message)) {}
};

}  // namespace ray
#endif
