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

#include "ray/common/buffer.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/task/task_common.h"
#include "ray/protobuf/common.pb.h"

namespace ray {
/// Wrap a protobuf message.
class RayException : public MessageWrapper<rpc::RayException> {
  friend class RayErrorBuilder;

 public:
  RayException(rpc::ErrorType error_type, const std::string &error_message,
               Language language, JobID job_id, WorkerID worker_id, TaskID task_id,
               ActorID actor_id, ObjectID object_id, const std::string &ip, pid_t pid,
               const std::string &proc_title, const std::string &file, uint64_t lineno,
               const std::string &function, const std::string &traceback,
               const std::string &data, std::shared_ptr<rpc::RayException> cause) {
    message_->set_error_type(error_type);
    message_->set_error_message(error_message);
    message_->set_language(language);
    message_->set_job_id(job_id.Binary());
    message_->set_worker_id(worker_id.Binary());
    message_->set_task_id(task_id.Binary());
    message_->set_actor_id(actor_id.Binary());
    message_->set_actor_id(object_id.Binary());
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

  const rpc::ErrorType ErrorType() const { return message_->error_type(); }

  std::shared_ptr<Buffer> GetBuffer() const {
    auto serialized = message_->SerializeAsString();
    return std::make_shared<LocalMemoryBuffer>(static_cast<uint8_t *>(serialized.c_str()),
                                               serialized.size(),
                                               /*copy_data=*/true);
  }

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

}  // namespace ray
#endif
