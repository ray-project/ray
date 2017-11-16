// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "ray/gcs/client.h"

#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

AsyncGCSClient::AsyncGCSClient() {}

AsyncGCSClient::~AsyncGCSClient() {}

Status AsyncGCSClient::Connect(const std::string& address, int port) {
  context_.reset(new RedisContext());
  RETURN_NOT_OK(context_->Connect(address, port));
  object_table_.reset(new ObjectTable(context_));
  task_table_.reset(new TaskTable(context_));
  return Status::OK();
}

Status Attach(plasma::EventLoop& event_loop) {

  return Status::OK();
}

ObjectTable& AsyncGCSClient::object_table() {
  return *object_table_;
}

TaskTable& AsyncGCSClient::task_table() {
  return *task_table_;
}

FunctionTable& AsyncGCSClient::function_table() {
  return *function_table_;
}

ClassTable& AsyncGCSClient::class_table() {
  return *class_table_;
}

}  // namespace gcs

}  // namespace ray
