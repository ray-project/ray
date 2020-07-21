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

#include "ray/util/event.h"

namespace ray {
thread_local std::unique_ptr<RayEventContext> RayEventContext::instance_ = nullptr;

LogBasedEventReporter::LogBasedEventReporter(rpc::Event_SourceType source_type, std::string log_dir)
    : log_dir_(log_dir) {
  RAY_CHECK(log_dir_ != "");
  if (log_dir_.back() != '/') {
    log_dir_ += '/';
  }
  boost::filesystem::create_directories(log_dir_);
  file_name_ = "event_" + Event_SourceType_Name(source_type);
  if (source_type == rpc::Event_SourceType::Event_SourceType_CORE_WORKER ||
      source_type == rpc::Event_SourceType::Event_SourceType_COMMON) {
    file_name_ += "_" + std::to_string(getpid());
  }
}

std::string LogBasedEventReporter::EventToString(rpc::Event &event) {
  std::stringstream result;

  struct tm *dt;
  char time_buffer[30];
  time_t epoch_time_as_time_t = event.timestamp() / 1000;

  dt = localtime(&epoch_time_as_time_t);
  strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S.", dt);

  result << std::string(time_buffer) << std::setw(5) << std::setfill('0')
         << event.timestamp() % 1000 << separator_ << event.event_id() + separator_
         << event.job_id() + separator_ << event.node_id() + separator_
         << event.task_id() + separator_
         << Event_SourceType_Name(event.source_type()) + separator_
         << event.source_hostname() + separator_
         << std::to_string(event.source_pid()) + separator_
         << Event_Severity_Name(event.severity()) + separator_
         << event.label() + separator_ << event.message();
  return result.str();
}

void RayEventContext::SetEventContext(rpc::Event_SourceType source_type,
                                      std::string node_id, std::string job_id,
                                      std::string task_id) {
  source_type_ = source_type;
  node_id_ = node_id;
  job_id_ = job_id;
  task_id_ = task_id;
}

void RayEventContext::ResetEventContext() {
  source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  node_id_ = "";
  job_id_ = "";
  task_id_ = "";
}

void RayEvent::SendMessage(const std::string &message) {
  RAY_CHECK(rpc::Event_SourceType_IsValid(RayEventContext::Instance().GetSourceType()));
  RAY_CHECK(rpc::Event_Severity_IsValid(severity_));

  if (EventManager::Instance().IsEmpty()) {
    return;
  }

  rpc::Event event;

  std::string event_id_buffer = std::string(18, ' ');
  FillRandom(&event_id_buffer);
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (size_t i = 0; i < event_id_buffer.size(); i++) {
    unsigned char val = event_id_buffer[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  event.set_event_id(result);

  event.set_node_id(RayEventContext::Instance().GetNodeID());
  event.set_job_id(RayEventContext::Instance().GetJobID());
  event.set_task_id(RayEventContext::Instance().GetTaskID());

  event.set_source_type(RayEventContext::Instance().GetSourceType());
  event.set_source_hostname(RayEventContext::Instance().GetSourceHostname());
  event.set_source_pid(RayEventContext::Instance().GetSourcePid());

  event.set_severity(severity_);
  event.set_label(label_);
  event.set_message(message);
  event.set_timestamp(current_sys_time_ms());

  EventManager::Instance().Publish(event);
}

}  // namespace ray