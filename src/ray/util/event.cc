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
///
/// LogEventReporter
///
LogEventReporter::LogEventReporter(rpc::Event_SourceType source_type,
                                   std::string &log_dir, bool force_flush,
                                   int rotate_max_file_size, int rotate_max_file_num)
    : log_dir_(log_dir),
      force_flush_(force_flush),
      rotate_max_file_size_(rotate_max_file_size),
      rotate_max_file_num_(rotate_max_file_num) {
  RAY_CHECK(log_dir_ != "");
  if (log_dir_.back() != '/') {
    log_dir_ += '/';
  }

  // generate file name, if the soucrce type is RAYLET or GCS, the file name would like
  // event_GCS.log, event_RAYLET.log other condition would like
  // event_CORE_WOREKER_{pid}.log
  file_name_ = "event_" + Event_SourceType_Name(source_type);
  if (source_type == rpc::Event_SourceType::Event_SourceType_CORE_WORKER ||
      source_type == rpc::Event_SourceType::Event_SourceType_COMMON) {
    file_name_ += "_" + std::to_string(getpid());
  }
  file_name_ += ".log";

  std::string log_sink_key = GetReporterKey() + log_dir_ + file_name_;
  log_sink_ = spdlog::get(log_sink_key);
  // If the file size is over {rotate_max_file_size_} MB, this file would be renamed
  // for example event_GCS.0.log, event_GCS.1.log, event_GCS.2.log ...
  // We alow to rotate for {rotate_max_file_num_} times.
  if (log_sink_ == nullptr) {
    log_sink_ =
        spdlog::rotating_logger_mt(log_sink_key, log_dir_ + file_name_,
                                   1048576 * rotate_max_file_size_, rotate_max_file_num_);
  }
  log_sink_->set_pattern("%v");
}

LogEventReporter::~LogEventReporter() { Flush(); }

void LogEventReporter::Flush() { log_sink_->flush(); }

std::string LogEventReporter::EventToString(const rpc::Event &event) {
  std::stringstream result;

  boost::property_tree::ptree pt;

  auto time_stamp = event.timestamp();
  std::stringstream time_stamp_buffer;
  char time_buffer[30];
  time_t epoch_time_as_time_t = time_stamp / 1000000;

  struct tm *dt = localtime(&epoch_time_as_time_t);
  strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S.", dt);

  time_stamp_buffer << std::string(time_buffer) << std::setw(6) << std::setfill('0')
                    << time_stamp % 1000000;

  pt.put("time_stamp", time_stamp_buffer.str());
  pt.put("severity", Event_Severity_Name(event.severity()));
  pt.put("label", event.label());
  pt.put("event_id", event.event_id());
  pt.put("source_type", Event_SourceType_Name(event.source_type()));
  pt.put("host_name", event.source_hostname());
  pt.put("pid", std::to_string(event.source_pid()));
  pt.put("message", event.message());

  boost::property_tree::ptree pt_child;
  for (auto &ele : event.custom_fields()) {
    pt_child.put(ele.first, ele.second);
  }

  pt.add_child("custom_fields", pt_child);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt, false);

  // the final string is like:
  // {"time_stamp":"2020-08-29 14:18:15.998084","severity":"INFO","label":"label
  // 1","event_id":"de150792ceb151c815d359d4b675fcc6266a","source_type":"CORE_WORKER","host_name":"Macbool.local","pid":"20830","message":"send
  // message 1","custom_fields":{"task_id":"task 1","job_id":"job 1","node_id":"node 1"}}

  return ss.str();
}

void LogEventReporter::Report(const rpc::Event &event) {
  RAY_CHECK(Event_SourceType_IsValid(event.source_type()));
  RAY_CHECK(Event_Severity_IsValid(event.severity()));
  std::string result = EventToString(event);
  // Pop the last character from the result string because it is breakline '\n'.
  result.pop_back();

  log_sink_->info(result);
  if (force_flush_) {
    Flush();
  }
}

///
/// EventManager
///
EventManager &EventManager::Instance() {
  static EventManager instance_;
  return instance_;
}

bool EventManager::IsEmpty() { return reporter_map_.empty(); }

void EventManager::Publish(const rpc::Event &event) {
  for (const auto &element : reporter_map_) {
    (element.second)->Report(event);
  }
}

void EventManager::AddReporter(std::shared_ptr<BaseEventReporter> reporter) {
  reporter_map_.emplace(reporter->GetReporterKey(), reporter);
}

void EventManager::ClearReporters() { reporter_map_.clear(); }
///
/// RayEventContext
///
thread_local std::unique_ptr<RayEventContext> RayEventContext::context_ = nullptr;

RayEventContext &RayEventContext::Instance() {
  if (context_ == nullptr) {
    context_ = std::unique_ptr<RayEventContext>(new RayEventContext());
  }
  return *context_;
}

void RayEventContext::SetEventContext(
    rpc::Event_SourceType source_type,
    const std::unordered_map<std::string, std::string> &custom_fields) {
  source_type_ = source_type;
  custom_fields_ = custom_fields;
}

void RayEventContext::ResetEventContext() {
  source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  custom_fields_.clear();
}

void RayEventContext::SetCustomFields(const std::string &key, const std::string &value) {
  custom_fields_[key] = value;
}

void RayEventContext::SetCustomFields(
    const std::unordered_map<std::string, std::string> &custom_fields) {
  custom_fields_ = custom_fields;
}
///
/// RayEvent
///
void RayEvent::ReportEvent(const std::string &severity, const std::string &label,
                           const std::string &message) {
  rpc::Event_Severity severity_ele =
      rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_Severity_Parse(severity, &severity_ele));
  RayEvent(severity_ele, label) << message;
}

RayEvent::~RayEvent() { SendMessage(osstream_.str()); }

void RayEvent::SendMessage(const std::string &message) {
  RAY_CHECK(rpc::Event_SourceType_IsValid(RayEventContext::Instance().GetSourceType()));
  RAY_CHECK(rpc::Event_Severity_IsValid(severity_));

  if (EventManager::Instance().IsEmpty()) {
    return;
  }

  rpc::Event event;

  std::string event_id_buffer = std::string(18, ' ');
  FillRandom(&event_id_buffer);
  event.set_event_id(StringToHex(event_id_buffer));

  event.set_source_type(RayEventContext::Instance().GetSourceType());
  event.set_source_hostname(RayEventContext::Instance().GetSourceHostname());
  event.set_source_pid(RayEventContext::Instance().GetSourcePid());

  event.set_severity(severity_);
  event.set_label(label_);
  event.set_message(message);
  event.set_timestamp(current_sys_time_us());

  auto mp = RayEventContext::Instance().GetCustomFields();
  event.mutable_custom_fields()->insert(mp.begin(), mp.end());

  EventManager::Instance().Publish(event);
}

}  // namespace ray
