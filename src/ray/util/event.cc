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

#include <filesystem>

#include "absl/base/call_once.h"
#include "absl/time/time.h"

namespace ray {
///
/// LogEventReporter
///
LogEventReporter::LogEventReporter(rpc::Event_SourceType source_type,
                                   const std::string &log_dir,
                                   bool force_flush,
                                   int rotate_max_file_size,
                                   int rotate_max_file_num)
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
    log_sink_ = spdlog::rotating_logger_mt(log_sink_key,
                                           log_dir_ + file_name_,
                                           1048576 * rotate_max_file_size_,
                                           rotate_max_file_num_);
  }
  log_sink_->set_pattern("%v");
}

LogEventReporter::~LogEventReporter() { Flush(); }

void LogEventReporter::Flush() { log_sink_->flush(); }

std::string LogEventReporter::replaceLineFeed(std::string message) {
  std::stringstream ss;
  // If the message has \n or \r, we will replace with \\n
  for (int i = 0, len = message.size(); i < len; ++i) {
    if (message[i] == '\n' || message[i] == '\r') {
      ss << "\\n";
    } else {
      ss << message[i];
    }
  }
  return ss.str();
}

std::string LogEventReporter::EventToString(const rpc::Event &event,
                                            const json &custom_fields) {
  json j;

  auto time_stamp = event.timestamp();
  time_t epoch_time_as_time_t = time_stamp / 1000000;

  absl::Time absl_time = absl::FromTimeT(epoch_time_as_time_t);
  std::stringstream time_stamp_buffer;
  time_stamp_buffer << absl::FormatTime(
                           "%Y-%m-%d %H:%M:%S.", absl_time, absl::LocalTimeZone())
                    << std::setw(6) << std::setfill('0') << time_stamp % 1000000;

  j["time_stamp"] = time_stamp_buffer.str();
  j["severity"] = Event_Severity_Name(event.severity());
  j["label"] = event.label();
  j["event_id"] = event.event_id();
  j["source_type"] = Event_SourceType_Name(event.source_type());
  j["host_name"] = event.source_hostname();
  j["pid"] = std::to_string(event.source_pid());
  // Make sure the serialized json is one line in the event log.
  j["message"] = replaceLineFeed(event.message());
  j["custom_fields"] = custom_fields;

  // the final string is like:
  // {"time_stamp":"2020-08-29 14:18:15.998084","severity":"INFO","label":"label
  // 1","event_id":"de150792ceb151c815d359d4b675fcc6266a","source_type":"CORE_WORKER","host_name":"Macbool.local","pid":"20830","message":"send
  // message 1","custom_fields":{"task_id":"task 1","job_id":"job 1","node_id":"node 1"}}

  return j.dump();
}

void LogEventReporter::Report(const rpc::Event &event, const json &custom_fields) {
  RAY_CHECK(Event_SourceType_IsValid(event.source_type()));
  RAY_CHECK(Event_Severity_IsValid(event.severity()));
  std::string result = EventToString(event, custom_fields);

  log_sink_->info(result);
  if (force_flush_) {
    Flush();
  }
}

///
/// EventManager
///
EventManager::EventManager() {
  RayLog::AddFatalLogCallbacks({[](const std::string &label, const std::string &content) {
    RayEvent::ReportEvent("FATAL", label, content, __FILE__, __LINE__);
  }});
}

EventManager &EventManager::Instance() {
  static EventManager instance_;
  return instance_;
}

bool EventManager::IsEmpty() { return reporter_map_.empty(); }

void EventManager::Publish(const rpc::Event &event, const json &custom_fields) {
  for (const auto &element : reporter_map_) {
    (element.second)->Report(event, custom_fields);
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

std::unique_ptr<RayEventContext> RayEventContext::global_context_ = nullptr;

std::atomic<int> RayEventContext::global_context_started_setting_(0);

std::atomic<bool> RayEventContext::global_context_finished_setting_(false);

RayEventContext &RayEventContext::Instance() {
  if (context_ == nullptr) {
    context_ = std::unique_ptr<RayEventContext>(new RayEventContext());
  }
  return *context_;
}

RayEventContext &RayEventContext::GlobalInstance() {
  if (global_context_finished_setting_ == false) {
    static RayEventContext tmp_instance_;
    return tmp_instance_;
  }
  return *global_context_;
}

void RayEventContext::SetEventContext(
    rpc::Event_SourceType source_type,
    const absl::flat_hash_map<std::string, std::string> &custom_fields) {
  SetSourceType(source_type);
  UpdateCustomFields(custom_fields);

  if (!global_context_started_setting_.fetch_or(1)) {
    global_context_ = std::make_unique<RayEventContext>();
    global_context_->SetSourceType(source_type);
    global_context_->UpdateCustomFields(custom_fields);
    global_context_finished_setting_ = true;
  }
}

void RayEventContext::ResetEventContext() {
  source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  custom_fields_.clear();
  global_context_started_setting_ = 0;
  global_context_finished_setting_ = false;
}

void RayEventContext::UpdateCustomField(const std::string &key,
                                        const std::string &value) {
  // This method should be used while source type has been set.
  RAY_CHECK(GetInitialzed());
  custom_fields_[key] = value;
}

void RayEventContext::UpdateCustomFields(
    const absl::flat_hash_map<std::string, std::string> &custom_fields) {
  // This method should be used while source type has been set.
  RAY_CHECK(GetInitialzed());
  for (const auto &pair : custom_fields) {
    custom_fields_[pair.first] = pair.second;
  }
}
///
/// RayEvent
///
static rpc::Event_Severity severity_threshold_ = rpc::Event_Severity::Event_Severity_INFO;

static void SetEventLevel(const std::string &event_level) {
  std::string level = event_level;
  std::transform(level.begin(), level.end(), level.begin(), ::tolower);
  if (level == "info") {
    severity_threshold_ = rpc::Event_Severity::Event_Severity_INFO;
  } else if (level == "warning") {
    severity_threshold_ = rpc::Event_Severity::Event_Severity_WARNING;
  } else if (level == "error") {
    severity_threshold_ = rpc::Event_Severity::Event_Severity_ERROR;
  } else if (level == "fatal") {
    severity_threshold_ = rpc::Event_Severity::Event_Severity_FATAL;
  } else {
    RAY_LOG(WARNING) << "Unrecognized setting of event level " << level;
  }
  RAY_LOG(INFO) << "Set ray event level to " << level;
}

void RayEvent::ReportEvent(const std::string &severity,
                           const std::string &label,
                           const std::string &message,
                           const char *file_name,
                           int line_number) {
  rpc::Event_Severity severity_ele =
      rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_Severity_Parse(severity, &severity_ele));
  RayEvent(
      severity_ele, EventLevelToLogLevel(severity_ele), label, file_name, line_number)
      << message;
}

bool RayEvent::IsLevelEnabled(rpc::Event_Severity event_level) {
  return event_level >= severity_threshold_;
}

void RayEvent::SetLevel(const std::string &event_level) { SetEventLevel(event_level); }

RayLogLevel RayEvent::EventLevelToLogLevel(const rpc::Event_Severity &severity) {
  switch (severity) {
  case rpc::Event_Severity_INFO:
    return RayLogLevel::INFO;
  case rpc::Event_Severity_WARNING:
    return RayLogLevel::WARNING;
  case rpc::Event_Severity_ERROR:
  // Converts fatal events to error logs because fatal logs will lead to process exiting
  // directly.
  case rpc::Event_Severity_FATAL:
    return RayLogLevel::ERROR;
  default:
    RAY_LOG(ERROR) << "Can't cast severity " << severity;
  }
  return RayLogLevel::INFO;
}

RayEvent::~RayEvent() { SendMessage(osstream_.str()); }

void RayEvent::SendMessage(const std::string &message) {
  RAY_CHECK(rpc::Event_SourceType_IsValid(RayEventContext::Instance().GetSourceType()));
  RAY_CHECK(rpc::Event_Severity_IsValid(severity_));

  if (EventManager::Instance().IsEmpty()) {
    return;
  }

  const RayEventContext &context = RayEventContext::Instance().GetInitialzed()
                                       ? RayEventContext::Instance()
                                       : RayEventContext::GlobalInstance();

  static const int kEventIDSize = 18;
  static const std::string kEmptyEventIdHex = "disabled";
  std::string event_id;
  if (IsLevelEnabled(severity_)) {
    std::string event_id_buffer = std::string(kEventIDSize, ' ');
    FillRandom(&event_id_buffer);
    event_id = StringToHex(event_id_buffer);
    rpc::Event event;
    event.set_event_id(event_id);

    event.set_source_type(context.GetSourceType());
    event.set_source_hostname(context.GetSourceHostname());
    event.set_source_pid(context.GetSourcePid());

    event.set_severity(severity_);
    event.set_label(label_);
    event.set_message(message);
    event.set_timestamp(current_sys_time_us());

    auto mp = context.GetCustomFields();
    for (const auto &pair : mp) {
      custom_fields_[pair.first] = pair.second;
    }
    event.mutable_custom_fields()->insert(mp.begin(), mp.end());

    EventManager::Instance().Publish(event, custom_fields_);
  } else {
    event_id = kEmptyEventIdHex;
  }
  if (ray::RayLog::IsLevelEnabled(log_severity_)) {
    ::ray::RayLog(file_name_, line_number_, log_severity_)
        << "[ Event " << event_id << " " << custom_fields_.dump() << " ] " << message;
  }
}

static absl::once_flag init_once_;

void RayEventInit(rpc::Event_SourceType source_type,
                  const absl::flat_hash_map<std::string, std::string> &custom_fields,
                  const std::string &log_dir,
                  const std::string &event_level) {
  absl::call_once(init_once_, [&source_type, &custom_fields, &log_dir, &event_level]() {
    RayEventContext::Instance().SetEventContext(source_type, custom_fields);
    auto event_dir = std::filesystem::path(log_dir) / std::filesystem::path("events");
    ray::EventManager::Instance().AddReporter(
        std::make_shared<ray::LogEventReporter>(source_type, event_dir.string()));
    SetEventLevel(event_level);
    RAY_LOG(INFO) << "Ray Event initialized for " << Event_SourceType_Name(source_type);
  });
}

}  // namespace ray
