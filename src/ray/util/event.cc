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
