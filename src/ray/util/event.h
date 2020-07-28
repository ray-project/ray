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
#include <boost/asio/ip/host_name.hpp>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/event.pb.h"

namespace ray {

#define RAY_EVENT(event_type, label) \
  ::ray::RayEvent(::ray::rpc::Event_Severity::Event_Severity_##event_type, label)

class LogBasedEventReporter {
 public:
  virtual void Init() = 0;

  virtual void Report(rpc::Event &event) = 0;

  virtual void Close() = 0;

  virtual std::string GetReporterKey() = 0;

 protected:
  virtual std::string EventToString(rpc::Event &event) = 0;

 protected:
  std::string file_name_;
  std::string log_dir_;
};

class EventManager final {
 public:
  static EventManager &Instance();

  bool IsEmpty();

  void Publish(rpc::Event &event);

  void AddReporter(std::shared_ptr<LogBasedEventReporter> reporter);

  void ClearReporters();

 private:
  EventManager() {}

  EventManager(const EventManager &manager) = delete;

  const EventManager &operator=(const EventManager &manager) = delete;

 protected:
  std::unordered_map<std::string, std::shared_ptr<LogBasedEventReporter>> reporter_map_;
};

class RayEventContext final {
 public:
  static RayEventContext &Instance();

  // SetEventContext should only be called once in a process, unless ResetEventContext is
  // called.
  void SetEventContext(rpc::Event_SourceType source_type,
                       std::unordered_map<std::string, std::string> custom_field =
                           std::unordered_map<std::string, std::string>());

  void SetCustomField(std::string key, std::string value);

  void SetCustomField(std::unordered_map<std::string, std::string> custom_field);

  void ResetEventContext();

  inline const rpc::Event_SourceType &GetSourceType() const { return source_type_; }

  inline const std::string &GetSourceHostname() const { return source_hostname_; }

  inline int32_t GetSourcePid() const { return source_pid_; }

  inline const std::unordered_map<std::string, std::string> &GetCustomField() const {
    return custom_field_;
  }

 private:
  RayEventContext()
      : source_type_(rpc::Event_SourceType::Event_SourceType_COMMON),
        source_hostname_(boost::asio::ip::host_name()),
        source_pid_(getpid()) {}

  RayEventContext(const RayEventContext &event_context) = delete;

  const RayEventContext &operator=(const RayEventContext &event_context) = delete;

 private:
  rpc::Event_SourceType source_type_;
  std::string source_hostname_;
  int32_t source_pid_;
  std::unordered_map<std::string, std::string> custom_field_;

  static thread_local std::unique_ptr<RayEventContext> context_;
};

class RayEvent {
 public:
  RayEvent(rpc::Event_Severity severity, std::string label)
      : severity_(severity), label_(label) {}

  template <typename T>
  RayEvent &operator<<(const T &t) {
    osstream_ << t;
    return *this;
  }

  static void ReportEvent(std::string severity, std::string label, std::string message);

  ~RayEvent();

 private:
  RayEvent() {}

  void SendMessage(const std::string &message);

  RayEvent(const RayEvent &event) = delete;

  const RayEvent &operator=(const RayEvent &event) = delete;

 private:
  rpc::Event_Severity severity_;
  std::string label_;
  std::ostringstream osstream_;
};

}  // namespace ray
