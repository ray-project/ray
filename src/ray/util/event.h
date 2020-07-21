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
#ifndef RAY_EVENT_H_
#define RAY_EVENT_H_

#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>
#include <boost/asio/ip/host_name.hpp>
#include <boost/filesystem.hpp>
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/event.pb.h"

namespace ray {

#define RAY_EVENT(event_type, label) \
  ::ray::RayEvent(::ray::rpc::Event_Severity::Event_Severity_##event_type, label)

class LogBasedEventReporter {
 public:
  LogBasedEventReporter(rpc::Event_SourceType source_type, std::string log_dir);
  
  virtual void Init() = 0;

  virtual void Report(rpc::Event &event) = 0;

  virtual void Close() = 0;

  virtual std::string GetReporterKey() = 0;

 protected:
  virtual std::string EventToString(rpc::Event &event);
 
 protected:
  const std::string separator_ = "|||";
  std::string file_name_;
  std::string log_dir_; 
};

class EventManager {
 public:
  static EventManager &Instance() {
    static EventManager INSTANCE;
    return INSTANCE;
  }

  bool IsEmpty() { return reporter_map_.empty(); }

  void Publish(rpc::Event &event) {
    for (const auto &element : reporter_map_) {
      (element.second)->Report(event);
    }
  }

  void AddReporter(std::shared_ptr<LogBasedEventReporter> reporter) {
    reporter_map_.emplace(reporter->GetReporterKey(), reporter);
  }

  void ClearReporters() { reporter_map_.clear(); }

 private:
  EventManager() {}

  EventManager(const EventManager &manager) = delete;

  const EventManager &operator=(const EventManager &manager) = delete;

 protected:
  std::unordered_map<std::string, std::shared_ptr<LogBasedEventReporter>> reporter_map_;
};

class RayEventContext {
 public:
  static RayEventContext &Instance() {
    if (instance_ == nullptr) {
      instance_ = std::unique_ptr<RayEventContext>(new RayEventContext());
    }
    return *instance_;
  }

  // SetEventContext should only be called once in a process, unless ResetEventContext is
  // called.
  void SetEventContext(rpc::Event_SourceType source_type, std::string node_id = "",
                       std::string job_id = "", std::string task_id = "");

  void ResetEventContext();

  inline void SetJobID(std::string job_id) { job_id_ = job_id; }

  inline void SetNodeID(std::string node_id) { node_id_ = node_id; }

  inline void SetTaskID(std::string task_id) { task_id_ = task_id; }

  inline std::string GetJobID() { return job_id_; }

  inline std::string GetNodeID() { return node_id_; }

  inline std::string GetTaskID() { return task_id_; }

  inline rpc::Event_SourceType GetSourceType() { return source_type_; }

  inline std::string GetSourceHostname() { return source_hostname_; }

  inline int32_t GetSourcePid() { return source_pid_; }

 private:
  RayEventContext() {}

  RayEventContext(const RayEventContext &event_context) = delete;

  const RayEventContext &operator=(const RayEventContext &manaevent_contextger) = delete;

 private:
  std::string job_id_ = "";
  std::string node_id_ = "";
  std::string task_id_ = "";
  rpc::Event_SourceType source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  std::string source_hostname_ = boost::asio::ip::host_name();
  int32_t source_pid_ = getpid();

  static thread_local std::unique_ptr<RayEventContext> instance_;
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

  static void ReportEvent(std::string severity, std::string label, std::string message) {
    rpc::Event_Severity severity_ele =
        rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
    RAY_CHECK(rpc::Event_Severity_Parse(severity, &severity_ele));
    RayEvent(severity_ele, label) << message;
  }

  ~RayEvent() { SendMessage(osstream_.str()); }

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

#endif