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
#include <boost/asio.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "src/ray/protobuf/event.pb.h"

namespace ray {

#define RAY_EVENT(event_type, label) \
  ::ray::RayEvent(::ray::rpc::Event_Severity::Event_Severity_##event_type, label)

// interface of event reporter
class BaseEventReporter {
 public:
  virtual void Init() = 0;

  virtual void Report(const rpc::Event &event) = 0;

  virtual void Close() = 0;

  virtual std::string GetReporterKey() = 0;
};
// responsible for writing event to specific file
class LogEventReporter : public BaseEventReporter {
 public:
  LogEventReporter(rpc::Event_SourceType source_type, std::string &log_dir,
                   bool force_flush = true, int rotate_max_file_size = 100,
                   int rotate_max_file_num = 20);

  virtual ~LogEventReporter();

 private:
  virtual std::string EventToString(const rpc::Event &event);

  virtual void Init() override {}

  virtual void Report(const rpc::Event &event) override;

  virtual void Close() override {}

  virtual void Flush();

  virtual std::string GetReporterKey() override { return "log.event.reporter"; }

 protected:
  std::string log_dir_;
  bool force_flush_;
  int rotate_max_file_size_;  // MB
  int rotate_max_file_num_;

  std::string file_name_;

  std::shared_ptr<spdlog::logger> log_sink_;
};

// store the reporters, add reporters and clean reporters
class EventManager final {
 public:
  static EventManager &Instance();

  bool IsEmpty();

  void Publish(const rpc::Event &event);

  // NOTE(ruoqiu) AddReporters, ClearPeporters (along with the Pushlish function) would
  // not be thread-safe. But we assume default initialization and shutdown are placed in
  // the construction and destruction of a resident class, or at the beginning and end of
  // a process.
  void AddReporter(std::shared_ptr<BaseEventReporter> reporter);

  void ClearReporters();

 private:
  EventManager() = default;

  EventManager(const EventManager &manager) = delete;

  const EventManager &operator=(const EventManager &manager) = delete;

 private:
  std::unordered_map<std::string, std::shared_ptr<BaseEventReporter>> reporter_map_;
};

// store the event context. Different workers of a process in core_worker have different
// contexts, so a singleton of thread_local needs to be maintained
class RayEventContext final {
 public:
  static RayEventContext &Instance();

  void SetEventContext(rpc::Event_SourceType source_type,
                       const std::unordered_map<std::string, std::string> &custom_fields =
                           std::unordered_map<std::string, std::string>());

  void SetCustomFields(const std::string &key, const std::string &value);

  void SetCustomFields(const std::unordered_map<std::string, std::string> &custom_fields);

  void ResetEventContext();

  inline const rpc::Event_SourceType &GetSourceType() const { return source_type_; }

  inline const std::string &GetSourceHostname() const { return source_hostname_; }

  inline int32_t GetSourcePid() const { return source_pid_; }

  inline const std::unordered_map<std::string, std::string> &GetCustomFields() const {
    return custom_fields_;
  }

 private:
  RayEventContext() {}

  RayEventContext(const RayEventContext &event_context) = delete;

  const RayEventContext &operator=(const RayEventContext &event_context) = delete;

 private:
  rpc::Event_SourceType source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  std::string source_hostname_ = boost::asio::ip::host_name();
  int32_t source_pid_ = getpid();
  std::unordered_map<std::string, std::string> custom_fields_;

  static thread_local std::unique_ptr<RayEventContext> context_;
};

// when the RayEvent is deconstructed, the context information is obtained from the
// RayEventContext, then the Event structure is generated and pushed to the EventManager
// for sending
class RayEvent {
 public:
  RayEvent(rpc::Event_Severity severity, const std::string &label)
      : severity_(severity), label_(label) {}

  template <typename T>
  RayEvent &operator<<(const T &t) {
    osstream_ << t;
    return *this;
  }

  static void ReportEvent(const std::string &severity, const std::string &label,
                          const std::string &message);

  ~RayEvent();

 private:
  RayEvent() = default;

  void SendMessage(const std::string &message);

  RayEvent(const RayEvent &event) = delete;

  const RayEvent &operator=(const RayEvent &event) = delete;

 private:
  rpc::Event_Severity severity_;
  std::string label_;
  std::ostringstream osstream_;
};

}  // namespace ray
