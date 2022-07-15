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
#include <gtest/gtest_prod.h>

#include <boost/asio.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "nlohmann/json.hpp"
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "src/ray/protobuf/event.pb.h"

using json = nlohmann::json;

namespace ray {

#define RAY_EVENT(event_type, label)                                            \
  if (ray::RayEvent::IsLevelEnabled(                                            \
          ::ray::rpc::Event_Severity::Event_Severity_##event_type) ||           \
      ray::RayLog::IsLevelEnabled(ray::RayEvent::EventLevelToLogLevel(          \
          ::ray::rpc::Event_Severity::Event_Severity_##event_type)))            \
  ::ray::RayEvent(::ray::rpc::Event_Severity::Event_Severity_##event_type,      \
                  ray::RayEvent::EventLevelToLogLevel(                          \
                      ::ray::rpc::Event_Severity::Event_Severity_##event_type), \
                  label,                                                        \
                  __FILE__,                                                     \
                  __LINE__)

// interface of event reporter
class BaseEventReporter {
 public:
  virtual void Init() = 0;

  virtual void Report(const rpc::Event &event, const json &custom_fields) = 0;

  virtual void Close() = 0;

  virtual std::string GetReporterKey() = 0;
};
// responsible for writing event to specific file
class LogEventReporter : public BaseEventReporter {
 public:
  LogEventReporter(rpc::Event_SourceType source_type,
                   const std::string &log_dir,
                   bool force_flush = true,
                   int rotate_max_file_size = 100,
                   int rotate_max_file_num = 20);

  virtual ~LogEventReporter();

 private:
  virtual std::string replaceLineFeed(std::string message);

  virtual std::string EventToString(const rpc::Event &event, const json &custom_fields);

  virtual void Init() override {}

  virtual void Report(const rpc::Event &event, const json &custom_fields) override;

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

  // We added `const json &custom_fields` here because we need to support typed custom
  // fields.
  // TODO(SongGuyang): Remove the protobuf `rpc::Event` and use an internal struct
  // instead.
  void Publish(const rpc::Event &event, const json &custom_fields);

  // NOTE(ruoqiu) AddReporters, ClearPeporters (along with the Pushlish function) would
  // not be thread-safe. But we assume default initialization and shutdown are placed in
  // the construction and destruction of a resident class, or at the beginning and end of
  // a process.
  void AddReporter(std::shared_ptr<BaseEventReporter> reporter);

  void ClearReporters();

 private:
  EventManager();

  EventManager(const EventManager &manager) = delete;

  const EventManager &operator=(const EventManager &manager) = delete;

 private:
  absl::flat_hash_map<std::string, std::shared_ptr<BaseEventReporter>> reporter_map_;
};

// store the event context. Different workers of a process in core_worker have different
// contexts, so a singleton of thread_local needs to be maintained
class RayEventContext final {
 public:
  static RayEventContext &Instance();

  RayEventContext() {}

  void SetEventContext(
      rpc::Event_SourceType source_type,
      const absl::flat_hash_map<std::string, std::string> &custom_fields =
          absl::flat_hash_map<std::string, std::string>());

  // Only for test, isn't thread-safe with SetEventContext.
  void ResetEventContext();

  // If the key already exists, replace the value. Otherwise, insert a new item.
  void UpdateCustomField(const std::string &key, const std::string &value);

  // Update the `custom_fields` into the existing items.
  // If the key already exists, replace the value. Otherwise, insert a new item.
  void UpdateCustomFields(
      const absl::flat_hash_map<std::string, std::string> &custom_fields);

  inline void SetSourceType(rpc::Event_SourceType source_type) {
    source_type_ = source_type;
  }

  inline const rpc::Event_SourceType &GetSourceType() const { return source_type_; }

  inline const std::string &GetSourceHostname() const { return source_hostname_; }

  inline int32_t GetSourcePid() const { return source_pid_; }

  inline const absl::flat_hash_map<std::string, std::string> &GetCustomFields() const {
    return custom_fields_;
  }

  inline bool GetInitialzed() const {
    return source_type_ != rpc::Event_SourceType::Event_SourceType_COMMON;
  }

 private:
  static RayEventContext &GlobalInstance();

  RayEventContext(const RayEventContext &event_context) = delete;

  const RayEventContext &operator=(const RayEventContext &event_context) = delete;

  rpc::Event_SourceType source_type_ = rpc::Event_SourceType::Event_SourceType_COMMON;
  std::string source_hostname_ = boost::asio::ip::host_name();
  int32_t source_pid_ = getpid();
  absl::flat_hash_map<std::string, std::string> custom_fields_;

  static thread_local std::unique_ptr<RayEventContext> context_;

  // This is a global context copied from the first context created by `SetEventContext`.
  // We make this global context in order to get a basic context in other private threads.
  static std::unique_ptr<RayEventContext> global_context_;
  static std::atomic<bool> global_context_finished_setting_;
  static std::atomic<int> global_context_started_setting_;

  friend class RayEvent;
};

// when the RayEvent is deconstructed, the context information is obtained from the
// RayEventContext, then the Event structure is generated and pushed to the EventManager
// for sending
class RayEvent {
 public:
  // We require file_name to be a string which has static storage before RayEvent
  // deconstructed. Otherwise we might have memory issues.
  RayEvent(rpc::Event_Severity severity,
           RayLogLevel log_severity,
           const std::string &label,
           const char *file_name,
           int line_number)
      : severity_(severity),
        log_severity_(log_severity),
        label_(label),
        file_name_(file_name),
        line_number_(line_number) {}

  template <typename T>
  RayEvent &operator<<(const T &t) {
    osstream_ << t;
    return *this;
  }

  /// Set custom field to current event.
  /// All the supported value type: `bool`, `int`, `unsigned int`, `float`, `double`,
  /// `std::string` and `nlohmann::json`.
  template <typename T>
  RayEvent &WithField(const std::string &key, const T &value) {
    custom_fields_[key] = value;
    return *this;
  }

  static void ReportEvent(const std::string &severity,
                          const std::string &label,
                          const std::string &message,
                          const char *file_name,
                          int line_number);

  /// Return whether or not the event level is enabled in current setting.
  ///
  /// \param event_level The input event level.
  /// \return True if input event level is not lower than the threshold.
  static bool IsLevelEnabled(rpc::Event_Severity event_level);

  static RayLogLevel EventLevelToLogLevel(const rpc::Event_Severity &severity);

  ~RayEvent();

 private:
  RayEvent() = default;

  void SendMessage(const std::string &message);

  RayEvent(const RayEvent &event) = delete;

  const RayEvent &operator=(const RayEvent &event) = delete;

  // Only for test
  static void SetLevel(const std::string &event_level);

  FRIEND_TEST(EventTest, TestLogLevel);

  FRIEND_TEST(EventTest, TestLogEvent);

 private:
  rpc::Event_Severity severity_;
  RayLogLevel log_severity_;
  std::string label_;
  const char *file_name_;
  int line_number_;
  json custom_fields_;
  std::ostringstream osstream_;
};

/// Ray Event initialization.
///
/// This function should be called when the main thread starts.
/// Redundant calls in other thread don't take effect.
/// \param source_type The type of current process.
/// \param custom_fields The global custom fields.
/// \param log_dir The log directory to generate event subdirectory.
/// \param event_level The input event level. It should be one of "info","warning",
/// "error" and "fatal". You can also use capital letters for the options above.
/// \return void.
void RayEventInit(rpc::Event_SourceType source_type,
                  const absl::flat_hash_map<std::string, std::string> &custom_fields,
                  const std::string &log_dir,
                  const std::string &event_level = "warning");

}  // namespace ray
