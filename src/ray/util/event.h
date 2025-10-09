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
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest_prod.h>

#include <boost/asio.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "nlohmann/json.hpp"
#include "ray/util/logging.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "src/ray/protobuf/event.pb.h"
#include "src/ray/protobuf/export_event.pb.h"

namespace ray {

// RAY_EVENT_EVERY_N/RAY_EVENT_EVERY_MS, adaped from
// https://github.com/google/glog/blob/master/src/glog/logging.h.in
#define RAY_EVENT_EVERY_N_VARNAME(base, line) RAY_EVENT_EVERY_N_VARNAME_CONCAT(base, line)
#define RAY_EVENT_EVERY_N_VARNAME_CONCAT(base, line) base##line

/// Macros for RAY_EVENT_EVERY_MS
#define RAY_EVENT_TIME_PERIOD RAY_EVENT_EVERY_N_VARNAME(timePeriod_, __LINE__)
#define RAY_EVENT_PREVIOUS_TIME_RAW RAY_EVENT_EVERY_N_VARNAME(previousTimeRaw_, __LINE__)
#define RAY_EVENT_TIME_DELTA RAY_EVENT_EVERY_N_VARNAME(deltaTime_, __LINE__)
#define RAY_EVENT_CURRENT_TIME RAY_EVENT_EVERY_N_VARNAME(currentTime_, __LINE__)
#define RAY_EVENT_PREVIOUS_TIME RAY_EVENT_EVERY_N_VARNAME(previousTime_, __LINE__)

#define RAY_EVENT_EVERY_MS(event_type, label, ms)                                     \
  constexpr std::chrono::milliseconds RAY_EVENT_TIME_PERIOD(ms);                      \
  static std::atomic<int64_t> RAY_EVENT_PREVIOUS_TIME_RAW;                            \
  const auto RAY_EVENT_CURRENT_TIME =                                                 \
      std::chrono::steady_clock::now().time_since_epoch();                            \
  const decltype(RAY_EVENT_CURRENT_TIME) RAY_EVENT_PREVIOUS_TIME(                     \
      RAY_EVENT_PREVIOUS_TIME_RAW.load(std::memory_order_relaxed));                   \
  const auto RAY_EVENT_TIME_DELTA = RAY_EVENT_CURRENT_TIME - RAY_EVENT_PREVIOUS_TIME; \
  if (RAY_EVENT_TIME_DELTA > RAY_EVENT_TIME_PERIOD)                                   \
    RAY_EVENT_PREVIOUS_TIME_RAW.store(RAY_EVENT_CURRENT_TIME.count(),                 \
                                      std::memory_order_relaxed);                     \
  if (ray::RayEvent::IsLevelEnabled(                                                  \
          ::ray::rpc::Event_Severity::Event_Severity_##event_type) &&                 \
      RAY_EVENT_TIME_DELTA > RAY_EVENT_TIME_PERIOD)                                   \
  RAY_EVENT(event_type, label)

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
  virtual ~BaseEventReporter() = default;

  virtual void Init() = 0;

  virtual void Report(const rpc::Event &event, const nlohmann::json &custom_fields) = 0;

  virtual void ReportExportEvent(const rpc::ExportEvent &export_event) = 0;

  virtual void Close() = 0;

  virtual std::string GetReporterKey() = 0;
};
// responsible for writing event to specific file
using SourceTypeVariant =
    std::variant<rpc::Event_SourceType, rpc::ExportEvent_SourceType>;
class LogEventReporter : public BaseEventReporter {
 public:
  LogEventReporter(SourceTypeVariant source_type,
                   const std::string &log_dir,
                   bool force_flush = true,
                   int rotate_max_file_size = 100,
                   int rotate_max_file_num = 20);

  ~LogEventReporter() override;

  void Report(const rpc::Event &event, const nlohmann::json &custom_fields) override;

  void ReportExportEvent(const rpc::ExportEvent &export_event) override;

 private:
  virtual std::string replaceLineFeed(std::string message);

  virtual std::string EventToString(const rpc::Event &event,
                                    const nlohmann::json &custom_fields);

  virtual std::string ExportEventToString(const rpc::ExportEvent &export_event);

  void Init() override {}

  void Close() override {}

  virtual void Flush();

  std::string GetReporterKey() override { return "log.event.reporter"; }

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

  EventManager(const EventManager &manager) = delete;

  const EventManager &operator=(const EventManager &manager) = delete;

  ~EventManager() = default;

  bool IsEmpty() const;

  // We added `const json &custom_fields` here because we need to support typed custom
  // fields.
  // TODO(SongGuyang): Remove the protobuf `rpc::Event` and use an internal struct
  // instead.
  void Publish(const rpc::Event &event, const nlohmann::json &custom_fields);

  void PublishExportEvent(const rpc::ExportEvent &export_event);

  // NOTE(ruoqiu) AddReporters, ClearPeporters (along with the Pushlish function) would
  // not be thread-safe. But we assume default initialization and shutdown are placed in
  // the construction and destruction of a resident class, or at the beginning and end of
  // a process.
  void AddReporter(std::shared_ptr<BaseEventReporter> reporter);

  void AddExportReporter(rpc::ExportEvent_SourceType source_type,
                         std::shared_ptr<LogEventReporter> reporter);

  void ClearReporters();

 private:
  EventManager();

  absl::flat_hash_map<std::string, std::shared_ptr<BaseEventReporter>> reporter_map_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<rpc::ExportEvent_SourceType, std::shared_ptr<LogEventReporter>>
      export_log_reporter_map_ ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

// store the event context. Different workers of a process in core_worker have different
// contexts, so a singleton of thread_local needs to be maintained
class RayEventContext final {
 public:
  static RayEventContext &Instance();

  RayEventContext() = default;

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

  void SetSourceType(rpc::Event_SourceType source_type) { source_type_ = source_type; }

  const rpc::Event_SourceType &GetSourceType() const { return source_type_; }

  const std::string &GetSourceHostname() const { return source_hostname_; }

  int32_t GetSourcePid() const { return source_pid_; }

  const absl::flat_hash_map<std::string, std::string> &GetCustomFields() const {
    return custom_fields_;
  }

  bool GetInitialzed() const {
    return source_type_ != rpc::Event_SourceType::Event_SourceType_COMMON;
  }

  RayEventContext(const RayEventContext &event_context) = delete;

  const RayEventContext &operator=(const RayEventContext &event_context) = delete;

  ~RayEventContext() = default;

 private:
  static RayEventContext &GlobalInstance();

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
           std::string label,
           const char *file_name,
           int line_number)
      : severity_(severity),
        log_severity_(log_severity),
        label_(std::move(label)),
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

  /// Return whether or not the event should be logged to a log file.
  ///
  /// \return True if event should be logged.
  static bool EmitToLogFile();

  static RayLogLevel EventLevelToLogLevel(const rpc::Event_Severity &severity);

  ~RayEvent();

  RayEvent(const RayEvent &event) = delete;

  const RayEvent &operator=(const RayEvent &event) = delete;

 private:
  RayEvent() = default;

  void SendMessage(const std::string &message);

  // Only for test
  static void SetLevel(const std::string &event_level);
  // Only for test
  static void SetEmitToLogFile(bool emit_to_log_file);

  FRIEND_TEST(EventTest, TestLogLevel);

  FRIEND_TEST(EventTest, TestLogEvent);

 private:
  rpc::Event_Severity severity_;
  RayLogLevel log_severity_;
  std::string label_;
  const char *file_name_;
  int line_number_;
  nlohmann::json custom_fields_;
  std::ostringstream osstream_;
};

using ExportEventDataPtr = std::variant<std::shared_ptr<rpc::ExportTaskEventData>,
                                        std::shared_ptr<rpc::ExportNodeData>,
                                        std::shared_ptr<rpc::ExportActorData>,
                                        std::shared_ptr<rpc::ExportDriverJobEventData>>;
class RayExportEvent {
 public:
  explicit RayExportEvent(ExportEventDataPtr event_data_ptr)
      : event_data_ptr_(std::move(event_data_ptr)) {}

  ~RayExportEvent();

  void SendEvent();

  RayExportEvent(const RayExportEvent &event) = delete;

  const RayExportEvent &operator=(const RayExportEvent &event) = delete;

 private:
  ExportEventDataPtr event_data_ptr_;
};

bool IsExportAPIEnabledSourceType(
    std::string_view source_type,
    bool enable_export_api_write_global,
    const std::vector<std::string> &enable_export_api_write_config_str);

/// Ray Event initialization.
///
/// This function should be called when the main thread starts.
/// Redundant calls in other thread don't take effect.
/// \param source_types List of source types the current process can create events for. If
/// there are multiple rpc::Event_SourceType source_types, the last one will be used as
/// the source type for the RAY_EVENT macro.
/// \param custom_fields The global custom fields. These are only set for
/// rpc::Event_SourceType events.
/// \param log_dir The log directory to generate event subdirectory.
/// \param event_level The input event level. It should be one of "info","warning",
/// "error" and "fatal". You can also use capital letters for the options above.
/// \param emit_event_to_log_file if True, it will emit the event to the process log file
/// (e.g., gcs_server.out). Otherwise, event will only be recorded to the event log file.
void RayEventInit(const std::vector<SourceTypeVariant> &source_types,
                  const absl::flat_hash_map<std::string, std::string> &custom_fields,
                  const std::string &log_dir,
                  const std::string &event_level = "warning",
                  bool emit_event_to_log_file = false);

/// Logic called by RayEventInit. This function can be called multiple times,
/// and has been separated out so RayEventInit can be called multiple times in
/// tests.
/// **Note**: This should only be called from tests.
void RayEventInit_(const std::vector<SourceTypeVariant> &source_types,
                   const absl::flat_hash_map<std::string, std::string> &custom_fields,
                   const std::string &log_dir,
                   const std::string &event_level,
                   bool emit_event_to_log_file);

}  // namespace ray
