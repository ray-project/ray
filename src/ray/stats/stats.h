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

#include <exception>
#include <string>
#include <unordered_map>

#include "absl/synchronization/mutex.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/ray_config.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

#include <boost/asio.hpp>

/// Include metric_defs.h to define measure items.
#include "ray/stats/metric_defs.h"

// TODO(sang) Put all states and logic into a singleton class Stats.
static std::shared_ptr<IOServicePool> metrics_io_service_pool;
static std::shared_ptr<MetricExporterClient> exporter;
static absl::Mutex stats_mutex;

/// Initialize stats for a process.
/// NOTE:
/// - stats::Init should be called only once per PROCESS. Redundant calls will be just
/// ignored.
/// - If you want to reinitialize, you should call stats::Shutdown().
/// - It is thread-safe.
/// We recommend you to use this only once inside a main script and add Shutdown() method
/// to any signal handler.
/// \param global_tags[in] Tags that will be appended to all metrics in this process.
/// \param metrics_agent_port[in] The port to export metrics at each node.
/// \param exporter_to_use[in] The exporter client you will use for this process' metrics.
static inline void Init(const TagsType &global_tags, const int metrics_agent_port,
                        std::shared_ptr<MetricExporterClient> exporter_to_use = nullptr,
                        int64_t metrics_report_batch_size =
                            RayConfig::instance().metrics_report_batch_size()) {
  absl::MutexLock lock(&stats_mutex);
  if (StatsConfig::instance().IsInitialized()) {
    RAY_CHECK(metrics_io_service_pool != nullptr);
    RAY_CHECK(exporter != nullptr);
    return;
  }

  RAY_CHECK(metrics_io_service_pool == nullptr);
  RAY_CHECK(exporter == nullptr);
  bool disable_stats = !RayConfig::instance().enable_metrics_collection();
  StatsConfig::instance().SetIsDisableStats(disable_stats);
  if (disable_stats) {
    RAY_LOG(INFO) << "Disabled stats.";
    return;
  }
  RAY_LOG(DEBUG) << "Initialized stats";

  metrics_io_service_pool = std::make_shared<IOServicePool>(1);
  metrics_io_service_pool->Run();
  instrumented_io_context *metrics_io_service = metrics_io_service_pool->Get();
  RAY_CHECK(metrics_io_service != nullptr);

  // Default exporter is a metrics agent exporter.
  if (exporter_to_use == nullptr) {
    std::shared_ptr<MetricExporterClient> stdout_exporter(new StdoutExporterClient());
    exporter.reset(new MetricsAgentExporter(stdout_exporter));
  } else {
    exporter = exporter_to_use;
  }

  // Set interval.
  StatsConfig::instance().SetReportInterval(absl::Milliseconds(std::max(
      RayConfig::instance().metrics_report_interval_ms(), static_cast<uint64_t>(1000))));
  StatsConfig::instance().SetHarvestInterval(
      absl::Milliseconds(std::max(RayConfig::instance().metrics_report_interval_ms() / 2,
                                  static_cast<uint64_t>(500))));

  MetricPointExporter::Register(exporter, metrics_report_batch_size);
  OpenCensusProtoExporter::Register(metrics_agent_port, (*metrics_io_service),
                                    "127.0.0.1");
  opencensus::stats::StatsExporter::SetInterval(
      StatsConfig::instance().GetReportInterval());
  opencensus::stats::DeltaProducer::Get()->SetHarvestInterval(
      StatsConfig::instance().GetHarvestInterval());
  StatsConfig::instance().SetGlobalTags(global_tags);
  StatsConfig::instance().SetIsInitialized(true);
}

/// Shutdown the initialized stats library.
/// This cleans up various threads and metadata for stats library.
static inline void Shutdown() {
  absl::MutexLock lock(&stats_mutex);
  if (!StatsConfig::instance().IsInitialized()) {
    // Return if stats had never been initialized.
    return;
  }
  metrics_io_service_pool->Stop();
  opencensus::stats::DeltaProducer::Get()->Shutdown();
  opencensus::stats::StatsExporter::Shutdown();
  metrics_io_service_pool = nullptr;
  exporter = nullptr;
  StatsConfig::instance().SetIsInitialized(false);
}
enum StatsType : int {
  COUNT,
  SUM,
  GAUGE
  // HISTOGRAM is not supported right now
  // HISTOGRAM
};

namespace details {
template <StatsType T>
struct StatsTypeMap {
  using type = void;
};

template <>
struct StatsTypeMap<COUNT> {
  using type = Count;
};

template <>
struct StatsTypeMap<SUM> {
  using type = Sum;
};

template <>
struct StatsTypeMap<GAUGE> {
  using type = Gauge;
};

std::vector<opencensus::tags::TagKey> convertTags(const std::vector<std::string> &names) {
  std::vector<opencensus::tags::TagKey> ret;
  for (auto &n : names) {
    ret.push_back(TagKeyType::Register(n));
  }
  return ret;
}

template <std::size_t I = 0, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type TupleRecord(
    std::tuple<Tp...> &, double, const std::unordered_map<std::string, std::string> &) {}

template <std::size_t I = 0, typename... Tp>
    inline typename std::enable_if <
    I<sizeof...(Tp), void>::type TupleRecord(
        std::tuple<Tp...> &t, double val,
        const std::unordered_map<std::string, std::string> &tags) {
  std::get<I>(t).Record(val, tags);
  TupleRecord<I + 1, Tp...>(t, val, tags);
}

template <StatsType... Ts>
struct Stats {
  Stats(const std::string &name, const std::string &description, const std::string &unit,
        const std::vector<std::string> &tag_keys)
      : stats_(std::make_tuple(std::move(typename StatsTypeMap<Ts>::type(
            name, description, unit, convertTags(tag_keys)))...)),
        tag_keys_(tag_keys) {}

  Stats(const std::string &name, const std::string &description, const std::string &unit)
      : Stats(name, description, unit, std::vector<std::string>()) {}

  Stats(const std::string &name, const std::string &description, const std::string &unit,
        const std::string &tag_key)
      : Stats(name, description, unit, std::vector<std::string>({tag_key})) {}

  std::tuple<typename StatsTypeMap<Ts>::type...> stats_;
  std::vector<std::string> tag_keys_;

  template <typename T>
  void Record(T val) {
    Record<T>(val, std::unordered_map<std::string, std::string>());
  }

  template <typename T>
  void Record(T val, const std::string &tag_val) {
    RAY_CHECK(tag_keys_.size() == 1);
    Record(val, {tag_keys_[0], tag_val});
  }

  template <typename T>
  void Record(T val, const std::unordered_map<std::string, std::string> &tags) {
    TupleRecord(stats_, static_cast<double>(val), tags);
  }
};
}  // namespace details

}  // namespace stats

}  // namespace ray

#define DEFINE_stats(name, unit, description, tags, types...) \
  static ray::stats::details::Stats<types> STATS_##name(#name, unit, tags, description)

#define DECLARE_stats(name) extern ray::stats::details::Stats<types> STATS_ name
