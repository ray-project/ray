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

#include <boost/optional/optional.hpp>
#include <exception>
#include <string>
#include <unordered_map>

#include "absl/synchronization/mutex.h"
#include "nlohmann/json.hpp"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/constants.h"
#include "ray/common/ray_config.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"

using json = nlohmann::json;

namespace ray {

namespace stats {

#include <boost/asio.hpp>

/// Include metric_defs.h to define measure items.
#include "ray/stats/metric_defs.h"

// TODO(sang) Put all states and logic into a singleton class Stats.
extern std::shared_ptr<IOServicePool> metrics_io_service_pool;
extern std::shared_ptr<MetricExporterClient> exporter;
extern absl::Mutex stats_mutex;

typedef std::function<void(Status status, const boost::optional<std::string> &result)>
    GetAgentAddressCallback;
typedef std::function<void(const GetAgentAddressCallback &callback)> GetAgentAddressFn;

/// Initialize stats for a process.
/// NOTE:
/// - stats::Init should be called only once per PROCESS. Redundant calls will be just
/// ignored.
/// - If you want to reinitialize, you should call stats::Shutdown().
/// - It is thread-safe.
/// We recommend you to use this only once inside a main script and add Shutdown() method
/// to any signal handler.
/// \param global_tags[in] Tags that will be appended to all metrics in this process.
/// \param exporter_to_use[in] The exporter client you will use for this process' metrics.
static inline void Init(const TagsType &global_tags, GetAgentAddressFn get_agent_address,
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
  RAY_LOG(INFO) << "Initialized stats with report interval "
                << RayConfig::instance().metrics_report_interval_ms() << "ms.";

  metrics_io_service_pool = std::make_shared<IOServicePool>(1);
  metrics_io_service_pool->Run();
  instrumented_io_context *metrics_io_service = metrics_io_service_pool->Get();
  RAY_CHECK(metrics_io_service != nullptr);

  std::shared_ptr<rpc::ClientCallManager> client_call_manager =
      std::make_shared<rpc::ClientCallManager>(*metrics_io_service);

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
  OpenCensusProtoExporter::Register(
      [get_agent_address, client_call_manager](GetMetricsAgentClientCallback callback) {
        get_agent_address([client_call_manager, callback](Status status, auto &value) {
          if (status.ok()) {
            RAY_LOG(INFO) << "Discover metrics agent addresss " << *value;
            try {
              json address = json::parse(*value);
              RAY_CHECK(address.size() == 2);
              const std::string ip = address[0].get<std::string>();
              const int port = address[1].get<int>();
              callback(status, std::make_shared<rpc::MetricsAgentClient>(
                                   ip, port, *client_call_manager));
            } catch (json::exception &ex) {
              RAY_LOG(ERROR) << "Discover metrics agent address failed: " << ex.what();
              callback(status, std::shared_ptr<rpc::MetricsAgentClient>());
            }
          } else {
            RAY_LOG(ERROR) << "Discover metrics agent address failed: " << status;
            callback(status, std::shared_ptr<rpc::MetricsAgentClient>());
          }
        });
      });
  opencensus::stats::StatsExporter::SetInterval(
      StatsConfig::instance().GetReportInterval());
  opencensus::stats::DeltaProducer::Get()->SetHarvestInterval(
      StatsConfig::instance().GetHarvestInterval());
  StatsConfig::instance().SetGlobalTags(global_tags);
  for (auto &f : StatsConfig::instance().PopInitializers()) {
    f();
  }
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
  RAY_LOG(INFO) << "Shutdown stats.";
  metrics_io_service_pool->Stop();
  opencensus::stats::DeltaProducer::Get()->Shutdown();
  opencensus::stats::StatsExporter::Shutdown();
  metrics_io_service_pool = nullptr;
  exporter = nullptr;
  StatsConfig::instance().SetIsInitialized(false);
}

}  // namespace stats

}  // namespace ray
