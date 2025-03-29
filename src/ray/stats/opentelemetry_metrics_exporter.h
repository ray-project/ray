// Copyright 2025 The Ray Authors.
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

#include <memory>
#include <string>

#include "absl/synchronization/mutex.h"

#include "ray/common/id.h"
#include "ray/util/logging.h"
#include "ray/rpc/metrics_agent_client.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/metrics/v1/metrics.pb.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"

namespace ray::stats {

class OpenTelemetryStatsExporter : public opentelemetry::sdk::metrics::PushMetricExporter {
 public:
 OpenTelemetryStatsExporter(std::shared_ptr<rpc::MetricsAgentClient> agent_client,
                              const WorkerID &worker_id,
                              opentelemetry::sdk::metrics::AggregationTemporality aggregation_temporality);

  // Export a ResourceMetrics proto to the agent.
  opentelemetry::sdk::common::ExportResult Export(const sdk::metrics::ResourceMetrics &data) noexcept override {
    // TODO(hjiang): Handle already shutdown case.
    for (auto &record : data.scope_metric_data_)
    {
      // TODO(hjiang): Real upload logic.
    }
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  opentelemetry::sdk::metrics::AggregationTemporality GetAggregationTemporality(
    opentelemetry::sdk::metrics::InstrumentType instrument_type) const noexcept override {
      return aggregation_temporality_;
  }

bool ForceFlush(
    std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

bool Shutdown(
    std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

 private:
  WorkerID worker_id_;
  absl::Mutex mu_;
  std::shared_ptr<rpc::MetricsAgentClient> client_ ABSL_GUARDED_BY(&mu_);
  opentelemetry::sdk::metrics::AggregationTemporality aggregation_temporality_;
};

}  // namespace ray::stats
