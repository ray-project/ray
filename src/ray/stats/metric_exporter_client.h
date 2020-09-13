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

#include "ray/rpc/metrics_agent_client.h"
#include "ray/stats/metric.h"

namespace ray {
namespace stats {
/// Interface class for abstract metrics exporter client.
class MetricExporterClient {
 public:
  virtual void ReportMetrics(const std::vector<MetricPoint> &points) = 0;
  virtual ~MetricExporterClient() = default;
};

/// Default stdout exporter client can log metrics info for debug.
/// In decorator pattern, a basic concrete class is needed, so we
/// use stdout as the default concrete class.
class StdoutExporterClient : public MetricExporterClient {
 public:
  void ReportMetrics(const std::vector<MetricPoint> &points) override;
};

/// The decoration mode is that the user can apply it by configuring different
/// combinations.
/// Usage:
/// std::shared_ptr<MetricExporterClient> exporter(new StdoutExporterClient());
/// std::shared_ptr<MetricExporterClient> dashboard_exporter_client(
///         new DashboardExporterCLient(exporter, gcs_rpc_client));
///  Both dahsboard client and std logging will emit when
//  dahsboard_exporter_client->ReportMetrics(points) is called.
/// Actually, opentsdb exporter can be added like above mentioned style.
class MetricExporterDecorator : public MetricExporterClient {
 public:
  MetricExporterDecorator(std::shared_ptr<MetricExporterClient> exporter);
  virtual void ReportMetrics(const std::vector<MetricPoint> &points);

 private:
  std::shared_ptr<MetricExporterClient> exporter_;
};

class MetricsAgentExporter : public MetricExporterDecorator {
 public:
  MetricsAgentExporter(std::shared_ptr<MetricExporterClient> exporter);

  ~MetricsAgentExporter() {}

  void ReportMetrics(const std::vector<MetricPoint> &points) override;
};

}  // namespace stats
}  // namespace ray
