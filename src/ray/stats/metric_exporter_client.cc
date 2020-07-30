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

#include <algorithm>

#include "ray/stats/metric_exporter_client.h"

namespace ray {
namespace stats {

///
/// Stdout Exporter
///
void StdoutExporterClient::ReportMetrics(const std::vector<MetricPoint> &points) {
  RAY_LOG(DEBUG) << "Metric point size : " << points.size();
}

///
/// Metrics Exporter Decorator
///
MetricExporterDecorator::MetricExporterDecorator(
    std::shared_ptr<MetricExporterClient> exporter)
    : exporter_(exporter) {}

void MetricExporterDecorator::ReportMetrics(const std::vector<MetricPoint> &points) {
  if (exporter_) {
    exporter_->ReportMetrics(points);
  }
}

///
/// Metrics Agent Exporter
///
MetricsAgentExporter::MetricsAgentExporter(std::shared_ptr<MetricExporterClient> exporter,
                                           const int port,
                                           boost::asio::io_service &io_service,
                                           const std::string address)
    : MetricExporterDecorator(exporter), client_call_manager_(io_service) {
  client_.reset(new rpc::MetricsAgentClient(address, port, client_call_manager_));
}

void MetricsAgentExporter::ReportMetrics(const std::vector<MetricPoint> &points) {
  MetricExporterDecorator::ReportMetrics(points);
  rpc::ReportMetricsRequest request;
  for (auto point : points) {
    auto metric_point = request.add_metrics_points();
    metric_point->set_metric_name(point.metric_name);
    metric_point->set_timestamp(point.timestamp);
    metric_point->set_value(point.value);
    auto mutable_tags = metric_point->mutable_tags();
    for (auto &tag : point.tags) {
      (*mutable_tags)[tag.first] = tag.second;
    }
    // If description and units information is requested from
    // the metrics agent, append the information.
    // TODO(sang): It can be inefficient if there are lots of new registered metrics.
    // We should make it more efficient if there's compelling use cases.
    if (should_update_description_) {
      metric_point->set_description(point.measure_descriptor.description());
      metric_point->set_units(point.measure_descriptor.units());
    }
  }
  should_update_description_ = false;

  // TODO(sang): Should retry metrics report if it fails.
  client_->ReportMetrics(
      request, [this](const Status &status, const rpc::ReportMetricsReply &reply) {
        should_update_description_ = reply.metrcs_description_required();
      });
}

}  // namespace stats
}  // namespace ray
