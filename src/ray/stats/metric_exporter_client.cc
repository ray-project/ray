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

void StdoutExporterClient::ReportMetrics(const MetricPoints &points) {
  RAY_LOG(DEBUG) << "Metric point size : " << points.size();
}

MetricExporterDecorator::MetricExporterDecorator(
    std::shared_ptr<MetricExporterClient> exporter)
    : exporter_(exporter) {}

void MetricExporterDecorator::ReportMetrics(const MetricPoints &points) {
  if (exporter_) {
    exporter_->ReportMetrics(points);
  }
}

GcsExporterClient::GcsExporterClient(std::shared_ptr<MetricExporterClient> exporter,
                                     rpc::GcsRpcClient &gcs_rpc_client)
    : MetricExporterDecorator(exporter), gcs_rpc_client_(gcs_rpc_client) {}

void GcsExporterClient::ReportMetrics(const MetricPoints &points) {
  // Apply upper decorators first.
  MetricExporterDecorator::ReportMetrics(points);

  rpc::StatsMetrics stats_metrics;
  for (auto &point : points) {
    auto metric = stats_metrics.add_metrics();
    metric->set_metric_name(point.metric_name);
    metric->set_timestamp(point.timestamp);
    metric->set_value(point.value);
    std::for_each(point.tags.begin(), point.tags.end(),
                  [&metric](const std::pair<std::string, std::string> &point_tag) {
                    auto tag = metric->add_tags();
                    tag->set_key(point_tag.first);
                    tag->set_value(point_tag.second);
                  });
  }
  rpc::ReportMetricsRequest report_metrics_request;
  report_metrics_request.mutable_stats_metrics()->CopyFrom(stats_metrics);
  std::promise<bool> promise;
  gcs_rpc_client_.ReportMetrics(
      report_metrics_request,
      [&promise](const Status &status, const rpc::ReportMetricsReply &reply) {
        promise.set_value(status.ok());
      });
  auto status =
      promise.get_future().wait_for(std::chrono::milliseconds(kMetricExporterRpcTimeout));
  if (status == std::future_status::ready) {
    RAY_LOG(WARNING) << "Metric export rpc request failed.";
  }
}

}  // namespace stats
}  // namespace ray
