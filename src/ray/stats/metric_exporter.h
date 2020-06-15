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

#ifndef RAY_METRIC_EXPORTER_H
#define RAY_METRIC_EXPORTER_H
#include "absl/memory/memory.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"

#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace stats {

/// Main function of metric exporter is collecting indicator information from
/// opencensus data view, and finally sends it to the remote ( for example
/// send metrics to gcs service through rpc. How to use it? Register metrics
/// exporter after raylet main thread launched. Actually its exporter
///  has never beed used in raylet, we will enable it later.
class MetricExporter final : public opencensus::stats::StatsExporter::Handler {
 public:
  explicit MetricExporter(std::shared_ptr<MetricExporterClient> metric_exporter_client)
      : metric_exporter_client_(metric_exporter_client) {}
  ~MetricExporter() = default;
  static void Register(std::shared_ptr<MetricExporterClient> metric_exporter_client) {
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<MetricExporter>(metric_exporter_client));
  }

  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                  opencensus::stats::ViewData>> &data) override;

 private:
  template <class DTYPE>
  void ExportToPoints(const opencensus::stats::ViewData::DataMap<DTYPE> &view_data,
                      const std::string &metric_name, std::vector<std::string> &keys,
                      MetricPoints &points) {
    for (const auto &row : view_data) {
      std::unordered_map<std::string, std::string> tags;
      for (size_t i = 0; i < keys.size(); ++i) {
        tags[keys[i]] = row.first[i];
      }
      // Current timestamp is used for point not view data time.
      MetricPoint point{.metric_name = metric_name,
                        //.timestamp = absl::ToUnixMillis(view_data.end_time()),
                        .timestamp = current_sys_time_ms(),
                        .value = static_cast<double>(row.second),
                        .tags = tags};
      RAY_LOG(DEBUG) << "Metric name " << metric_name << ", value " << point.value;
      points.push_back(std::move(point));
      if (points.size() >= kMaxBatchSize) {
        RAY_LOG(DEBUG) << "Point size : " << points.size();
        metric_exporter_client_->ReportMetrics(points);
        points.clear();
      }
    }
  }

 private:
  std::shared_ptr<MetricExporterClient> metric_exporter_client_;
  static constexpr size_t kMaxBatchSize = 100;
};

}  // namespace stats
}  // namespace ray

#endif
