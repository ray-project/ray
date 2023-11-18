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

#include "absl/memory/memory.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/rpc/client_call.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace stats {

/// Main function of metric exporter is collecting indicator information from
/// opencensus data view, and sends it to the remote (for example
/// sends metrics to dashboard agents through RPC). How to use it? Register metrics
/// exporter after a main thread launched.
class MetricPointExporter final : public opencensus::stats::StatsExporter::Handler {
 public:
  explicit MetricPointExporter(
      std::shared_ptr<MetricExporterClient> metric_exporter_client,
      size_t report_batch_size = kDefaultBatchSize)
      : metric_exporter_client_(metric_exporter_client),
        report_batch_size_(report_batch_size) {}

  ~MetricPointExporter() = default;

  static void Register(std::shared_ptr<MetricExporterClient> metric_exporter_client,
                       size_t report_batch_size) {
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<MetricPointExporter>(metric_exporter_client,
                                               report_batch_size));
  }

  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                  opencensus::stats::ViewData>> &data) override;

 protected:
  void addGlobalTagsToGrpcMetric(MetricPoint &metric);

 private:
  template <class DTYPE>
  /// Extract raw data from view data, then metric exporter clients can use them
  /// in points schema.
  /// \param view_data, raw data in map
  /// \param metric_name, metric name of view data
  /// \param keys, metric tags map
  /// \param points, memory metric vector instance
  void ExportToPoints(const opencensus::stats::ViewData::DataMap<DTYPE> &view_data,
                      const opencensus::stats::MeasureDescriptor &measure_descriptor,
                      std::vector<std::string> &keys,
                      std::vector<MetricPoint> &points) {
    const auto &metric_name = measure_descriptor.name();
    for (const auto &row : view_data) {
      std::unordered_map<std::string, std::string> tags;
      for (size_t i = 0; i < keys.size(); ++i) {
        tags[keys[i]] = row.first[i];
      }
      // Current timestamp is used for point not view data time.
      MetricPoint point{metric_name,
                        current_sys_time_ms(),
                        static_cast<double>(row.second),
                        tags,
                        measure_descriptor};
      points.push_back(std::move(point));
      if (points.size() >= report_batch_size_) {
        metric_exporter_client_->ReportMetrics(points);
        points.clear();
      }
    }
  }

 private:
  std::shared_ptr<MetricExporterClient> metric_exporter_client_;
  /// Auto max minbatch size for reporting metrics to external components.
  static constexpr size_t kDefaultBatchSize = 100;
  size_t report_batch_size_;
};

class OpenCensusProtoExporter final : public opencensus::stats::StatsExporter::Handler {
 public:
  OpenCensusProtoExporter(const int port,
                          instrumented_io_context &io_service,
                          const std::string address,
                          const WorkerID &worker_id,
                          size_t report_batch_size,
                          size_t max_grpc_payload_size);

  OpenCensusProtoExporter(std::unique_ptr<rpc::MetricsAgentClient> &&agent_client,
                          const WorkerID &worker_id,
                          size_t report_batch_size,
                          size_t max_grpc_payload_size);

  ~OpenCensusProtoExporter() = default;

  static void Register(const int port,
                       instrumented_io_context &io_service,
                       const std::string address,
                       const WorkerID &worker_id,
                       size_t report_batch_size,
                       size_t max_grpc_payload_size) {
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<OpenCensusProtoExporter>(port, io_service, address, worker_id, report_batch_size, max_grpc_payload_size));
  }

  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                  opencensus::stats::ViewData>> &data) override;
  void SendData(rpc::ReportOCMetricsRequest &request);

  /// Adds data from the provided ViewDescriptor into target proto payload to be sent to an
  /// agent aggregating metrics on this node
  ///
  /// \param view_descriptor, descriptor of the metric
  /// \param view_data, data container aggregating time-series for this metric (across different set of tags)
  /// \param request_proto, target proto payload to embed metric values into
  /// \return number of time-series added to proto payload
  size_t AddMetricsData(const opencensus::stats::ViewDescriptor &view_descriptor,
                        const opencensus::stats::ViewData &view_data,
                        rpc::ReportOCMetricsRequest &request_proto);

 protected:
  void addGlobalTagsToGrpcMetric(opencensus::proto::metrics::v1::Metric &metric);

 private:
  /// Lock to protect the client
  mutable absl::Mutex mu_;
  /// Client to call a metrics agent gRPC server.
  std::unique_ptr<rpc::MetricsAgentClient> client_ ABSL_GUARDED_BY(&mu_);
  /// The worker ID of the current component.
  WorkerID worker_id_;
  /// The maximum batch size to be included in a single gRPC metrics report request.
  size_t report_batch_size_;
  /// Max gRPC payload size being sent to an agent, allowing us to make sure
  /// that batches stays w/in the threshold of the gRPC max message size set by an agent
  size_t max_grpc_payload_size_;
};

}  // namespace stats
}  // namespace ray
