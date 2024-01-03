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

#include "ray/stats/metric_exporter.h"

#include <future>
#include <string_view>

namespace ray {
namespace stats {

namespace {
inline constexpr std::string_view kGrpcIoMetricsNamePrefix = "grpc.io/";
}

template <>
void MetricPointExporter::ExportToPoints(
    const opencensus::stats::ViewData::DataMap<opencensus::stats::Distribution>
        &view_data,
    const opencensus::stats::MeasureDescriptor &measure_descriptor,
    std::vector<std::string> &keys,
    std::vector<MetricPoint> &points) {
  // Return if no raw data found in view map.
  if (view_data.size() == 0) {
    return;
  }

  const auto &metric_name = measure_descriptor.name();

  // NOTE(lingxuan.zlx): No sampling in histogram data, so all points all be filled in.
  std::unordered_map<std::string, std::string> tags;
  for (size_t i = 0; i < view_data.begin()->first.size(); ++i) {
    tags[keys[i]] = view_data.begin()->first[i];
  }
  // Histogram metric will be append suffix with mean/max/min.
  double hist_mean = 0.0;
  double hist_max = 0.0;
  double hist_min = 0.0;
  bool in_first_hist_data = true;
  for (const auto &row : view_data) {
    if (in_first_hist_data) {
      hist_mean = static_cast<double>(row.second.mean());
      hist_max = static_cast<double>(row.second.max());
      hist_min = static_cast<double>(row.second.min());
      in_first_hist_data = false;
    } else {
      hist_mean += static_cast<double>(row.second.mean());
      hist_max = std::max(hist_max, static_cast<double>(row.second.max()));
      hist_min = std::min(hist_min, static_cast<double>(row.second.min()));
    }
  }
  hist_mean /= view_data.size();
  MetricPoint mean_point = {
      metric_name + ".mean", current_sys_time_ms(), hist_mean, tags, measure_descriptor};
  MetricPoint max_point = {
      metric_name + ".max", current_sys_time_ms(), hist_max, tags, measure_descriptor};
  MetricPoint min_point = {
      metric_name + ".min", current_sys_time_ms(), hist_min, tags, measure_descriptor};
  points.push_back(std::move(mean_point));
  points.push_back(std::move(max_point));
  points.push_back(std::move(min_point));

  if (points.size() >= report_batch_size_) {
    metric_exporter_client_->ReportMetrics(points);
    points.clear();
  }
}

void MetricPointExporter::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &data) {
  std::vector<MetricPoint> points;
  // NOTE(lingxuan.zlx): There is no sampling in view data, so all raw metric
  // data will be processed.
  for (const auto &datum : data) {
    auto &descriptor = datum.first;
    auto &view_data = datum.second;

    std::vector<std::string> keys;
    for (size_t i = 0; i < descriptor.columns().size(); ++i) {
      keys.push_back(descriptor.columns()[i].name());
    }
    const auto &measure_descriptor = descriptor.measure_descriptor();
    switch (view_data.type()) {
    case opencensus::stats::ViewData::Type::kDouble:
      ExportToPoints<double>(view_data.double_data(), measure_descriptor, keys, points);
      break;
    case opencensus::stats::ViewData::Type::kInt64:
      ExportToPoints<int64_t>(view_data.int_data(), measure_descriptor, keys, points);
      break;
    case opencensus::stats::ViewData::Type::kDistribution:
      ExportToPoints<opencensus::stats::Distribution>(
          view_data.distribution_data(), measure_descriptor, keys, points);
      break;
    default:
      RAY_LOG(FATAL) << "Unknown view data type.";
      break;
    }
  }
  for (auto &point : points) {
    addGlobalTagsToGrpcMetric(point);
  }
  metric_exporter_client_->ReportMetrics(points);
}

/// Hack. We want to add GlobalTags to all our metrics, but gRPC OpenCencus plugin is not
/// configurable at all so we don't have chance to add our own tags. We use this hack to
/// append the tags in export time.
void MetricPointExporter::addGlobalTagsToGrpcMetric(MetricPoint &metric) {
  if (std::string_view(metric.metric_name).substr(0, kGrpcIoMetricsNamePrefix.size()) ==
      kGrpcIoMetricsNamePrefix) {
    for (const auto &[key, value] : ray::stats::StatsConfig::instance().GetGlobalTags()) {
      metric.tags[key.name()] = value;
    }
  }
}

OpenCensusProtoExporter::OpenCensusProtoExporter(const int port,
                                                 instrumented_io_context &io_service,
                                                 const std::string address,
                                                 const WorkerID &worker_id,
                                                 size_t report_batch_size,
                                                 size_t max_grpc_payload_size)
    : OpenCensusProtoExporter(
          std::make_shared<rpc::MetricsAgentClientImpl>(address, port, io_service),
          worker_id,
          report_batch_size,
          max_grpc_payload_size) {}

OpenCensusProtoExporter::OpenCensusProtoExporter(
    std::shared_ptr<rpc::MetricsAgentClient> agent_client,
    const WorkerID &worker_id,
    size_t report_batch_size,
    size_t max_grpc_payload_size)
    : worker_id_(worker_id),
      report_batch_size_(report_batch_size),
      // To make sure we're not overflowing Agent's set gRPC max message size, we will be
      // tracking target payload binary size and make sure it stays w/in 95% of the
      // threshold
      proto_payload_size_threshold_bytes_((size_t)(max_grpc_payload_size * .95f)) {
  absl::MutexLock l(&mu_);
  client_ = std::move(agent_client);
};

/// Hack. We want to add GlobalTags to all our metrics, but gRPC OpenCencus plugin is not
/// configurable at all so we don't have chance to add our own tags. We use this hack to
/// append the tags in export time.
void OpenCensusProtoExporter::addGlobalTagsToGrpcMetric(
    opencensus::proto::metrics::v1::Metric &metric) {
  if (std::string_view(metric.metric_descriptor().name())
          .substr(0, kGrpcIoMetricsNamePrefix.size()) == kGrpcIoMetricsNamePrefix) {
    for (const auto &[key, value] : ray::stats::StatsConfig::instance().GetGlobalTags()) {
      metric.mutable_metric_descriptor()->add_label_keys()->set_key(key.name());
      for (auto &timeseries : *metric.mutable_timeseries()) {
        timeseries.add_label_values()->set_value(value);
      }
    }
  }
}

size_t OpenCensusProtoExporter::nextPayloadSizeCheckAt(size_t cur_batch_size) {
  size_t remaining = report_batch_size_ - cur_batch_size;
  return cur_batch_size + (remaining - 1) / 2;
}

void OpenCensusProtoExporter::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>> &data) {
  // Start converting opencensus data into their protobuf format.
  // The format can be found here
  // https://github.com/census-instrumentation/opencensus-proto/blob/master/src/opencensus/proto/metrics/v1/metrics.proto
  rpc::ReportOCMetricsRequest request_proto = createRequestProtoPayload();

  size_t cur_batch_size = 0;
  // NOTE: Because each payload size check is linear in the number of fields w/in the
  //       payload we intentionally sample it to happen only log(batch_size) times
  size_t next_size_check_at = nextPayloadSizeCheckAt(cur_batch_size);
  for (const auto &[descriptor, datum] : data) {
    ProcessMetricsData(
        descriptor, datum, request_proto, cur_batch_size, next_size_check_at);
  }

  if (cur_batch_size > 0) {
    SendData(request_proto);
  }
}

void OpenCensusProtoExporter::SendData(const rpc::ReportOCMetricsRequest &request) {
  RAY_LOG(DEBUG) << "Exporting metrics, metrics: " << request.metrics_size()
                 << ", payload size: " << request.ByteSizeLong();
  absl::MutexLock l(&mu_);
  client_->ReportOCMetrics(
      request, [](const Status &status, const rpc::ReportOCMetricsReply &reply) {
        RAY_UNUSED(reply);
        if (!status.ok()) {
          RAY_LOG_EVERY_N(WARNING, 10000)
              << "Export metrics to agent failed: " << status
              << ". This won't affect Ray, but you can lose metrics from the cluster.";
        }
      });
}

rpc::ReportOCMetricsRequest OpenCensusProtoExporter::createRequestProtoPayload() {
  auto request_proto = rpc::ReportOCMetricsRequest();
  request_proto.set_worker_id(worker_id_.Binary());

  return request_proto;
}

opencensus::proto::metrics::v1::Metric *addMetricProtoPayload(
    const opencensus::stats::ViewDescriptor &view_descriptor,
    rpc::ReportOCMetricsRequest &request_proto) {
  // Add metric proto object (to hold corresponding metric definition and time-series).
  auto metric_proto = request_proto.add_metrics();
  // Add metric descriptor
  auto metric_descriptor_proto = metric_proto->mutable_metric_descriptor();

  auto &measure_descriptor = view_descriptor.measure_descriptor();
  metric_descriptor_proto->set_name(measure_descriptor.name());
  metric_descriptor_proto->set_description(measure_descriptor.description());
  metric_descriptor_proto->set_unit(measure_descriptor.units());

  for (const auto &tag_key : view_descriptor.columns()) {
    metric_descriptor_proto->add_label_keys()->set_key(tag_key.name());
  };

  return metric_proto;
}

bool OpenCensusProtoExporter::handleBatchOverflows(
    const rpc::ReportOCMetricsRequest &request_proto,
    size_t cur_batch_size,
    size_t &next_payload_size_check_at) {
  bool should_check_payload_size = cur_batch_size == next_payload_size_check_at;
  /// If it exceeds the batch size, send data.
  if (cur_batch_size >= report_batch_size_) {
    SendData(request_proto);
    return true;
  } else if (should_check_payload_size) {
    size_t cur_payload_size = request_proto.ByteSizeLong();
    if (cur_payload_size >= proto_payload_size_threshold_bytes_) {
      SendData(request_proto);
      return true;
    }

    next_payload_size_check_at = nextPayloadSizeCheckAt(cur_batch_size);

    RAY_LOG(DEBUG) << "Current payload size: " << cur_payload_size
                   << " (next payload size check will be at "
                   << next_payload_size_check_at << ")";
  }

  return false;
}

void OpenCensusProtoExporter::ProcessMetricsData(
    const opencensus::stats::ViewDescriptor &view_descriptor,
    const opencensus::stats::ViewData &view_data,
    rpc::ReportOCMetricsRequest &request_proto,
    size_t &cur_batch_size,
    size_t &next_payload_size_check_at) {
  // Unpack the fields we need for in memory data structure.
  auto metric_proto_ptr = addMetricProtoPayload(view_descriptor, request_proto);

  // Helpers for writing the actual `TimeSeries`.
  auto start_time = absl::ToUnixSeconds(view_data.start_time());
  auto end_time = absl::ToUnixSeconds(view_data.end_time());
  auto make_new_data_point_proto = [this,
                                    &request_proto,
                                    &metric_proto_ptr,
                                    &cur_batch_size,
                                    &next_payload_size_check_at,
                                    view_descriptor,
                                    start_time,
                                    end_time](
                                       const std::vector<std::string> &tag_values) {
    // Prior to adding time-series to the batch, first validate whether batch still
    // has capacity or should be flushed
    bool flushed =
        handleBatchOverflows(request_proto, cur_batch_size, next_payload_size_check_at);
    if (flushed) {
      request_proto = createRequestProtoPayload();
      // NOTE: We have to also overwrite current metric_proto_ptr to point to a new Metric
      // proto
      //       payload inside new proto request payload
      metric_proto_ptr = addMetricProtoPayload(view_descriptor, request_proto);
      cur_batch_size = 0;
      next_payload_size_check_at = nextPayloadSizeCheckAt(cur_batch_size);
    }
    // Increment batch size
    cur_batch_size++;

    // Add new time-series to a proto payload
    auto metric_timeseries_proto = metric_proto_ptr->add_timeseries();

    metric_timeseries_proto->mutable_start_timestamp()->set_seconds(start_time);

    for (const auto &value : tag_values) {
      metric_timeseries_proto->add_label_values()->set_value(value);
    };

    auto point_proto = metric_timeseries_proto->add_points();
    point_proto->mutable_timestamp()->set_seconds(end_time);
    return point_proto;
  };

  // Write the `TimeSeries` for the given aggregated data type.
  switch (view_data.type()) {
  case opencensus::stats::ViewData::Type::kDouble:
    for (const auto &row : view_data.double_data()) {
      auto point_proto = make_new_data_point_proto(row.first /*tag_values*/);
      point_proto->set_double_value(row.second);
    }
    break;
  case opencensus::stats::ViewData::Type::kInt64:
    for (const auto &row : view_data.int_data()) {
      auto point_proto = make_new_data_point_proto(row.first /*tag_values*/);
      point_proto->set_int64_value(row.second);
    }
    break;
  case opencensus::stats::ViewData::Type::kDistribution:
    for (const auto &row : view_data.distribution_data()) {
      opencensus::stats::Distribution dist_value = row.second;

      auto point_proto = make_new_data_point_proto(row.first /*tag_values*/);

      // Copy in memory data into `DistributionValue` protobuf.
      auto distribution_proto = point_proto->mutable_distribution_value();
      distribution_proto->set_count(dist_value.count());
      distribution_proto->set_sum(dist_value.count() * dist_value.mean());
      distribution_proto->set_sum_of_squared_deviation(
          dist_value.sum_of_squared_deviation());

      // Write the `BucketOption` and `Bucket` data.
      auto bucket_opt_proto =
          distribution_proto->mutable_bucket_options()->mutable_explicit_();
      for (const auto &bound : dist_value.bucket_boundaries().lower_boundaries()) {
        bucket_opt_proto->add_bounds(bound);
      }
      for (const auto &count : dist_value.bucket_counts()) {
        distribution_proto->add_buckets()->set_count(count);
      }
    }
    break;
  default:
    RAY_LOG(FATAL) << "Unknown view data type.";
    break;
  }
  // NOTE: We add global tags at the end to make sure these are not overridden by
  //       the emitter
  addGlobalTagsToGrpcMetric(*metric_proto_ptr);
}

}  // namespace stats
}  // namespace ray
