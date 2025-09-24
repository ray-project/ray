// Copyright 2023 The Ray Authors.
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

#ifdef _WIN32
// Prevent inclusion of winsock.h
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <chrono>
#include <iostream>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/strings/str_join.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/internal/stats_exporter_impl.h"
#include "opencensus/stats/stats.h"
#include "ray/stats/metric_defs.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/stats.h"
#include "ray/stats/tag_defs.h"
#include "ray/util/logging.h"

namespace ray {

using namespace stats;
using opencensus::stats::Aggregation;
using opencensus::stats::BucketBoundaries;
using opencensus::stats::MeasureInt64;
using opencensus::stats::ViewData;
using opencensus::stats::ViewDescriptor;
using opencensus::tags::TagKey;
using ::testing::UnorderedPointwise;

class MockMetricsAgentClient : public rpc::MetricsAgentClient {
 public:
  MockMetricsAgentClient() {}

  void ReportOCMetrics(
      const rpc::ReportOCMetricsRequest &request,
      const rpc::ClientCallback<rpc::ReportOCMetricsReply> &callback) override {
    reportOCMetricsRequests_.push_back(request);
    callback(Status::OK(), {});
  }

  void HealthCheck(const rpc::HealthCheckRequest &request,
                   const rpc::ClientCallback<rpc::HealthCheckReply> &callback) override {}

  void WaitForServerReady(std::function<void(const Status &)> init_exporter_fn) override {
  }

  const std::vector<rpc::ReportOCMetricsRequest> &CollectedReportOCMetricsRequests()
      const {
    return reportOCMetricsRequests_;
  }

 private:
  std::vector<rpc::ReportOCMetricsRequest> reportOCMetricsRequests_;
};

const auto method_tag_key = TagKey::Register("grpc_client_method");
const auto status_tag_key = TagKey::Register("grpc_client_status");

TEST(OpenCensusProtoExporterTest, adds_global_tags_to_grpc) {
  const stats::TagsType global_tags = {{stats::LanguageKey, "CPP"},
                                       {stats::WorkerIdKey, "1000"}};
  StatsConfig::instance().SetGlobalTags(global_tags);

  auto measure = MeasureInt64::Register(
      "grpc.io/client/sent_messages_per_rpc", "Number of messages received per RPC", "1");

  const opencensus::stats::ViewDescriptor view_descriptor =
      ViewDescriptor()
          .set_name("grpc.io/client/sent_messages_per_rpc")
          .set_measure(measure.GetDescriptor().name())
          .set_aggregation(opencensus::stats::Aggregation::Count())
          .add_column(method_tag_key);

  view_descriptor.RegisterForExport();
  opencensus::stats::View view(view_descriptor);

  opencensus::stats::Record({{measure, 1}}, {{method_tag_key, "MyService.myMethod"}});

  opencensus::stats::DeltaProducer::Get()->Flush();
  opencensus::stats::StatsExporterImpl::Get()->Export();

  const auto view_data = view.GetData();
  auto mockClient = std::make_shared<MockMetricsAgentClient>();
  OpenCensusProtoExporter ocProtoExporter(mockClient, WorkerID::Nil(), 1000, 10000);

  rpc::ReportOCMetricsRequest proto;

  ocProtoExporter.ExportViewData({
      {view_descriptor, view_data},
  });

  ASSERT_THAT(mockClient->CollectedReportOCMetricsRequests().size(), 1);
  std::unordered_map<std::string, std::string> labels;
  auto metric = mockClient->CollectedReportOCMetricsRequests()[0].metrics()[0];
  for (int i = 0; i < metric.metric_descriptor().label_keys_size(); i++) {
    labels.emplace(metric.metric_descriptor().label_keys(i).key(),
                   metric.timeseries(0).label_values(i).value());
  }
  std::unordered_map<std::string, std::string> expected_labels = {
      {method_tag_key.name(), "MyService.myMethod"},
      {stats::LanguageKey.name(), "CPP"},
      {stats::WorkerIdKey.name(), "1000"}};
  ASSERT_EQ(labels, expected_labels);
}

// Register view
auto measure =
    MeasureInt64::Register("rpc_counter", "Simply counting RPCs, one at a time", "1");

TEST(OpenCensusProtoExporterTest, export_view_data_split_by_batch_size) {
  const opencensus::stats::ViewDescriptor view_descriptor =
      ViewDescriptor()
          .set_name("rpc_counter")
          .set_measure(measure.GetDescriptor().name())
          .set_aggregation(opencensus::stats::Aggregation::Count())
          .add_column(method_tag_key)
          .add_column(status_tag_key);

  view_descriptor.RegisterForExport();

  opencensus::stats::View view(view_descriptor);

  // Record metrics (2 distinct time-series)
  opencensus::stats::Record(
      {{measure, 1}}, {{method_tag_key, "Service.FirstMethod"}, {status_tag_key, "OK"}});
  opencensus::stats::Record(
      {{measure, 1}},
      {{method_tag_key, "Service.FirstMethod"}, {status_tag_key, "INTERNAL_FAILURE"}});
  opencensus::stats::Record(
      {{measure, 1}}, {{method_tag_key, "Service.SecondMethod"}, {status_tag_key, "OK"}});
  opencensus::stats::Record(
      {{measure, 1}},
      {{method_tag_key, "Service.SecondMethod"}, {status_tag_key, "UNAVAILABLE"}});

  opencensus::stats::DeltaProducer::Get()->Flush();
  opencensus::stats::StatsExporterImpl::Get()->Export();

  const auto view_data = view.GetData();

  {
    //
    // Test #1: Fitting all time-series inside of single batch
    //   - Batch-size is 4
    //   - Exporting 4 time-series
    //   - Only 1 RPC payload should be sent
    //
    size_t kBatchSize = 4;
    // Initialize the exporter
    auto mockClient = std::make_shared<MockMetricsAgentClient>();
    OpenCensusProtoExporter ocProtoExporter(
        mockClient, WorkerID::Nil(), kBatchSize, 10000);

    rpc::ReportOCMetricsRequest proto;

    ocProtoExporter.ExportViewData({
        {view_descriptor, view_data},
    });

    ASSERT_THAT(mockClient->CollectedReportOCMetricsRequests().size(), 1);
  }

  {
    //
    // Test #2: Splitting time-series across 2 batches
    //   - Batch-size is 2
    //   - Exporting 4 time-series
    //   - 2 RPC payloads should be sent
    //
    size_t kBatchSize = 2;
    // Initialize the exporter
    auto mockClient = std::make_shared<MockMetricsAgentClient>();
    OpenCensusProtoExporter ocProtoExporter(
        mockClient, WorkerID::Nil(), kBatchSize, 10000);

    rpc::ReportOCMetricsRequest proto;

    ocProtoExporter.ExportViewData({{view_descriptor, view_data}});

    ASSERT_THAT(mockClient->CollectedReportOCMetricsRequests().size(), 2);
  }
}

TEST(OpenCensusProtoExporterTest, export_view_data_split_by_payload_size) {
  const opencensus::stats::ViewDescriptor view_descriptor =
      ViewDescriptor()
          .set_name("rpc_counter")
          .set_measure(measure.GetDescriptor().name())
          .set_aggregation(opencensus::stats::Aggregation::Count())
          .add_column(method_tag_key)
          .add_column(status_tag_key);

  view_descriptor.RegisterForExport();

  opencensus::stats::View view(view_descriptor);

  // Record metrics (2 distinct time-series)
  opencensus::stats::Record(
      {{measure, 1}}, {{method_tag_key, "Service.FirstMethod"}, {status_tag_key, "OK"}});
  opencensus::stats::Record(
      {{measure, 1}},
      {{method_tag_key, "Service.FirstMethod"}, {status_tag_key, "INTERNAL_FAILURE"}});
  opencensus::stats::Record(
      {{measure, 1}}, {{method_tag_key, "Service.SecondMethod"}, {status_tag_key, "OK"}});
  opencensus::stats::Record(
      {{measure, 1}},
      {{method_tag_key, "Service.SecondMethod"}, {status_tag_key, "UNAVAILABLE"}});

  opencensus::stats::DeltaProducer::Get()->Flush();
  opencensus::stats::StatsExporterImpl::Get()->Export();

  const auto view_data = view.GetData();
  {
    //
    // Test #1: Splitting time-series across 2 batches (overflows payload size)
    //   - Batch-size is 4, max-payload size is 250 (1 metric def + 2 time-series)
    //   - Exporting 4 time-series
    //   - 2 RPC payloads should be sent (1 payload will be taking ~180 bytes, it'll be
    //   split in 2)
    //
    size_t kBatchSize = 4;
    size_t maxPayloadSize = 250;
    // Initialize the exporter
    auto mockClient = std::make_shared<MockMetricsAgentClient>();
    OpenCensusProtoExporter ocProtoExporter(
        mockClient, WorkerID::Nil(), kBatchSize, maxPayloadSize);

    rpc::ReportOCMetricsRequest proto;

    ocProtoExporter.ExportViewData({
        {view_descriptor, view_data},
    });

    auto requests = mockClient->CollectedReportOCMetricsRequests();
    ASSERT_THAT(requests.size(), 2);
    for (int i = 0; i < 2; ++i) {
      // Both batches have to have 1 metric with 2 time-series each
      auto metrics = requests[i].metrics();
      ASSERT_THAT(metrics.size(), 1);
      ASSERT_THAT(metrics[0].timeseries().size(), 2);
    }
  }

  {
    //
    // Test #2: Splitting time-series across 6 batches (overflows payload size)
    //   - Batch-size is 6, max-payload size is 250 (1 metric def + 2 time-series)
    //   - Exporting 12 time-series
    //   - 6 RPC payloads should be sent (since 1 payload will be taking ~250 bytes, it'll
    //   be split in 6)
    //
    size_t kBatchSize = 6;
    size_t maxPayloadSize = 250;  // 50% of the expected target payload size
    // Initialize the exporter
    auto mockClient = std::make_shared<MockMetricsAgentClient>();
    OpenCensusProtoExporter ocProtoExporter(
        mockClient, WorkerID::Nil(), kBatchSize, maxPayloadSize);

    rpc::ReportOCMetricsRequest proto;

    // NOTE: To avoid excessive boilerplate we just feed in same metrics
    // to simulate larger batches
    ocProtoExporter.ExportViewData({{view_descriptor, view_data},
                                    {view_descriptor, view_data},
                                    {view_descriptor, view_data}});

    auto requests = mockClient->CollectedReportOCMetricsRequests();
    ASSERT_THAT(requests.size(), 6);
    for (int i = 0; i < 6; ++i) {
      // Each of the batches have to have 1 metric with 2 time-series each
      auto metrics = requests[i].metrics();
      // ASSERT_THAT(metrics.size(), 1);
      ASSERT_THAT(metrics[0].timeseries().size(), 2);
    }
  }

  {
    //
    // Test #3: Splitting time-series across 1 batches (no overflowing)
    //   - Batch-size is 12 (all), max-payload size is 1000
    //   - Exporting 12 time-series
    //   - 1 RPC payloads should be sent (since 1 payload will be taking ~180 bytes, it'll
    //   be split in 6)
    //
    size_t kBatchSize = 12;
    size_t maxPayloadSize = 1000;  // 50% of the expected target payload size
    // Initialize the exporter
    auto mockClient = std::make_shared<MockMetricsAgentClient>();
    OpenCensusProtoExporter ocProtoExporter(
        mockClient, WorkerID::Nil(), kBatchSize, maxPayloadSize);

    rpc::ReportOCMetricsRequest proto;

    // NOTE: To avoid excessive boilerplate we just feed in same metrics
    // to simulate larger batches
    ocProtoExporter.ExportViewData({{view_descriptor, view_data},
                                    {view_descriptor, view_data},
                                    {view_descriptor, view_data}});
    auto requests = mockClient->CollectedReportOCMetricsRequests();
    ASSERT_THAT(requests.size(), 1);
    ASSERT_THAT(requests[0].metrics().size(), 3);
    // Batch have to have 3 metric with 4 time-series each
    for (int i = 0; i < 3; ++i) {
      ASSERT_THAT(requests[0].metrics()[i].timeseries().size(), 4);
    }
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
