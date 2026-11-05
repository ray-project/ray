// Copyright 2026 The Ray Authors.
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

#include "ray/stats/bucket_histogram_metric.h"

#include <gtest/gtest.h>
#include <opentelemetry/sdk/metrics/data/metric_data.h>
#include <opentelemetry/sdk/metrics/data/point_data.h>
#include <opentelemetry/sdk/metrics/export/metric_producer.h>
#include <opentelemetry/sdk/metrics/instruments.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/metric_reader.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/ray_config.h"
#include "ray/observability/open_telemetry_metric_recorder.h"
#include "ray/stats/metric.h"
#include "ray/stats/quadratic_bucket_scheme.h"

namespace ray {
namespace stats {

using namespace std::literals;
using OpenTelemetryMetricRecorder = ray::observability::OpenTelemetryMetricRecorder;
namespace otel_metrics = opentelemetry::sdk::metrics;

namespace {

// A reader that collects on demand, so the test can read back what the recorder's
// MeterProvider aggregated. The recorder only exports through its OTLP reader once
// Start() is called, which these tests don't do.
class CollectingMetricReader : public otel_metrics::MetricReader {
 public:
  otel_metrics::AggregationTemporality GetAggregationTemporality(
      otel_metrics::InstrumentType) const noexcept override {
    return otel_metrics::AggregationTemporality::kCumulative;
  }

 private:
  bool OnForceFlush(std::chrono::microseconds) noexcept override { return true; }
  bool OnShutDown(std::chrono::microseconds) noexcept override { return true; }
};

// Bucket index the exported histogram assigns to a value. Mirrors
// opentelemetry-cpp's BucketBinarySearch (std::lower_bound over the explicit
// bounds), which gives Prometheus `le` semantics: a bucket includes its upper
// bound.
int ExportedBucket(const std::vector<double> &boundaries, double value) {
  return std::lower_bound(boundaries.begin(), boundaries.end(), value) -
         boundaries.begin();
}

// Every bound, the representable values on either side of it, zero and an
// overflow value.
std::vector<double> BoundaryProbes(const std::vector<double> &boundaries) {
  std::vector<double> probes = {0.0, 2 * boundaries.back()};
  for (double bound : boundaries) {
    probes.push_back(std::nextafter(bound, 0.0));
    probes.push_back(bound);
    probes.push_back(std::nextafter(bound, 2 * boundaries.back()));
  }
  return probes;
}

}  // namespace

class BucketHistogramMetricTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    StatsConfig::instance().SetGlobalTags({});
    StatsConfig::instance().SetIsDisableStats(false);
    for (auto &f : StatsConfig::instance().PopInitializers()) {
      f();
    }
    StatsConfig::instance().SetIsInitialized(true);
    // Attach before any metric in this suite is created, so the reader sees every
    // recorded value.
    reader_ = std::make_shared<CollectingMetricReader>();
    OpenTelemetryMetricRecorder::GetInstance().meter_provider_->AddMetricReader(reader_);
  }

 protected:
  // Returns the histogram data point the recorder aggregated for the metric and
  // label set, or nullopt if there is none.
  static std::optional<otel_metrics::HistogramPointData> CollectHistogram(
      const std::string &name, const otel_metrics::PointAttributes &attributes) {
    std::optional<otel_metrics::HistogramPointData> result;
    reader_->Collect([&](otel_metrics::ResourceMetrics &resource_metrics) {
      for (const auto &scope_metrics : resource_metrics.scope_metric_data_) {
        for (const auto &metric_data : scope_metrics.metric_data_) {
          if (metric_data.instrument_descriptor.name_ != name) {
            continue;
          }
          for (const auto &point : metric_data.point_data_attr_) {
            if (point.attributes == attributes) {
              result = opentelemetry::nostd::get<otel_metrics::HistogramPointData>(
                  point.point_data);
            }
          }
        }
      }
      return true;
    });
    return result;
  }

  static std::shared_ptr<CollectingMetricReader> reader_;
};

std::shared_ptr<CollectingMetricReader> BucketHistogramMetricTest::reader_;

// Every value, including one exactly on a boundary, lands in the same bucket in an
// explicit-bucket histogram with the scheme's ExplicitBounds as in
// QuadraticBucketScheme::ToBucket.
TEST_F(BucketHistogramMetricTest, ExplicitBoundsMatchQuadraticScheme) {
  const std::vector<std::pair<int, double>> configs = {
      {2, 1.0}, {16, 500.0}, {128, 10000.0}, {256, 60000.0}};
  for (const auto &[num_buckets, max_value] : configs) {
    SCOPED_TRACE(testing::Message()
                 << "num_buckets=" << num_buckets << " max_value=" << max_value);
    QuadraticBucketScheme scheme(num_buckets, max_value);
    const std::vector<double> boundaries = scheme.ExplicitBounds();
    ASSERT_EQ(boundaries.size(), static_cast<size_t>(num_buckets - 1));
    EXPECT_EQ(boundaries.back(), max_value);

    std::vector<double> probes = BoundaryProbes(boundaries);
    probes.push_back(-1.0);
    for (double value : probes) {
      ASSERT_EQ(ExportedBucket(boundaries, value), scheme.ToBucket(value))
          << "value=" << value;
    }
  }
}

// Records through every Record overload with OpenTelemetry enabled, and checks the
// histogram the recorder aggregated: the scheme's bounds, and each value counted in
// the bucket QuadraticBucketScheme::ToBucket assigns it.
TEST_F(BucketHistogramMetricTest, RecordsIntoSchemeBucketsWithOpenTelemetry) {
  // On by default; this test checks the OpenTelemetry path, so don't skip it.
  ASSERT_TRUE(::RayConfig::instance().enable_open_telemetry());
  const std::string name = "bucket_histogram_metric_test_record";
  const int num_buckets = 64;
  const double max_value = 1000.0;
  BucketHistogramMetric metric(name,
                               "A test bucket histogram metric",
                               "ms",
                               max_value,
                               num_buckets,
                               /*tag_keys=*/{"Tag1"});
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(name));

  const QuadraticBucketScheme scheme(num_buckets, max_value);
  const std::vector<double> boundaries = scheme.ExplicitBounds();
  // Negative values aren't probed: OpenTelemetry drops them from histograms.
  const std::vector<double> probes = BoundaryProbes(boundaries);
  std::vector<uint64_t> expected_counts(num_buckets, 0);
  double expected_sum = 0.0;
  for (double value : probes) {
    metric.Record(value, {{"Tag1"sv, "A"}});
    expected_counts[scheme.ToBucket(value)]++;
    expected_sum += value;
  }
  metric.Record(5.0, TagsType{{TagKeyType::Register("Tag1"), "B"}});
  // Without tags, Tag1 is recorded as an empty value.
  metric.Record(2 * max_value);

  auto tagged = CollectHistogram(name, {{"Tag1", std::string("A")}});
  ASSERT_TRUE(tagged.has_value());
  EXPECT_EQ(tagged->boundaries_, boundaries);
  EXPECT_EQ(tagged->counts_, expected_counts);
  EXPECT_EQ(tagged->count_, probes.size());
  EXPECT_DOUBLE_EQ(opentelemetry::nostd::get<double>(tagged->sum_), expected_sum);

  auto tags_type = CollectHistogram(name, {{"Tag1", std::string("B")}});
  ASSERT_TRUE(tags_type.has_value());
  EXPECT_EQ(tags_type->count_, 1);
  EXPECT_EQ(tags_type->counts_[scheme.ToBucket(5.0)], 1);

  auto untagged = CollectHistogram(name, {{"Tag1", std::string("")}});
  ASSERT_TRUE(untagged.has_value());
  EXPECT_EQ(untagged->count_, 1);
  EXPECT_EQ(untagged->counts_.back(), 1);  // The overflow bucket.
}

}  // namespace stats
}  // namespace ray
