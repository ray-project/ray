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

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/observer_result.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>

#include <cassert>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace ray {
namespace telemetry {

// OpenTelemetryMetricRecorder is a singleton class that initializes the OpenTelemetry
// grpc exporter and creates a Meter for recording metrics. It is responsible for
// exporting metrics to a repoter_agent.py endpoint at a given interval.
class OpenTelemetryMetricRecorder {
 public:
  // Returns the singleton instance of OpenTelemetryMetricRecorder. This should be
  // called after Register() to ensure the instance is initialized.
  static OpenTelemetryMetricRecorder &GetInstance();

  // Registers the OpenTelemetryMetricRecorder with the specified grpc endpoint,
  // interval and timeout. This should be called only once per process.
  void Register(const std::string &endpoint,
                std::chrono::milliseconds interval,
                std::chrono::milliseconds timeout);

  // Registers a gauge metric with the given name and description
  void RegisterGaugeMetric(const std::string &name, const std::string &description);

  // Set the value of a metric given the tags and the metric value.
  void SetMetricValue(const std::string &name,
                      const std::map<std::string, std::string> &tags,
                      double value);

  // Get the value of a metric given the tags.
  std::optional<double> GetMetricValue(const std::string &name,
                                       const std::map<std::string, std::string> &tags);

  // Helper function to collect gauge metric values. This function is called only once
  // per interval for each metric. It collects the values from the observations_by_name_
  // map and passes them to the observer.
  void CollectGaugeMetricValues(
      const std::string &name,
      const std::shared_ptr<opentelemetry::metrics::ObserverResultT<double>> &observer);

  // Delete copy and move constructors and assignment operators.
  OpenTelemetryMetricRecorder(const OpenTelemetryMetricRecorder &) = delete;
  OpenTelemetryMetricRecorder &operator=(const OpenTelemetryMetricRecorder &) = delete;
  OpenTelemetryMetricRecorder(OpenTelemetryMetricRecorder &&) = delete;
  OpenTelemetryMetricRecorder &operator=(OpenTelemetryMetricRecorder &&) = delete;

 private:
  OpenTelemetryMetricRecorder();
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;

  // Map of metric names to their observations (aka. set of tags and metric values).
  // This contains all data points for a given metric for a given interval. This map is
  // cleared at the end of each interval.
  std::unordered_map<std::string, std::map<std::map<std::string, std::string>, double>>
      observations_by_name_;
  // Map of metric names to their instrument pointers. This is used to ensure that
  // each metric is only registered once.
  std::unordered_map<std::string,
                     std::shared_ptr<opentelemetry::metrics::ObservableInstrument>>
      registered_instruments_;
  // Lock for thread safety when writing to maps.
  std::mutex mutex_;

  std::shared_ptr<opentelemetry::metrics::Meter> getMeter() {
    return meter_provider_->GetMeter("ray");
  }
};
}  // namespace telemetry
}  // namespace ray
