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

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/observer_result.h>
#include <opentelemetry/metrics/sync_instruments.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>

#include <cassert>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"

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
  void RegisterGrpcExporter(const std::string &endpoint,
                            std::chrono::milliseconds interval,
                            std::chrono::milliseconds timeout);

  // Flush the remaining metrics. Note that this is a reset rather than a complete
  // shutdown, so it can be consistent with the shutdown behavior of stats.h.
  void Shutdown();

  // Registers a gauge metric with the given name and description
  void RegisterGaugeMetric(const std::string &name, const std::string &description);

  // Registers a counter metric with the given name and description
  void RegisterCounterMetric(const std::string &name, const std::string &description);

  // Check if a metric with the given name is registered.
  bool IsMetricRegistered(const std::string &name);

  // Set the value of a metric given the tags and the metric value.
  void SetMetricValue(const std::string &name,
                      absl::flat_hash_map<std::string, std::string> &&tags,
                      double value);

  // Get the value of a metric given the tags.
  std::optional<double> GetObservableMetricValue(
      const std::string &name, const absl::flat_hash_map<std::string, std::string> &tags);

  // Helper function to collect gauge metric values. This function is called only once
  // per interval for each metric. It collects the values from the observations_by_name_
  // map and passes them to the observer.
  void CollectGaugeMetricValues(
      const std::string &name,
      const opentelemetry::nostd::shared_ptr<
          opentelemetry::metrics::ObserverResultT<double>> &observer);

  // Delete copy constructors and assignment operators. Skip generation of the move
  // constructors and assignment operators.
  OpenTelemetryMetricRecorder(const OpenTelemetryMetricRecorder &) = delete;
  OpenTelemetryMetricRecorder &operator=(const OpenTelemetryMetricRecorder &) = delete;
  ~OpenTelemetryMetricRecorder() = default;

 private:
  OpenTelemetryMetricRecorder();
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;

  // Map of metric names to their observations (aka. set of tags and metric values).
  // This contains all data points for a given metric for a given interval. This map is
  // cleared at the end of each interval.
  absl::flat_hash_map<
      std::string,
      absl::flat_hash_map<absl::flat_hash_map<std::string, std::string>, double>>
      observations_by_name_;
  // Map of metric names to their instrument pointers. This is used to ensure
  // that each metric is only registered once.
  absl::flat_hash_map<
      std::string,
      opentelemetry::nostd::variant<
          opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>,
          opentelemetry::nostd::unique_ptr<
              opentelemetry::metrics::SynchronousInstrument>>>
      registered_instruments_;
  // List of gauge callback names. This is used as data for the gauge callbacks.
  std::list<std::string> gauge_callback_names_;
  // Lock for thread safety when modifying state.
  std::mutex mutex_;
  // Flag to indicate if the recorder is shutting down. This is used to make sure that
  // the recorder will only shutdown once.
  std::atomic<bool> is_shutdown_{false};

  void SetObservableMetricValue(const std::string &name,
                                absl::flat_hash_map<std::string, std::string> &&tags,
                                double value);

  void SetSynchronousMetricValue(const std::string &name,
                                 absl::flat_hash_map<std::string, std::string> &&tags,
                                 double value);

  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> GetMeter() {
    return meter_provider_->GetMeter("ray");
  }
};
}  // namespace telemetry
}  // namespace ray
