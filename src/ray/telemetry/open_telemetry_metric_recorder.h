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
#include <opentelemetry/sdk/metrics/meter_provider.h>

#include <chrono>

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

  // Delete copy and move constructors and assignment operators.
  OpenTelemetryMetricRecorder(const OpenTelemetryMetricRecorder &) = delete;
  OpenTelemetryMetricRecorder &operator=(const OpenTelemetryMetricRecorder &) = delete;
  OpenTelemetryMetricRecorder(OpenTelemetryMetricRecorder &&) = delete;
  OpenTelemetryMetricRecorder &operator=(OpenTelemetryMetricRecorder &&) = delete;

 private:
  OpenTelemetryMetricRecorder();
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;
};
}  // namespace telemetry
}  // namespace ray
