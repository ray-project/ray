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
#include "ray/telemetry/open_telemetry_metric_recorder.h"

#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/instruments.h>

#include <cassert>
#include <utility>

#include "ray/util/logging.h"

// Anonymous namespace that contains the private callback functions for the
// OpenTelemetry metrics.
namespace {
using ray::telemetry::OpenTelemetryMetricRecorder;

static void _DoubleGaugeCallback(opentelemetry::metrics::ObserverResult observer,
                                 void *state) {
  const std::string *name_ptr = static_cast<const std::string *>(state);
  const std::string &name = *name_ptr;
  OpenTelemetryMetricRecorder &recorder = OpenTelemetryMetricRecorder::GetInstance();
  // Note: The observer is expected to be of type double, so we can safely cast it.
  auto obs = opentelemetry::nostd::get<
      opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObserverResultT<double>>>(
      observer);
  recorder.CollectGaugeMetricValues(name, obs);
}

}  // anonymous namespace

namespace ray {
namespace telemetry {

OpenTelemetryMetricRecorder &OpenTelemetryMetricRecorder::GetInstance() {
  static auto *instance = new OpenTelemetryMetricRecorder();
  return *instance;
}

void OpenTelemetryMetricRecorder::RegisterGrpcExporter(
    const std::string &endpoint,
    std::chrono::milliseconds interval,
    std::chrono::milliseconds timeout) {
  // Create an OTLP exporter
  opentelemetry::exporter::otlp::OtlpGrpcMetricExporterOptions exporter_options;
  exporter_options.endpoint = endpoint;
  auto exporter = std::make_unique<opentelemetry::exporter::otlp::OtlpGrpcMetricExporter>(
      exporter_options);

  // Initialize the OpenTelemetry SDK and create a Meter
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions reader_options;
  reader_options.export_interval_millis = interval;
  reader_options.export_timeout_millis = timeout;
  auto reader =
      std::make_unique<opentelemetry::sdk::metrics::PeriodicExportingMetricReader>(
          std::move(exporter), reader_options);
  meter_provider_->AddMetricReader(std::move(reader));
}

OpenTelemetryMetricRecorder::OpenTelemetryMetricRecorder() {
  // Default constructor
  meter_provider_ = std::make_shared<opentelemetry::sdk::metrics::MeterProvider>();
  opentelemetry::metrics::Provider::SetMeterProvider(
      opentelemetry::nostd::shared_ptr<opentelemetry::metrics::MeterProvider>(
          meter_provider_));
}

void OpenTelemetryMetricRecorder::Shutdown() {
  bool expected = false;
  if (!is_shutdown_.compare_exchange_strong(expected, true)) {
    // Already shut down, skip
    return;
  }
  meter_provider_->ForceFlush();
  meter_provider_->Shutdown();
}

void OpenTelemetryMetricRecorder::CollectGaugeMetricValues(
    const std::string &name,
    const opentelemetry::nostd::shared_ptr<
        opentelemetry::metrics::ObserverResultT<double>> &observer) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = observations_by_name_.find(name);
  if (it == observations_by_name_.end()) {
    return;  // Not registered
  }
  for (const auto &observation : it->second) {
    observer->Observe(observation.second, observation.first);
  }
}

void OpenTelemetryMetricRecorder::RegisterGaugeMetric(const std::string &name,
                                                      const std::string &description) {
  std::string *name_ptr;
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      instrument;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (registered_instruments_.contains(name)) {
      return;  // Already registered
    }
    gauge_callback_names_.push_back(name);
    name_ptr = &gauge_callback_names_.back();
    instrument = GetMeter()->CreateDoubleObservableGauge(name, description, "");
    observations_by_name_[name] = {};
    registered_instruments_[name] = instrument;
  }
  // Important: Do not hold mutex_ (mutex A) when registering the callback.
  //
  // The callback function will be invoked later by the OpenTelemetry SDK,
  // and it will attempt to acquire mutex_ (A) again. Meanwhile, both this function
  // and the callback may also acquire an internal mutex (mutex B) owned by the
  // instrument object.
  //
  // If we hold mutex A while registering the callback—and the callback later tries
  // to acquire A while holding B—a lock-order inversion may occur, leading to
  // a potential deadlock.
  //
  // To avoid this, ensure the callback is registered *after* releasing mutex_ (A).
  instrument->AddCallback(&_DoubleGaugeCallback, static_cast<void *>(name_ptr));
}

bool OpenTelemetryMetricRecorder::IsMetricRegistered(const std::string &name) {
  std::lock_guard<std::mutex> lock(mutex_);
  return registered_instruments_.contains(name);
}

void OpenTelemetryMetricRecorder::SetMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = observations_by_name_.find(name);
  RAY_CHECK(it != observations_by_name_.end())
      << "Metric " << name
      << " is not registered. Please register it before setting a value.";
  it->second[std::move(tags)] = value;  // Set or update the value
}

std::optional<double> OpenTelemetryMetricRecorder::GetMetricValue(
    const std::string &name, const absl::flat_hash_map<std::string, std::string> &tags) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = observations_by_name_.find(name);
  if (it == observations_by_name_.end()) {
    return std::nullopt;  // Not registered
  }
  auto tag_it = it->second.find(tags);
  if (tag_it != it->second.end()) {
    return tag_it->second;  // Get the value
  }
  return std::nullopt;
}

}  // namespace telemetry
}  // namespace ray
