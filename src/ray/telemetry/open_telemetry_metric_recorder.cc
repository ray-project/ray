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
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/instruments.h>

#include <cassert>
#include <utility>

#include "ray/util/logging.h"

// Anonymous namespace that contains the private callback functions for the
// OpenTelemetry metrics.
namespace {
using ray::telemetry::OpenTelemetryMetricRecorder;

static void _DoubleGaugeCallback(
    std::variant<std::shared_ptr<opentelemetry::metrics::ObserverResultT<long>>,
                 std::shared_ptr<opentelemetry::metrics::ObserverResultT<double>>>
        observer,
    void *state) {
  const std::string *name_ptr = static_cast<const std::string *>(state);
  const std::string &name = *name_ptr;
  OpenTelemetryMetricRecorder &recorder = OpenTelemetryMetricRecorder::GetInstance();
  // Note: The observer is expected to be of type double, so we can safely cast it.
  auto obs = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<double>>>(
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

void OpenTelemetryMetricRecorder::Shutdown() { meter_provider_->ForceFlush(); }

void OpenTelemetryMetricRecorder::CollectGaugeMetricValues(
    const std::string &name,
    const std::shared_ptr<opentelemetry::metrics::ObserverResultT<double>> &observer) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = observations_by_name_.find(name);
  if (it == observations_by_name_.end()) {
    return;  // Not registered
  }
  for (const auto &observation : it->second) {
    observer->Observe(observation.second, observation.first);
  }
  // Clear the observations after exporting so the next interval starts fresh
  observations_by_name_[name].clear();
}

void OpenTelemetryMetricRecorder::RegisterGaugeMetric(const std::string &name,
                                                      const std::string &description) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (registered_observable_instruments_.find(name) !=
      registered_observable_instruments_.end()) {
    return;  // Already registered
  }
  auto instrument = getMeter()->CreateDoubleObservableGauge(name, description, "");
  std::string *name_ptr = new std::string(name);
  instrument->AddCallback(&_DoubleGaugeCallback, static_cast<void *>(name_ptr));
  observations_by_name_[name] = {};
  registered_observable_instruments_[name] = instrument;
}

void OpenTelemetryMetricRecorder::RegisterCounterMetric(const std::string &name,
                                                        const std::string &description) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (registered_synchronous_instruments_.find(name) !=
      registered_synchronous_instruments_.end()) {
    return;  // Already registered
  }
  auto instrument = getMeter()->CreateDoubleCounter(name, description, "");
  registered_synchronous_instruments_[name] = std::move(instrument);
}

bool OpenTelemetryMetricRecorder::IsMetricRegistered(const std::string &name) const {
  return (registered_observable_instruments_.find(name) !=
              registered_observable_instruments_.end() ||
          registered_synchronous_instruments_.find(name) !=
              registered_synchronous_instruments_.end());
  )
}

void OpenTelemetryMetricRecorder::SetMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  if (isObservableMetric(name)) {
    return setObservableMetricValue(name, std::move(tags), value);
  } else {
    return setSynchronousMetricValue(name, std::move(tags), value);
  }
}

std::optional<double> OpenTelemetryMetricRecorder::GetObservableMetricValue(
    const std::string &name,
    const absl::flat_hash_map<std::string, std::string> &tags) const {
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

void OpenTelemetryMetricRecorder::setObservableMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = observations_by_name_.find(name);
  if (it == observations_by_name_.end()) {
    RAY_CHECK(false) << "Metric " << name
                     << " is not registered. Please register it before setting a value.";
    return;  // Not registered
  }
  it->second[std::move(tags)] = value;  // Set or update the value
}

bool OpenTelemetryMetricRecorder::setSynchronousMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = registered_synchronous_instruments_.find(name);
  if (it == registered_synchronous_instruments_.end()) {
    return false;  // Not registered
  }
  auto &instrument = it->second;
  if (auto counter =
          dynamic_cast<opentelemetry::metrics::Counter<double> *>(instrument.get())) {
    counter->Add(value, std::move(tags));
    return true;
  } else {
    // Unknown or unsupported instrument type
    RAY_CHECK(false) << "Unsupported synchronous instrument type for metric: " << name;
    return false;
  }
}

}  // namespace telemetry
}  // namespace ray
