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
#include "ray/telemetry/open_telemetry_metric_recorder.h"

#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/instruments.h>

#include <cassert>
#include <chrono>

namespace ray {
namespace telemetry {

OpenTelemetryMetricRecorder &OpenTelemetryMetricRecorder::GetInstance() {
  static OpenTelemetryMetricRecorder instance;
  return instance;
}

void OpenTelemetryMetricRecorder::Register(const std::string &endpoint,
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
  opentelemetry::metrics::Provider::SetMeterProvider(meter_provider_);
}

}  // namespace telemetry
}  // namespace ray
