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
#include "ray/observability/open_telemetry_metric_recorder.h"

#include <opentelemetry/context/context.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/sdk/metrics/aggregation/histogram_aggregation.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/instruments.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector.h>
#include <opentelemetry/sdk/metrics/view/meter_selector.h>
#include <opentelemetry/sdk/metrics/view/view.h>
#include <opentelemetry/sdk/metrics/view/view_registry.h>

#include <cassert>
#include <utility>

#include "ray/common/constants.h"
#include "ray/rpc/authentication/authentication_mode.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "ray/rpc/common.h"
#include "ray/util/logging.h"

// Anonymous namespace that contains the private callback functions for the
// OpenTelemetry metrics.
namespace {
using ray::observability::OpenTelemetryMetricRecorder;

static void DoubleGaugeCallback(opentelemetry::metrics::ObserverResult observer,
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

class OpenTelemetryMetricExporter
    : public opentelemetry::exporter::otlp::OtlpGrpcMetricExporter {
 public:
  explicit OpenTelemetryMetricExporter(
      const opentelemetry::exporter::otlp::OtlpGrpcMetricExporterOptions &options)
      : opentelemetry::exporter::otlp::OtlpGrpcMetricExporter(options) {}

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::sdk::metrics::ResourceMetrics &data) noexcept override {
    const opentelemetry::sdk::common::ExportResult result =
        opentelemetry::exporter::otlp::OtlpGrpcMetricExporter::Export(data);
    if (result != opentelemetry::sdk::common::ExportResult::kSuccess) {
      RAY_LOG(WARNING) << "Failed to export metrics to the metrics agent. Result: "
                       << static_cast<int>(result);
    }
    return result;
  }
};

}  // anonymous namespace

namespace ray {
namespace observability {

OpenTelemetryMetricRecorder &OpenTelemetryMetricRecorder::GetInstance() {
  // Note: This creates a singleton instance of the OpenTelemetryMetricRecorder. The
  // singleton lives until and is cleaned up automatically by the process exit. The
  // OpenTelemetryMetricRecorder is created this way so that the singleton instance
  // can be used to register/record metrics across the codebase easily.
  static auto *instance = new OpenTelemetryMetricRecorder();
  return *instance;
}

void OpenTelemetryMetricRecorder::Start(const std::string &endpoint,
                                        std::chrono::milliseconds interval,
                                        std::chrono::milliseconds timeout) {
  // Create an OTLP exporter
  exporter_options_.endpoint = endpoint;
  // This line ensures that only the delta values for count and sum are exported during
  // each collection interval. This is necessary because the dashboard agent already
  // accumulates these metrics—re-accumulating them during export would lead to double
  // counting.
  exporter_options_.aggregation_temporality =
      opentelemetry::exporter::otlp::PreferredAggregationTemporality::kDelta;
  // Add authentication token to metadata if auth is enabled
  if (rpc::GetAuthenticationMode() == rpc::AuthenticationMode::TOKEN) {
    auto token = rpc::AuthenticationTokenLoader::instance().GetToken();
    if (token && !token->empty()) {
      const std::string auth_key(kAuthTokenKey);
      exporter_options_.metadata.erase(auth_key);
      exporter_options_.metadata.insert({auth_key, token->ToAuthorizationHeaderValue()});
    }
  }
  // Configure TLS/SSL credentials to match how Ray's gRPC servers are configured.
  // When USE_TLS is enabled, the dashboard agent's gRPC server uses SSL, so the
  // OpenTelemetry exporter must also use SSL to connect successfully.
  // See https://github.com/ray-project/ray/issues/59968
  if (RayConfig::instance().USE_TLS()) {
    exporter_options_.use_ssl_credentials = true;

    // Load CA certificate for server verification.
    // Reuse ReadCert from ray/rpc/common.h for consistency with other TLS code paths.
    std::string ca_cert_file = std::string(RayConfig::instance().TLS_CA_CERT());
    if (!ca_cert_file.empty()) {
      std::string ca_cert = rpc::ReadCert(ca_cert_file);
      RAY_CHECK(!ca_cert.empty())
          << "Failed to read CA certificate file: " << ca_cert_file;
      exporter_options_.ssl_credentials_cacert_as_string = std::move(ca_cert);
    }

#ifdef ENABLE_OTLP_GRPC_SSL_MTLS_PREVIEW
    // Load client certificate and key for mutual TLS (mTLS).
    // Ray's gRPC server requires client authentication when CA cert is configured.
    // Note: mTLS support requires the OpenTelemetry SDK to be built with
    // ENABLE_OTLP_GRPC_SSL_MTLS_PREVIEW defined.
    //
    // We reuse TLS_SERVER_CERT and TLS_SERVER_KEY for the client certificate because
    // Ray components (raylet, gcs_server, etc.) act as both servers and clients,
    // using the same certificate for bidirectional mTLS communication. This is
    // consistent with how other Ray gRPC clients are configured.
    std::string client_cert_file = std::string(RayConfig::instance().TLS_SERVER_CERT());
    std::string client_key_file = std::string(RayConfig::instance().TLS_SERVER_KEY());
    if (!client_cert_file.empty()) {
      std::string client_cert = rpc::ReadCert(client_cert_file);
      RAY_CHECK(!client_cert.empty())
          << "Failed to read client certificate file: " << client_cert_file;
      exporter_options_.ssl_client_cert_string = std::move(client_cert);
    }
    if (!client_key_file.empty()) {
      std::string client_key = rpc::ReadCert(client_key_file);
      RAY_CHECK(!client_key.empty())
          << "Failed to read client key file: " << client_key_file;
      exporter_options_.ssl_client_key_string = std::move(client_key);
    }
    RAY_LOG(INFO) << "OpenTelemetry metric exporter configured with TLS and mTLS enabled";
#else
    // Ray's gRPC server requires client certificates (mTLS) when TLS is enabled.
    // Without mTLS support, the OpenTelemetry exporter will fail to connect.
    // This is a fatal error because metric export will silently fail otherwise.
    RAY_LOG(FATAL)
        << "OpenTelemetry metric exporter cannot be configured: TLS is enabled "
        << "but mTLS support is not available (SDK built without "
        << "ENABLE_OTLP_GRPC_SSL_MTLS_PREVIEW). Ray's gRPC servers require "
        << "client certificates when TLS is enabled.";
#endif
  } else {
    exporter_options_.use_ssl_credentials = false;
    exporter_options_.ssl_credentials_cacert_as_string.clear();
#ifdef ENABLE_OTLP_GRPC_SSL_MTLS_PREVIEW
    exporter_options_.ssl_client_cert_string.clear();
    exporter_options_.ssl_client_key_string.clear();
#endif
  }
  auto exporter = std::make_unique<OpenTelemetryMetricExporter>(exporter_options_);

  // Initialize the OpenTelemetry SDK and create a Meter
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions reader_options;
  reader_options.export_interval_millis = interval;
  reader_options.export_timeout_millis = timeout;
  auto reader =
      std::make_unique<opentelemetry::sdk::metrics::PeriodicExportingMetricReader>(
          std::move(exporter), reader_options);
  // Reset the is_shutdown_ flag to false to ensure the newly added metric reader will
  // be shut down correctly.
  //
  // In most cases, OpenTelemetryMetricRecorder is initialized and shut down only once
  // per process, so setting this to false is effectively a no-op. However, in the driver
  // process, the recorder may be initialized and shut down multiple times (e.g., repeated
  // calls to ray.init() and ray.shutdown()). In such cases, is_shutdown_ may already be
  // true when we reach this point (leaking from the previous ray cluster). Resetting it
  // to false ensures that the newly added metric reader will be shut down correctly.
  is_shutdown_ = false;
  meter_provider_->AddMetricReader(std::move(reader));
}

OpenTelemetryMetricRecorder::OpenTelemetryMetricRecorder() {
  if (RayConfig::instance().disable_open_telemetry_sdk_log()) {
    opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogLevel(
        opentelemetry::sdk::common::internal_log::LogLevel::None);
  }
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
  RAY_CHECK(it != observations_by_name_.end())
      << "Metric " << name << " is not registered";
  for (const auto &observation : it->second) {
    observer->Observe(observation.second, observation.first);
  }
  it->second.clear();
}

void OpenTelemetryMetricRecorder::RegisterGaugeMetric(const std::string &name,
                                                      const std::string &description) {
  std::string *name_ptr;
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      instrument;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (registered_instruments_.contains(name)) {
      // Already registered.  Note that this is a common case for metrics defined
      // via Metric interface. See https://github.com/ray-project/ray/issues/54538
      // for more details.
      return;
    }
    gauge_metric_names_.push_back(name);
    name_ptr = &gauge_metric_names_.back();
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
  instrument->AddCallback(&DoubleGaugeCallback, static_cast<void *>(name_ptr));
}

bool OpenTelemetryMetricRecorder::IsMetricRegistered(const std::string &name) {
  std::lock_guard<std::mutex> lock(mutex_);
  return registered_instruments_.contains(name);
}

void OpenTelemetryMetricRecorder::RegisterCounterMetric(const std::string &name,
                                                        const std::string &description) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (registered_instruments_.contains(name)) {
    // Already registered.  Note that this is a common case for metrics defined
    // via Metric interface. See https://github.com/ray-project/ray/issues/54538
    // for more details.
    return;
  }
  auto instrument = GetMeter()->CreateDoubleCounter(name, description, "");
  registered_instruments_[name] = std::move(instrument);
}

void OpenTelemetryMetricRecorder::RegisterSumMetric(const std::string &name,
                                                    const std::string &description) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (registered_instruments_.contains(name)) {
    // Already registered.  Note that this is a common case for metrics defined
    // via Metric interface. See https://github.com/ray-project/ray/issues/54538
    // for more details.
    return;
  }
  auto instrument = GetMeter()->CreateDoubleUpDownCounter(name, description, "");
  registered_instruments_[name] = std::move(instrument);
}

void OpenTelemetryMetricRecorder::RegisterHistogramMetric(
    const std::string &name,
    const std::string &description,
    const std::vector<double> &buckets) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (registered_instruments_.contains(name)) {
    // Already registered.  Note that this is a common case for metrics defined
    // via Metric interface. See https://github.com/ray-project/ray/issues/54538
    // for more details.
    return;
  }
  // Create a histogram instrument with explicit buckets
  // TODO(can-anyscale): use factory pattern for a cleaner creation of histogram view:
  // https://github.com/open-telemetry/opentelemetry-cpp/blob/main/examples/metrics_simple/metrics_ostream.cc#L93.
  // This requires a new version of the OpenTelemetry SDK. See
  // https://github.com/ray-project/ray/issues/54538 for the complete backlog of Ray
  // metric infra improvements.
  auto aggregation_config =
      std::make_shared<opentelemetry::sdk::metrics::HistogramAggregationConfig>();
  aggregation_config->boundaries_ = buckets;
  auto view = std::make_unique<opentelemetry::sdk::metrics::View>(
      name,
      description,
      /*unit=*/"",
      opentelemetry::sdk::metrics::AggregationType::kHistogram,
      aggregation_config);

  auto instrument_selector =
      std::make_unique<opentelemetry::sdk::metrics::InstrumentSelector>(
          opentelemetry::sdk::metrics::InstrumentType::kHistogram,
          name,
          /*unit_filter=*/"");
  auto meter_selector = std::make_unique<opentelemetry::sdk::metrics::MeterSelector>(
      meter_name_, /*meter_version=*/"", /*schema_url=*/"");
  meter_provider_->AddView(
      std::move(instrument_selector), std::move(meter_selector), std::move(view));
  auto instrument = GetMeter()->CreateDoubleHistogram(name, description, /*unit=*/"");
  registered_instruments_[name] = std::move(instrument);
}

void OpenTelemetryMetricRecorder::SetMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (observations_by_name_.contains(name)) {
    SetObservableMetricValue(name, std::move(tags), value);
  } else {
    SetSynchronousMetricValue(name, std::move(tags), value);
  }
}

void OpenTelemetryMetricRecorder::SetObservableMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  auto it = observations_by_name_.find(name);
  RAY_CHECK(it != observations_by_name_.end())
      << "Metric " << name
      << " is not registered. Please register it before setting a value.";
  it->second[std::move(tags)] = value;  // Set or update the value
}

void OpenTelemetryMetricRecorder::SetSynchronousMetricValue(
    const std::string &name,
    absl::flat_hash_map<std::string, std::string> &&tags,
    double value) {
  auto it = registered_instruments_.find(name);
  RAY_CHECK(it != registered_instruments_.end())
      << "Metric " << name
      << " is not registered. Please register it before setting a value.";
  auto &instrument = it->second;
  auto *sync_instr_ptr = opentelemetry::nostd::get_if<
      opentelemetry::nostd::unique_ptr<opentelemetry::metrics::SynchronousInstrument>>(
      &instrument);
  RAY_CHECK(sync_instr_ptr != nullptr)
      << "Metric " << name << " is not a synchronous instrument";
  if (auto *counter = dynamic_cast<opentelemetry::metrics::Counter<double> *>(
          sync_instr_ptr->get())) {
    counter->Add(value, std::move(tags));
  } else if (auto *sum = dynamic_cast<opentelemetry::metrics::UpDownCounter<double> *>(
                 sync_instr_ptr->get())) {
    sum->Add(value, std::move(tags));
  } else if (auto *histogram = dynamic_cast<opentelemetry::metrics::Histogram<double> *>(
                 sync_instr_ptr->get())) {
    histogram->Record(value, std::move(tags), opentelemetry::context::Context());
  } else {
    // Unknown or unsupported instrument type
    RAY_CHECK(false) << "Unsupported synchronous instrument type for metric: " << name;
  }
}

}  // namespace observability
}  // namespace ray
