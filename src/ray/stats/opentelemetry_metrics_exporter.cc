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

#include "ray/stats/opentelemetry_metrics_exporter.h"

namespace ray::stats {

using namespace opentelemetry::proto::metrics::v1;
using namespace opentelemetry::proto::collector::metrics::v1;
using namespace opentelemetry::proto::resource::v1;

OpenTelemetryStatsExporter::OpenTelemetryProtoExporter(
    std::shared_ptr<rpc::MetricsAgentClient> agent_client, const WorkerID &worker_id)
    : client_(std::move(agent_client)), worker_id_(worker_id) {}

void OpenTelemetryStatsExporter::Export(
    const ResourceMetrics &resource_metrics) {
    ExportMetricsServiceRequest request;
    *request.add_resource_metrics() = resource_metrics;
    SendData(request);
}

void OpenTelemetryStatsExporter::SendData(
    const ExportMetricsServiceRequest &request) {
    absl::MutexLock l(&mu_);
    client_->ExportMetrics(
        request, [](const Status &status, const rpc::ExportMetricsResponse &reply) {
        RAY_UNUSED(reply);
        if (!status.ok()) {
            RAY_LOG_EVERY_N(WARNING, 10000)
                << "Export OTEL metrics failed: " << status
                << ". This won't affect Ray, but you may lose metrics.";
        }
        });
}

}  // namespace ray::stats
