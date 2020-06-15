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

#ifndef RAY_METRIC_EXPORTER_CLIENT_H
#define RAY_METRIC_EXPORTER_CLIENT_H
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

#include "ray/stats/metric.h"

namespace ray {
namespace stats {
/// Interface class for abstract metrics exporter client.
class MetricExporterClient {
 public:
  virtual void ReportMetrics(const MetricPoints &points) = 0;
};

/// Default stdout exporter client can log metrics info for debug.
class StdoutExporterClient : public MetricExporterClient {
 public:
  void ReportMetrics(const MetricPoints &points) override;
};

/// The decoration mode is that the user can apply it by configuring different
/// combinations.
/// Usage:
/// std::shared_ptr<MetricExporterClient> exporter(new StdoutExporterClient());
/// std::shared_ptr<MetricExporterClient> gcs_exporter_client(
///         new GcsExporterClient(exporter, gcs_rpc_client));
///  Both gcs rpc and std logging will emit when
//  gcs_exporter_client->ReportMetrics(points) is called.
/// Actually, opentsdb exporter can be added like above mentioned style.
class MetricExporterDecorator : public MetricExporterClient {
 public:
  MetricExporterDecorator(std::shared_ptr<MetricExporterClient> exporter);
  virtual void ReportMetrics(const MetricPoints &points);

 private:
  std::shared_ptr<MetricExporterClient> exporter_;
};

/// GcsExporterClient is used for exporting metrics to GCS server via RPC.
class GcsExporterClient : public MetricExporterDecorator {
 public:
  GcsExporterClient(std::shared_ptr<MetricExporterClient> exporter,
                    rpc::GcsRpcClient &gcs_rpc_client);
  void ReportMetrics(const MetricPoints &points) override;

 private:
  rpc::GcsRpcClient &gcs_rpc_client_;
  static constexpr uint32_t kMetricExporterRpcTimeout = 2000;
};

class OpentsdbExporterClient : public MetricExporterDecorator {
 public:
  OpentsdbExporterClient(std::shared_ptr<MetricExporterClient> exporter)
      : MetricExporterDecorator(exporter) {}
  void ReportMetrics(const MetricPoints &points) override {
    // TODO(lingxuan.zlx): opentsdb client is used for report to backend
    // storage.
  }
};

}  // namespace stats
}  // namespace ray
#endif
