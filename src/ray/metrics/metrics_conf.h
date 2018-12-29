#ifndef RAY_METRICS_METRICS_CONF_H
#define RAY_METRICS_METRICS_CONF_H

#include "ray/metrics/registry/metrics_registry_interface.h"
#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

/// \class MetricsConf
///
/// The configuration of metrics.
class MetricsConf {
 public:
  /// Create a configuration from the given config string.
  ///
  /// \param conf_str The string of configuration. The items of
  /// it should be joined with comma like "k1,v1,k2,v2[,...]".
  explicit MetricsConf(const std::string &conf_str = "");

  virtual ~MetricsConf() = default;

  /// Get the options of registry.
  ///
  /// \return The options of registry.
  const RegistryOption &GetRegistryOption() const;

  /// Get the options of reporter.
  ///
  /// \return The options of reporter.
  const ReporterOption &GetReporterOption() const;

  /// Get the name of registry implementation.
  ///
  /// \return The name of registry implementation.
  const std::string &GetRegistryName() const;

  /// Get the name of reporter implementation.
  ///
  /// \return The name of reporter implementation.
  const std::string &GetReporterName() const;

 protected:
  /// Initialize the configuration from the given config string.
  ///
  /// \param conf_str The string of configuration.
  void Init(const std::string &conf_str);

 protected:
  std::string registry_name_;
  RegistryOption registry_options_;

  std::string reporter_name_;
  ReporterOption reporter_options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_CONF_H
