
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

class GcsServer;

// Represents a metric value
struct MetricValue {
  double value;
  std::string type;  // e.g., "counter", "gauge", "histogram"
  std::string unit;  // e.g., "bytes", "seconds", "count"
  std::vector<std::pair<std::string, std::string>> labels;
  int64_t timestamp;
};

// Represents a metric definition
struct MetricDefinition {
  std::string name;
  std::string description;
  std::string type;
  std::string unit;
  std::vector<std::string> label_names;
};

// Represents an alert rule
struct AlertRule {
  std::string name;
  std::string metric_name;
  std::string condition;  // e.g., "> 0.8", "< 100"
  std::string severity;   // e.g., "info", "warning", "error", "critical"
  std::string message;
  int64_t evaluation_interval;  // in milliseconds
};

// Represents an alert
struct Alert {
  std::string rule_name;
  std::string metric_name;
  double metric_value;
  std::string severity;
  std::string message;
  int64_t timestamp;
  bool is_active;
};

class MetricsManager {
 public:
  MetricsManager(GcsServer &gcs_server, instrumented_io_context &io_context);
  ~MetricsManager() = default;

  // Start/stop the metrics manager
  Status Start();
  Status Stop();

  // Register a new metric
  Status RegisterMetric(const std::string &name,
                       const MetricDefinition &definition);

  // Update metric value
  Status UpdateMetric(const std::string &name,
                     double value,
                     const std::vector<std::pair<std::string, std::string>> &labels);

  // Get metric value
  Status GetMetric(const std::string &name,
                  const std::vector<std::pair<std::string, std::string>> &labels,
                  MetricValue *value);

  // Get all metrics
  Status GetAllMetrics(std::vector<std::pair<std::string, MetricValue>> *metrics);

  // Register an alert rule
  Status RegisterAlertRule(const AlertRule &rule);

  // Get alert rule
  Status GetAlertRule(const std::string &name,
                     AlertRule *rule);

  // Get all alert rules
  Status GetAllAlertRules(std::vector<AlertRule> *rules);

  // Get active alerts
  Status GetActiveAlerts(std::vector<Alert> *alerts);

  // Get alert history
  Status GetAlertHistory(const std::string &rule_name,
                        std::vector<Alert> *history);

 private:
  // Evaluate alert rules
  void EvaluateAlertRules();

  // Parse alert condition
  Status ParseAlertCondition(const std::string &condition,
                           double *threshold,
                           std::string *operator_type);

  // Check if alert condition is met
  bool IsAlertConditionMet(double value,
                          double threshold,
                          const std::string &operator_type);

  // Reference to GCS server
  GcsServer &gcs_server_;

  // IO context
  instrumented_io_context &io_context_;

  // Metric definitions
  std::unordered_map<std::string, MetricDefinition> metric_definitions_;

  // Metric values
  std::unordered_map<std::string, std::unordered_map<std::string, MetricValue>> metric_values_;

  // Alert rules
  std::unordered_map<std::string, AlertRule> alert_rules_;

  // Active alerts
  std::unordered_map<std::string, Alert> active_alerts_;

  // Alert history
  std::unordered_map<std::string, std::vector<Alert>> alert_history_;

  // Maximum history size per alert rule
  const size_t max_history_size_ = 1000;
};

}  // namespace gcs
}  // namespace ray 
#pragma once
 