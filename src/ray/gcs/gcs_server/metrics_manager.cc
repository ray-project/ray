#include "ray/gcs/gcs_server/metrics_manager.h"
#include "ray/gcs/gcs_server/gcs_server.h"

#include <regex>

namespace ray {
namespace gcs {

MetricsManager::MetricsManager(GcsServer &gcs_server,
                             instrumented_io_context &io_context)
    : gcs_server_(gcs_server), io_context_(io_context) {}

Status MetricsManager::Start() {
  // Initialize metrics and alerts
  metric_definitions_.clear();
  metric_values_.clear();
  alert_rules_.clear();
  active_alerts_.clear();
  alert_history_.clear();
  return Status::OK();
}

Status MetricsManager::Stop() {
  // Clear metrics and alerts
  metric_definitions_.clear();
  metric_values_.clear();
  alert_rules_.clear();
  active_alerts_.clear();
  alert_history_.clear();
  return Status::OK();
}

Status MetricsManager::RegisterMetric(const std::string &name,
                                    const MetricDefinition &definition) {
  // Check if metric already exists
  if (metric_definitions_.find(name) != metric_definitions_.end()) {
    return Status::Invalid("Metric already exists");
  }

  // Register metric definition
  metric_definitions_[name] = definition;
  return Status::OK();
}

Status MetricsManager::UpdateMetric(const std::string &name,
                                  double value,
                                  const std::vector<std::pair<std::string, std::string>> &labels) {
  // Check if metric exists
  auto def_it = metric_definitions_.find(name);
  if (def_it == metric_definitions_.end()) {
    return Status::NotFound("Metric not found");
  }

  // Validate labels
  if (labels.size() != def_it->second.label_names.size()) {
    return Status::Invalid("Invalid number of labels");
  }

  // Create label key
  std::string label_key;
  for (const auto &label : labels) {
    label_key += label.first + "=" + label.second + ";";
  }

  // Update metric value
  MetricValue metric_value;
  metric_value.value = value;
  metric_value.type = def_it->second.type;
  metric_value.unit = def_it->second.unit;
  metric_value.labels = labels;
  metric_value.timestamp = std::chrono::system_clock::now().time_since_epoch().count();

  metric_values_[name][label_key] = metric_value;

  // Evaluate alert rules for this metric
  EvaluateAlertRules();

  return Status::OK();
}

Status MetricsManager::GetMetric(const std::string &name,
                               const std::vector<std::pair<std::string, std::string>> &labels,
                               MetricValue *value) {
  // Check if metric exists
  auto values_it = metric_values_.find(name);
  if (values_it == metric_values_.end()) {
    return Status::NotFound("Metric not found");
  }

  // Create label key
  std::string label_key;
  for (const auto &label : labels) {
    label_key += label.first + "=" + label.second + ";";
  }

  // Get metric value
  auto value_it = values_it->second.find(label_key);
  if (value_it == values_it->second.end()) {
    return Status::NotFound("Metric value not found for given labels");
  }

  *value = value_it->second;
  return Status::OK();
}

Status MetricsManager::GetAllMetrics(std::vector<std::pair<std::string, MetricValue>> *metrics) {
  metrics->clear();

  for (const auto &metric_entry : metric_values_) {
    for (const auto &value_entry : metric_entry.second) {
      metrics->emplace_back(metric_entry.first, value_entry.second);
    }
  }

  return Status::OK();
}

Status MetricsManager::RegisterAlertRule(const AlertRule &rule) {
  // Check if alert rule already exists
  if (alert_rules_.find(rule.name) != alert_rules_.end()) {
    return Status::Invalid("Alert rule already exists");
  }

  // Check if metric exists
  if (metric_definitions_.find(rule.metric_name) == metric_definitions_.end()) {
    return Status::NotFound("Metric not found");
  }

  // Validate condition
  double threshold;
  std::string operator_type;
  RAY_RETURN_NOT_OK(ParseAlertCondition(rule.condition, &threshold, &operator_type));

  // Register alert rule
  alert_rules_[rule.name] = rule;
  return Status::OK();
}

Status MetricsManager::GetAlertRule(const std::string &name,
                                  AlertRule *rule) {
  auto it = alert_rules_.find(name);
  if (it == alert_rules_.end()) {
    return Status::NotFound("Alert rule not found");
  }

  *rule = it->second;
  return Status::OK();
}

Status MetricsManager::GetAllAlertRules(std::vector<AlertRule> *rules) {
  rules->clear();
  rules->reserve(alert_rules_.size());

  for (const auto &entry : alert_rules_) {
    rules->push_back(entry.second);
  }

  return Status::OK();
}

Status MetricsManager::GetActiveAlerts(std::vector<Alert> *alerts) {
  alerts->clear();
  alerts->reserve(active_alerts_.size());

  for (const auto &entry : active_alerts_) {
    alerts->push_back(entry.second);
  }

  return Status::OK();
}

Status MetricsManager::GetAlertHistory(const std::string &rule_name,
                                     std::vector<Alert> *history) {
  auto it = alert_history_.find(rule_name);
  if (it == alert_history_.end()) {
    return Status::NotFound("Alert rule not found");
  }

  *history = it->second;
  return Status::OK();
}

void MetricsManager::EvaluateAlertRules() {
  auto now = std::chrono::system_clock::now().time_since_epoch().count();

  for (const auto &rule_entry : alert_rules_) {
    const auto &rule = rule_entry.second;
    auto metric_it = metric_values_.find(rule.metric_name);
    if (metric_it == metric_values_.end()) {
      continue;
    }

    for (const auto &value_entry : metric_it->second) {
      const auto &metric_value = value_entry.second;

      // Parse condition
      double threshold;
      std::string operator_type;
      auto status = ParseAlertCondition(rule.condition, &threshold, &operator_type);
      if (!status.ok()) {
        continue;
      }

      // Check if condition is met
      bool is_triggered = IsAlertConditionMet(metric_value.value, threshold, operator_type);

      // Create or update alert
      Alert alert;
      alert.rule_name = rule.name;
      alert.metric_name = rule.metric_name;
      alert.metric_value = metric_value.value;
      alert.severity = rule.severity;
      alert.message = rule.message;
      alert.timestamp = now;
      alert.is_active = is_triggered;

      if (is_triggered) {
        active_alerts_[rule.name] = alert;
      } else {
        active_alerts_.erase(rule.name);
      }

      // Add to history
      auto &history = alert_history_[rule.name];
      history.push_back(alert);

      // Trim history if needed
      if (history.size() > max_history_size_) {
        history.erase(history.begin());
      }
    }
  }
}

Status MetricsManager::ParseAlertCondition(const std::string &condition,
                                         double *threshold,
                                         std::string *operator_type) {
  std::regex condition_regex(R"(^\s*(>|<|>=|<=|==|!=)\s*([0-9]+\.?[0-9]*)\s*$)");
  std::smatch matches;

  if (!std::regex_match(condition, matches, condition_regex)) {
    return Status::Invalid("Invalid alert condition format");
  }

  *operator_type = matches[1].str();
  *threshold = std::stod(matches[2].str());
  return Status::OK();
}

bool MetricsManager::IsAlertConditionMet(double value,
                                       double threshold,
                                       const std::string &operator_type) {
  if (operator_type == ">") {
    return value > threshold;
  } else if (operator_type == "<") {
    return value < threshold;
  } else if (operator_type == ">=") {
    return value >= threshold;
  } else if (operator_type == "<=") {
    return value <= threshold;
  } else if (operator_type == "==") {
    return value == threshold;
  } else if (operator_type == "!=") {
    return value != threshold;
  }
  return false;
}

}  // namespace gcs
}  // namespace ray 