#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

class GcsServer;

// Represents a configuration value
struct ConfigValue {
  std::string value;
  std::string type;  // e.g., "string", "int", "float", "bool", "json"
  bool is_required;
  std::string description;
  std::string default_value;
  std::vector<std::string> allowed_values;  // Empty means any value is allowed
};

// Represents a configuration change
struct ConfigChange {
  std::string key;
  std::string old_value;
  std::string new_value;
  std::string source_node;
  int64_t timestamp;
};

class ConfigManager {
 public:
  ConfigManager(GcsServer &gcs_server, instrumented_io_context &io_context);
  ~ConfigManager() = default;

  // Start/stop the configuration manager
  Status Start();
  Status Stop();

  // Register a configuration key with metadata
  Status RegisterConfig(const std::string &key,
                       const ConfigValue &config);

  // Get configuration value
  Status GetConfig(const std::string &key,
                  std::string *value);

  // Get all configuration values
  Status GetAllConfig(std::unordered_map<std::string, std::string> *config);

  // Update configuration value
  Status UpdateConfig(const std::string &key,
                     const std::string &value);

  // Validate configuration value
  Status ValidateConfig(const std::string &key,
                       const std::string &value);

  // Apply configuration changes
  Status ApplyConfig(const std::string &key);

  // Get configuration metadata
  Status GetConfigMetadata(const std::string &key,
                          ConfigValue *config);

  // Get all configuration metadata
  Status GetAllConfigMetadata(std::unordered_map<std::string, ConfigValue> *config);

  // Get configuration change history
  Status GetConfigHistory(const std::string &key,
                         std::vector<ConfigChange> *history);

 private:
  // Validate configuration type
  Status ValidateConfigType(const std::string &value,
                          const std::string &type);

  // Apply configuration change
  Status ApplyConfigChange(const ConfigChange &change);

  // Reference to GCS server
  GcsServer &gcs_server_;

  // IO context
  instrumented_io_context &io_context_;

  // Configuration values
  std::unordered_map<std::string, std::string> config_values_;

  // Configuration metadata
  std::unordered_map<std::string, ConfigValue> config_metadata_;

  // Configuration change history
  std::unordered_map<std::string, std::vector<ConfigChange>> config_history_;

  // Maximum history size per key
  const size_t max_history_size_ = 100;
};

}  // namespace gcs
}  // namespace ray 