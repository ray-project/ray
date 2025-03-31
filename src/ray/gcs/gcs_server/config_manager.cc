#include "ray/gcs/gcs_server/config_manager.h"
#include "ray/gcs/gcs_server/gcs_server.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace ray {
namespace gcs {

ConfigManager::ConfigManager(GcsServer &gcs_server,
                           instrumented_io_context &io_context)
    : gcs_server_(gcs_server), io_context_(io_context) {}

Status ConfigManager::Start() {
  // Initialize configuration
  config_values_.clear();
  config_metadata_.clear();
  config_history_.clear();
  return Status::OK();
}

Status ConfigManager::Stop() {
  // Clear configuration
  config_values_.clear();
  config_metadata_.clear();
  config_history_.clear();
  return Status::OK();
}

Status ConfigManager::RegisterConfig(const std::string &key,
                                  const ConfigValue &config) {
  // Check if config already exists
  if (config_metadata_.find(key) != config_metadata_.end()) {
    return Status::Invalid("Configuration key already exists");
  }

  // Register config metadata
  config_metadata_[key] = config;

  // Set default value if provided
  if (!config.default_value.empty()) {
    RAY_RETURN_NOT_OK(UpdateConfig(key, config.default_value));
  }

  return Status::OK();
}

Status ConfigManager::GetConfig(const std::string &key,
                              std::string *value) {
  // Check if config exists
  auto it = config_values_.find(key);
  if (it == config_values_.end()) {
    return Status::NotFound("Configuration key not found");
  }

  *value = it->second;
  return Status::OK();
}

Status ConfigManager::GetAllConfig(std::unordered_map<std::string, std::string> *config) {
  *config = config_values_;
  return Status::OK();
}

Status ConfigManager::UpdateConfig(const std::string &key,
                                 const std::string &value) {
  // Validate config
  RAY_RETURN_NOT_OK(ValidateConfig(key, value));

  // Create config change
  ConfigChange change;
  change.key = key;
  change.old_value = config_values_[key];
  change.new_value = value;
  change.source_node = gcs_server_.GetSelfNodeId();
  change.timestamp = std::chrono::system_clock::now().time_since_epoch().count();

  // Apply config change
  RAY_RETURN_NOT_OK(ApplyConfigChange(change));

  return Status::OK();
}

Status ConfigManager::ValidateConfig(const std::string &key,
                                   const std::string &value) {
  // Check if config exists
  auto it = config_metadata_.find(key);
  if (it == config_metadata_.end()) {
    return Status::NotFound("Configuration key not found");
  }

  const auto &metadata = it->second;

  // Check if value is allowed
  if (!metadata.allowed_values.empty()) {
    bool found = false;
    for (const auto &allowed : metadata.allowed_values) {
      if (value == allowed) {
        found = true;
        break;
      }
    }
    if (!found) {
      return Status::Invalid("Value not in allowed values list");
    }
  }

  // Validate type
  RAY_RETURN_NOT_OK(ValidateConfigType(value, metadata.type));

  return Status::OK();
}

Status ConfigManager::ApplyConfig(const std::string &key) {
  // Check if config exists
  auto it = config_values_.find(key);
  if (it == config_values_.end()) {
    return Status::NotFound("Configuration key not found");
  }

  // TODO: Implement actual configuration application logic
  // This would involve:
  // 1. Notifying relevant components of the configuration change
  // 2. Updating system state based on the new configuration
  // 3. Handling any errors that occur during application

  return Status::OK();
}

Status ConfigManager::GetConfigMetadata(const std::string &key,
                                     ConfigValue *config) {
  auto it = config_metadata_.find(key);
  if (it == config_metadata_.end()) {
    return Status::NotFound("Configuration key not found");
  }

  *config = it->second;
  return Status::OK();
}

Status ConfigManager::GetAllConfigMetadata(
    std::unordered_map<std::string, ConfigValue> *config) {
  *config = config_metadata_;
  return Status::OK();
}

Status ConfigManager::GetConfigHistory(const std::string &key,
                                    std::vector<ConfigChange> *history) {
  auto it = config_history_.find(key);
  if (it == config_history_.end()) {
    return Status::NotFound("Configuration key not found");
  }

  *history = it->second;
  return Status::OK();
}

Status ConfigManager::ValidateConfigType(const std::string &value,
                                      const std::string &type) {
  if (type == "string") {
    return Status::OK();
  } else if (type == "int") {
    try {
      std::stoi(value);
    } catch (const std::exception &) {
      return Status::Invalid("Invalid integer value");
    }
  } else if (type == "float") {
    try {
      std::stof(value);
    } catch (const std::exception &) {
      return Status::Invalid("Invalid float value");
    }
  } else if (type == "bool") {
    if (value != "true" && value != "false") {
      return Status::Invalid("Invalid boolean value");
    }
  } else if (type == "json") {
    rapidjson::Document d;
    if (d.Parse(value.c_str()).HasParseError()) {
      return Status::Invalid("Invalid JSON value");
    }
  } else {
    return Status::Invalid("Unknown configuration type");
  }

  return Status::OK();
}

Status ConfigManager::ApplyConfigChange(const ConfigChange &change) {
  // Update value
  config_values_[change.key] = change.new_value;

  // Add to history
  auto &history = config_history_[change.key];
  history.push_back(change);

  // Trim history if needed
  if (history.size() > max_history_size_) {
    history.erase(history.begin());
  }

  return Status::OK();
}

}  // namespace gcs
}  // namespace ray 