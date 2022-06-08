#pragma once
#include "nlohmann/json.hpp"
#include <nlohmann/json-schema.hpp>
#include <fstream>

using json = nlohmann::json;
using nlohmann::json_schema::json_validator;

class RuntimeEnvPluginSchemaManager {
  public:
    static RuntimeEnvPluginSchemaManager &GetInstance() {
      static RuntimeEnvPluginSchemaManager schema_manager;
      return schema_manager;
    }

    void LoadSchemas(const std::vector<std::string> &schema_paths) {
      for (const auto &path : schema_paths) {
        std::ifstream ifs(path.c_str());
        json schema;
        ifs >> schema;
        if (!schema.contains("title")) {
          std::cerr << "No valid title in "<< path << "\n";
          continue;
        }
         std::string name;
        schema.at("title").get_to(name);
        std::shared_ptr<json_validator> validator(new json_validator);
        try {
            validator->set_root_schema(schema);
        } catch (const std::exception &e) {
            std::cerr << "Load schema " << path << " failed: " << e.what() << "\n";
            continue;
        }
        if (schemas_.find(name) != schemas_.end()) {
          std::cerr << "The schema " << path << " 'title' already exists" << "\n";
          continue;
        }
        schemas_.emplace(name, validator);
      }
    }

    void validate(const std::string name, json &j) {
        if (schemas_.find(name) == schemas_.end()) {
          throw std::invalid_argument("Invalid plugin name " + name);
        }
        schemas_[name]->validate(j);
    }

  private:
    std::unordered_map<std::string, std::shared_ptr<json_validator>> schemas_;
};