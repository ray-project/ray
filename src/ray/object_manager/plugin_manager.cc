#include "ray/object_manager/plugin_manager.h"

#include "nlohmann/json.hpp"

using json = nlohmann::json;

namespace ray {

using DefaultClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();
using DefaultRunnerCreator = std::unique_ptr<plasma::ObjectStoreRunnerInterface> (*)(
                                std::string,
                                int64_t,
                                bool,
                                std::string,
                                std::string);
using PluginRunnerCreator = std::unique_ptr<plasma::ObjectStoreRunnerInterface> (*)();
using PluginClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();

PluginManager &PluginManager::GetInstance() {
  static PluginManager instance;
  return instance;
}

/// A creator function of default object store client (PlasmaClient).
std::shared_ptr<plasma::ObjectStoreClientInterface> CreateDefaultClientInstance() {
  return std::make_shared<plasma::PlasmaClient>();
}

/// A creator function of default object store runner (PlasmaStoreRunner).
std::unique_ptr<plasma::ObjectStoreRunnerInterface> CreateDefaultRunnerInstance(
    std::string store_socket_name,
    int64_t object_store_memory,
    bool huge_pages,
    std::string plasma_directory,
    std::string fallback_directory) {
  return std::make_unique<plasma::PlasmaStoreRunner>(store_socket_name,
                                                     object_store_memory,
                                                     huge_pages,
                                                     plasma_directory,
                                                     fallback_directory);
}

void PluginManager::LoadObjectStorePlugin() {
  void *handle = dlopen(plugin_path_.c_str(), RTLD_NOW);
  if (!handle) {
    std::cerr << "Failed to load shared library: " << dlerror() << std::endl;
    return;
  }

  PluginClientCreator client_creator =
      reinterpret_cast<PluginClientCreator>(dlsym(handle, "CreateClient"));
  if (!client_creator) {
    std::cerr << "Failed to get CreateClient function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }
  client_creators_[plugin_name_] = client_creator;

  PluginRunnerCreator runner_creator =
      reinterpret_cast<PluginRunnerCreator>(dlsym(handle, "CreateRunner"));
  if (!runner_creator) {
    std::cerr << "Failed to get CreateRunner function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }
  runner_creators_[plugin_name_] = runner_creator;

  dlclose(handle);
  return;
}

std::shared_ptr<plasma::ObjectStoreClientInterface>
PluginManager::CreateObjectStoreClientInstance(const std::string &plugin_name) {

  if (plugin_name == "default") {
    return std::any_cast<DefaultClientCreator>(client_creators_[plugin_name])();
  }
  return std::any_cast<PluginClientCreator>(client_creators_[plugin_name])();
}

std::unique_ptr<plasma::ObjectStoreRunnerInterface>
PluginManager::CreateObjectStoreRunnerInstance(const std::string &plugin_name) {
  if (plugin_name == "default") {
    return std::any_cast<DefaultRunnerCreator>(runner_creators_[plugin_name])(
        default_runner_params_.store_socket_name,
        default_runner_params_.object_store_memory,
        default_runner_params_.huge_pages,
        default_runner_params_.plasma_directory,
        default_runner_params_.fallback_directory);
  }
  return std::any_cast<PluginRunnerCreator>(runner_creators_[plugin_name])();
}

void PluginManager::LoadObjectStoreClientPlugin() {
  void *handle = dlopen(plugin_path_.c_str(), RTLD_NOW);
  if (!handle) {
    std::cerr << "Failed to load shared library: " << dlerror() << std::endl;
    return;
  }

  PluginClientCreator client_creator =
      reinterpret_cast<PluginClientCreator>(dlsym(handle, "CreateClient"));
  if (!client_creator) {
    std::cerr << "Failed to get CreateClient function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }

  client_creators_[plugin_name_] = client_creator;
  dlclose(handle);
  return;
}

void PluginManager::SetObjectStoreClients(const std::string plugin_name,
                                          const std::string plugin_path,
                                          const std::string plugin_params) {
  plugin_name_ = plugin_name;
  plugin_path_ = plugin_path;
  plugin_params_ = plugin_params;

  if (plugin_name != "default") {
    LoadObjectStoreClientPlugin();
  } else {
    default_client_creator_ = &CreateDefaultClientInstance;
    client_creators_[plugin_name] = default_client_creator_;
  }
}

void PluginManager::SetObjectStores(const std::string plugin_name,
                                    const std::string plugin_path,
                                    const std::string plugin_params) {
  plugin_name_ = plugin_name;
  plugin_path_ = plugin_path;
  plugin_params_ = plugin_params;

  // LoadPlugin
  if (plugin_name != "default") {
    LoadObjectStorePlugin();
  } else {

    // Parse the plugin parameters
    json params_map = json::parse(plugin_params);
    std::string store_socket_name = params_map.value("store_socket_name", "");
    int64_t object_store_memory = params_map["object_store_memory"];
    bool huge_pages = params_map.value("huge_pages", false);
    std::string plasma_directory = params_map.value("plasma_directory", "");
    std::string fallback_directory = params_map.value("fallback_directory", "");


    default_runner_params_.store_socket_name = store_socket_name;
    default_runner_params_.object_store_memory = object_store_memory;
    default_runner_params_.huge_pages = huge_pages;
    default_runner_params_.plasma_directory = plasma_directory;
    default_runner_params_.fallback_directory = fallback_directory;

    default_client_creator_ = &CreateDefaultClientInstance;
    default_runner_creator_ = &CreateDefaultRunnerInstance;

    client_creators_["default"] = default_client_creator_;
    runner_creators_["default"] = default_runner_creator_;

  }
}

}  // namespace ray

