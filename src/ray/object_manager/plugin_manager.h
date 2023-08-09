#pragma once

#include <dlfcn.h>
#include <iostream>
#include <map>
#include <memory>
#include "ray/util/logging.h"

#include "ray/object_manager/plasma/object_store_client_interface.h"
#include "ray/object_manager/plasma/object_store_runner_interface.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/object_manager/plasma/store_runner.h"


namespace ray {

class PluginManager {
  public:

  using DefaultClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();
  using DefaultRunnerCreator = std::unique_ptr<plasma::ObjectStoreRunnerInterface> (*)(
                                   std::string,
                                   int64_t,
                                   bool,
                                   std::string,
                                   std::string);


  /// Create a object store runner instance that is currently in use.
  ///
  /// \param plugin_name The name of the object store runner plugin.
  std::unique_ptr<plasma::ObjectStoreRunnerInterface> CreateObjectStoreRunnerInstance(
      const std::string& plugin_name);


  /// Create a object store client instance that is currently in use.
  ///
  /// \param plugin_name The name of the object store client plugin.
  std::shared_ptr<plasma::ObjectStoreClientInterface> CreateObjectStoreClientInstance(
      const std::string& plugin_name);


  /// Set up object store clients according to the name of the plugin, the path
  /// of the plugin shared library, and also the provided plugin parameters.
  /// If the plugin name is 'default', it would load the current PlasmaClient as the
  /// client instance; if the plugin name is other than 'default', it would load the
  /// client creators function from the shared library provided from the plugin path.
  ///
  /// \param plugin_name The name of the object store client plugin.
  /// \param plugin_path The path to the plugin shared library.
  /// \param plugin_params The parameters to the plugin.
  void SetObjectStoreClients(const std::string plugin_name,
                             const std::string plugin_path,
                             const std::string plugin_params);


  /// Set up both object store clients and runners according to the name of the
  /// plugin, the path of the plugin shared library, and also the provided
  /// plugin parameters.
  ///
  /// If the plugin name is 'default', it would load the current PlasmaClient and
  /// PlasmaStoreRunner as the client/store runner instance; if the plugin name
  /// is other than 'default', it would load the client/runner creators function
  /// from the shared library provided by the plugin path.
  ///
  /// \param plugin_name The name of the object store client plugin.
  /// \param plugin_path The path to the plugin shared library.
  /// \param plugin_params The parameters to the plugin.
  void SetObjectStores(const std::string plugin_name, 
                       const std::string plugin_path,
                       const std::string plugin_params);

  /// Load the object Store client creators and store runner creators from the
  /// shared library.
  ///
  void LoadObjectStorePlugin();

  /// Load the object store client creators from the shared library.
  ///
  void LoadObjectStoreClientPlugin();


  /// Get an instance from the singleton PluginManager.
  ///
  static PluginManager &GetInstance();

 private:
  PluginManager(const PluginManager&) = delete;
  PluginManager(PluginManager &&) = delete;
  PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager &&) = delete;
  PluginManager(){}
  ~PluginManager(){}

  /// Define parameter struct for creating default runner
  struct DefaultRunnerParams {
    std::string store_socket_name;
    int64_t object_store_memory;
    bool huge_pages;
    std::string plasma_directory;
    std::string fallback_directory;
  };

  /// Parameter struct for creating default object store runner
  DefaultRunnerParams default_runner_params_;

  /// Object store client instance creators
  std::map<std::string, std::any> client_creators_;

  /// Object store runner instance creators
  std::map<std::string, std::any> runner_creators_;

  /// Creator for default object store client (original plasma client)
  DefaultClientCreator  default_client_creator_;

  /// Creator for default object store runner (original plasma store runner)
  DefaultRunnerCreator  default_runner_creator_;

  /// Plugin name
  std::string plugin_name_;

  /// Plugin path
  std::string plugin_path_;

  /// Plugin parameters
  std::string plugin_params_;

};

} // namespace ray
