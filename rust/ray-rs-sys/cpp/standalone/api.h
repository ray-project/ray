#pragma once

#include "ray/core_worker/core_worker_options.h"
// #include "rust/cxx.h"

namespace ray {

class RayConfig {
 public:
  // The address of the Ray cluster to connect to.
  // If not provided, it will be initialized from environment variable "RAY_ADDRESS" by
  // default.
  std::string address = "";

  // Whether or not to run this application in a local mode. This is used for debugging.
  bool local_mode = false;

  // An array of directories or dynamic library files that specify the search path for
  // user code. This parameter is not used when the application runs in local mode.
  // Only searching the top level under a directory.
  std::vector<std::string> code_search_path;

  // The command line args to be appended as parameters of the `ray start` command. It
  // takes effect only if Ray head is started by a driver. Run `ray start --help` for
  // details.
  std::vector<std::string> head_args = {};

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  boost::optional<std::string> redis_password_;
};

void Init(ray::RayConfig &config, core::CoreWorkerOptions::TaskExecutionCallback callback,
          int argc, char **argv);

void StartWorkerWithCallback(core::CoreWorkerOptions::TaskExecutionCallback callback,
                             int argc, char **argv);

void SetConfigToWorker();

/// Initialize Ray runtime with config.
void Init(ray::RayConfig &config);

/// Initialize Ray runtime with config and command-line arguments.
/// If a parameter is explicitly set in command-line arguments, the parameter value will
/// be overwritten.
void Init(ray::RayConfig &config, int argc, char **argv);

/// Initialize Ray runtime with default config.
void Init();

/// Check if ray::Init has been called yet.
bool IsInitialized();

/// Shutdown Ray runtime.
void Shutdown();

}
