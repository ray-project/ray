
#include "./config_internal.h"
#include "./process_helper.h"

#include "ray/core_worker/core_worker_options.h"

namespace ray {

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
