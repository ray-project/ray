#include <gflags/gflags.h>
#include <signal.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "ray/object_manager/plasma/store_runner.h"

#ifdef __linux__
#define SHM_DEFAULT_PATH "/dev/shm"
#else
#define SHM_DEFAULT_PATH "/tmp"
#endif

// Command-line flags.
DEFINE_string(d, SHM_DEFAULT_PATH, "directory where to create the memory-backed file");
DEFINE_bool(h, false, "whether to enable hugepage support");
DEFINE_string(s, "",
              "socket name where the Plasma store will listen for requests, required");
DEFINE_int64(m, -1, "amount of memory in bytes to use for Plasma store, required");
DEFINE_bool(z, false, "Run idle as a placeholder, optional");

// Function to use (instead of ARROW_LOG(FATAL)) for usage, etc. errors before
// the main server loop starts, so users don't get a backtrace if they
// simply forgot a command-line switch.
void ExitWithUsageError(const char *error_msg) {
  std::cerr << gflags::ProgramInvocationShortName() << ": " << error_msg << std::endl;
  exit(1);
}

void HandleSignal(int signal) {
  if (signal == SIGTERM) {
    RAY_LOG(INFO) << "SIGTERM Signal received, closing Plasma Server...";
    plasma::plasma_store_runner->Stop();
  }
}

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage("Plasma store server.\nUsage: ");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();
  // Directory where plasma memory mapped files are stored.
  std::string plasma_directory = FLAGS_d;
  bool hugepages_enabled = FLAGS_h;
  std::string socket_name = FLAGS_s;
  int64_t system_memory = FLAGS_m;
  bool keep_idle = FLAGS_z;

  if (!keep_idle) {
    if (socket_name.empty() || system_memory == -1) {
      // Nicer error message for the case where the user ran the program without
      // any of the required command-line switches.
      ExitWithUsageError(
          "please specify socket for incoming connections with -s, "
          "and the amount of memory (in bytes) to use with -m");
    } else if (socket_name.empty()) {
      ExitWithUsageError("please specify socket for incoming connections with -s");
    } else if (system_memory == -1) {
      ExitWithUsageError("please specify the amount of memory (in bytes) to use with -m");
    }
    RAY_CHECK(!plasma_directory.empty());

    plasma::plasma_store_runner.reset(new plasma::PlasmaStoreRunner(
        socket_name, system_memory, hugepages_enabled, plasma_directory));
    // Install signal handler before starting the eventloop.
#ifndef _WIN32  // TODO(mehrdadn): Is there an equivalent of this we need for Windows?
    // Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
    // to a client that has already died, the store could die.
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGTERM, HandleSignal);
    plasma::plasma_store_runner->Start();
    plasma::plasma_store_runner.reset();
  } else {
    RAY_LOG(INFO) << "The Plasma Store is started with the '-z' flag, "
                  << "and it will run idle as a placeholder.";
    while (true) {
      std::this_thread::sleep_for(std::chrono::hours(1000));
    }
  }

  return 0;
}
