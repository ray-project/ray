#include <cstdlib>

#include "src/ray/util/logging.h"
#include "src/ray/util/util.h"

int main(int argc, char **argv) {
  setenv("RAY_ROTATION_MAX_BYTES", "10", /*overwrite=*/1);
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"/tmp/nov25_logging_test");
  RAY_LOG(ERROR) << "helloworld";
  RAY_LOG(ERROR) << "helloworld";
  return 0;
}
