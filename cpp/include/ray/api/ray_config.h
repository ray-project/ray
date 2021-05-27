
#pragma once
#include <ray/api/common_types.h>
#include <ray/api/logging.h>
#include <ray/api/ray_exception.h>
#include <memory>
#include <string>

namespace ray {
namespace api {

class RayConfig {
 public:
  // The address of the Ray cluster to connect to.
  std::string address;

  // Whether or not to run this application in a local mode. This is used for debugging.
  bool local_mode = true;

  // The dynamic library path which contains remote fuctions of users.
  // This parameter is not used when the application runs in local mode.
  // TODO(guyang.sgy): Put this param into job config instead.
  std::string dynamic_library_path = "";

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  std::string redis_password_ = "5241590000000000";

  // Update config values based on command-line arguments. If a parameter is explicitly
  // set in command-line arguments, the parameter value will be updated, otherwise, the
  // parameter value will stay unchanged.
  void SetArgs(int *argc, char ***argv) {
    argc_ = argc;
    argv_ = argv;
  }
  int *argc_;
  char ***argv_;
};

}  // namespace api
}  // namespace ray