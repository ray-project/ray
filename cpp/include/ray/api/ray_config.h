
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
  // The redis address of existing cluster.
  std::string address;

  // Whether or not to run this program in a single process. This is used for debugging.
  bool local_mode = true;

  // The dynamic library path which contains remote fuctions of users.
  // Local mode does not need to set this parameter.
  std::string dynamic_library_path = "";

  /* The following are unstable parameters and their use is discouraged. */

  // Prevent external clients without the password from connecting.
  std::string redis_password_ = "5241590000000000";

  // Set argc and argv from command line. The params in argv will be parsed by `gflag`.
  // And Command line parameters have higher priority.
  void SetArgs(int *argc, char ***argv) {
    argc_ = argc;
    argv_ = argv;
  }
  int *argc_;
  char ***argv_;
};

}  // namespace api
}  // namespace ray