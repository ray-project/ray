
#pragma once
#include <ray/api/ray_exception.h>
#include <memory>
#include <string>
#include "boost/optional.hpp"

namespace ray {
namespace api {

class RayConfig {
 public:
  // The address of the Ray cluster to connect to.
  std::string address = "";

  // Whether or not to run this application in a local mode. This is used for debugging.
  bool local_mode = false;

  // The dynamic library path which contains remote fuctions of users.
  // This parameter is not used when the application runs in local mode.
  // TODO(guyang.sgy): Put this param into job config instead.
  std::string dynamic_library_path = "";

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  boost::optional<std::string> redis_password_;
};

}  // namespace api
}  // namespace ray