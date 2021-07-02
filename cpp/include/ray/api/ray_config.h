
#pragma once
#include <ray/api/ray_exception.h>
#include <list>
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

  // A list of directories or dynamic library files that specify the search path for user
  // code. This parameter is not used when the application runs in local mode.
  std::list<std::string> code_search_path;

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  boost::optional<std::string> redis_password_;
};

}  // namespace api
}  // namespace ray