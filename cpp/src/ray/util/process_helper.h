#pragma once
#include <string>

#include "../config_internal.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace api {

class ProcessHelper {
 public:
  void RayStart(CoreWorkerOptions::TaskExecutionCallback callback);
  void RayStop();

  static ProcessHelper &GetInstance() {
    static ProcessHelper processHelper;
    return processHelper;
  }

  ProcessHelper(ProcessHelper const &) = delete;
  void operator=(ProcessHelper const &) = delete;

 private:
  ProcessHelper(){};
};
}  // namespace api
}  // namespace ray