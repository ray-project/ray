#include "ray/core_worker/common.h"

namespace ray {

std::string WorkerTypeString(WorkerType type) {
  if (type == WorkerType::DRIVER) {
    return "driver";
  } else if (type == WorkerType::WORKER) {
    return "worker";
  }
  RAY_CHECK(false);
  return "";
}

std::string LanguageString(Language language) {
  if (language == Language::PYTHON) {
    return "python";
  } else if (language == Language::JAVA) {
    return "java";
  }
  RAY_CHECK(false);
  return "";
}

}  // namespace ray
