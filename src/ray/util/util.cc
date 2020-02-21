#include "ray/util/util.h"

InitShutdownRAII::~InitShutdownRAII() {
  if (shutdown_ != nullptr) {
    shutdown_();
  }
}
