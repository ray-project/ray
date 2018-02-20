#ifndef TASK_SPECIFICATION_CC
#define TASK_SPECIFICATION_CC

#include "TaskSpecification.h"

namespace ray {

// TODO(atumanov): copy/paste most TaskSpec_* methods from task.h and make them
// methods of this class.
const char *TaskSpecification::Data() {
  return spec_.data();
}

size_t TaskSpecification::Size() {
  return spec_.size();
}

}

#endif
