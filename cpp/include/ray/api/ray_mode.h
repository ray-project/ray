
#pragma once

namespace ray { namespace api {

enum class RunMode { SINGLE_PROCESS, SINGLE_BOX, CLUSTER };

enum class WorkerMode { NONE, DRIVER, WORKER };

}  }// namespace ray::api