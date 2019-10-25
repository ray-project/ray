#ifndef RAY_CORE_WORKER_TASK_EXECUTION_H
#define RAY_CORE_WORKER_TASK_EXECUTION_H

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/profiling.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/worker_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

class CoreWorker;

namespace raylet {
class TaskSpecification;
}

/// The interface that contains all `CoreWorker` methods that are related to task
/// execution.
class CoreWorkerTaskExecutionInterface {
 public:

  CoreWorkerTaskExecutionInterface(CoreWorker &core_worker);

  /// Start receiving and executing tasks.
  /// \return void.
  void Run();

  /// Stop receiving and executing tasks.
  /// \return void.
  void Stop();

 private:

  /// Reference to the parent CoreWorker.
  /// TODO(edoakes) this is very ugly, but unfortunately necessary so that we
  /// can clear the ActorHandle state when we start executing a task. Two
  /// possible solutions are to either move the ActorHandle state into the
  /// WorkerContext or to remove this interface entirely.
  CoreWorker &core_worker_;

  friend class CoreWorker;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
