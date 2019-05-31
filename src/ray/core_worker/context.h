#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include "common.h"
#include "ray/raylet/task_spec.h"

namespace ray {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const DriverID &driver_id);

  int GetNextTaskIndex();

  int GetNextPutIndex();

  const DriverID &GetCurrentDriverID();

  const TaskID &GetCurrentTaskID();

  void SetCurrentTask(const raylet::TaskSpecification &spec);

 public:
  /// Type of the worker.
  const WorkerType worker_type;

  /// ID for this worker.
  const ClientID worker_id;

  /// Driver ID for this worker.
  DriverID current_driver_id;

 private:
  WorkerThreadContext& GetThreadContext();

  static WorkerThreadContext& GetThreadContext(const enum WorkerType worker_type, DriverID driver_id);

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context;

};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H