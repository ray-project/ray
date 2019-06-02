#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include "common.h"
#include "ray/raylet/task_spec.h"

namespace ray {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const DriverID &driver_id);

  const WorkerType GetWorkerType() const;

  const ClientID &GetWorkerID() const;

  const DriverID &GetCurrentDriverID() const;

  const TaskID &GetCurrentTaskID() const;

  void SetCurrentTask(const raylet::TaskSpecification &spec);

  int GetNextTaskIndex();

  int GetNextPutIndex();

 private:
  /// Type of the worker.
  const WorkerType worker_type;

  /// ID for this worker.
  const ClientID worker_id;

  /// Driver ID for this worker.
  DriverID current_driver_id;

 private:
  static WorkerThreadContext &GetThreadContext();

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H
