#ifndef GLOBAL_SCHEDULER_ALGORITHM_H
#define GLOBAL_SCHEDULER_ALGORITHM_H

#include <random>

#include "common.h"
#include "global_scheduler.h"
#include "task.h"

/* ==== The scheduling algorithm ====
 *
 * This file contains declaration for all functions and data structures that
 * need to be provided if you want to implement a new algorithm for the global
 * scheduler.
 *
 */

enum class GlobalSchedulerAlgorithm {
  SCHED_ALGORITHM_ROUND_ROBIN = 1,
  SCHED_ALGORITHM_TRANSFER_AWARE = 2,
  SCHED_ALGORITHM_MAX
};

/// The class encapsulating state managed by the global scheduling policy.
class GlobalSchedulerPolicyState {
 public:
  GlobalSchedulerPolicyState(int64_t round_robin_index)
      : round_robin_index_(round_robin_index), gen_(rd_()) {}

  GlobalSchedulerPolicyState() : round_robin_index_(0), gen_(rd_()) {}

  /// Return the policy's random number generator.
  ///
  /// @return The policy's random number generator.
  std::mt19937_64 &getRandomGenerator() { return gen_; }

  /// Return the round robin index maintained by policy state.
  ///
  /// @return The round robin index.
  int64_t getRoundRobinIndex() const { return round_robin_index_; }

 private:
  /// The index of the next local scheduler to assign a task to.
  int64_t round_robin_index_;
  /// Internally maintained random number engine device.
  std::random_device rd_;
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
};

/**
 * Create the state of the global scheduler policy. This state must be freed by
 * the caller.
 *
 * @return The state of the scheduling policy.
 */
GlobalSchedulerPolicyState *GlobalSchedulerPolicyState_init(void);

/**
 * Free the global scheduler policy state.
 *
 * @param policy_state The policy state to free.
 * @return Void.
 */
void GlobalSchedulerPolicyState_free(GlobalSchedulerPolicyState *policy_state);

/**
 * Main new task handling function in the global scheduler.
 *
 * @param state Global scheduler state.
 * @param policy_state State specific to the scheduling policy.
 * @param task New task to be scheduled.
 * @return True if the task was assigned to a local scheduler and false
 *         otherwise.
 */
bool handle_task_waiting(GlobalSchedulerState *state,
                         GlobalSchedulerPolicyState *policy_state,
                         Task *task);

/**
 * Handle the fact that a new object is available.
 *
 * @param state The global scheduler state.
 * @param policy_state The state managed by the scheduling policy.
 * @param object_id The ID of the object that is now available.
 * @return Void.
 */
void handle_object_available(GlobalSchedulerState *state,
                             GlobalSchedulerPolicyState *policy_state,
                             ObjectID object_id);

/**
 * Handle a heartbeat message from a local scheduler. TODO(rkn): this is a
 * placeholder for now.
 *
 * @param state The global scheduler state.
 * @param policy_state The state managed by the scheduling policy.
 * @return Void.
 */
void handle_local_scheduler_heartbeat(GlobalSchedulerState *state,
                                      GlobalSchedulerPolicyState *policy_state);

/**
 * Handle the presence of a new local scheduler. Currently, this just adds the
 * local scheduler to a queue of local schedulers.
 *
 * @param state The global scheduler state.
 * @param policy_state The state managed by the scheduling policy.
 * @param The db client ID of the new local scheduler.
 * @return Void.
 */
void handle_new_local_scheduler(GlobalSchedulerState *state,
                                GlobalSchedulerPolicyState *policy_state,
                                DBClientID db_client_id);

void handle_local_scheduler_removed(GlobalSchedulerState *state,
                                    GlobalSchedulerPolicyState *policy_state,
                                    DBClientID db_client_id);

#endif /* GLOBAL_SCHEDULER_ALGORITHM_H */
