#ifndef LS_POLICY_H
#define LS_POLICY_H


#include <unordered_map>

#include "LsQueue.h"
#include "LsPolicy.h"
#include "LsResources.h"


namespace ray {
class LsPolicy {
public:
  // A constructor that initializes the policy with a reference to queue state.
  LsPolicy(const LsQueue &lsqueue) : lsqueue_(lsqueue) {}
  // Perform a scheduling operation, given a set of cluster resources.
  // Produces a mapping of tasks to one of three possible destinations:
  // (1) local raylet (to be executed locally), (2) remote raylet,
  /// (3) global scheduler.
  std::unordered_map<TaskID, DBClientID, UniqueIDHasher> Schedule(
      const std::unordered_map<DBClientID, LsResources, UniqueIDHasher> &cluster_resources);
  // Perform a scheduling operation for a single task, given a set of

  virtual ~LsPolicy() {}
private:
  // Constant reference to Raylet queue state.
  const LsQueue &lsqueue_;
};
}
#endif
