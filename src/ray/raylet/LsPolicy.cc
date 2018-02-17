#ifndef LS_POLICY_CC
#define LS_POLICY_CC
#include "LsPolicy.h"
namespace ray{

/// Given a set of cluster resources, produce a placement decision on all work
/// in the queue.
unordered_map<TaskID, DBClientID> LsPolicy::Schedule(
    const unordered_map<DBClientID, LsResources> &cluster_resources) {
  // TODO(atumanov): policy implementation.
}

}
#endif
