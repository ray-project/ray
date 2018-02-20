#ifndef LS_POLICY_CC
#define LS_POLICY_CC
#include <unordered_map>

#include "LsPolicy.h"
#include "LsResources.h"

using namespace std;
namespace ray{

/// Given a set of cluster resources, produce a placement decision on all work
/// in the queue.
std::unordered_map<TaskID, DBClientID, UniqueIDHasher> LsPolicy::Schedule(
    const std::unordered_map<DBClientID, LsResources, UniqueIDHasher> &cluster_resources) {
  std::runtime_error("method not implemented");
  return std::unordered_map<TaskID, DBClientID, UniqueIDHasher>();
}

} // end namespace ray
#endif
