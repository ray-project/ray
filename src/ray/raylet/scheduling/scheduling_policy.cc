#include "ray/raylet/scheduling/scheduling_policy.h"

namespace ray {

namespace raylet_scheduling_policy {

int64_t HybridPolicy(const TaskRequest &task_request, const int64_t local_node_id,
                     const absl::flat_hash_map<int64_t, Node> &nodes,
                     float hybrid_threshold) {
  // Step 1: Generate the traversal order. We guarantee that the first node is local, to
  // encourage local scheduling. The rest of the traversal order should be globally
  // consistent, to encourage using "warm" workers.
  std::vector<int64_t> round;
  {
    round.push_back(local_node_id);
    for (const auto &pair : nodes) {
      if (pair.first != local_node_id) {
        round.push_back(pair.first);
      }
    }
    std::sort(round.begin() + 1, round.end());
  }

  int64_t best_node_id = -1;
  float best_utilization_score = 1.0;
  bool best_is_available = false;

  // Step 2: Perform the round robin.
  for (auto node_id : round) {
    const auto &it = nodes.find(node_id);
    RAY_CHECK(it != nodes.end());
    const auto &node = it->second;
    if (!node.GetLocalView().IsFeasible(task_request)) {
      RAY_LOG(ERROR) << "Skipping node " << node_id << " because it's infeasible";
      // Skip infeasible nodes. This task can sit on any raylet's infeasible queue.
      continue;
    }

    bool is_available = node.GetLocalView().IsAvailable(task_request);
    float critical_resource_utilization =
        node.GetLocalView().CalculateCriticalResourceUtilization();
    if (critical_resource_utilization < hybrid_threshold) {
      critical_resource_utilization = 0;
    }

    RAY_LOG(ERROR) << "Node with id: " << node_id << " is available: " << is_available
                   << ".";

    bool update_best_node = false;

    if (is_available) {
      // Always prioritize available nodes over nodes where the task must be queued first.
      if (!best_is_available) {
        update_best_node = true;
      } else if (critical_resource_utilization < best_utilization_score) {
        // Break ties between available nodes by their critical resource utilization.
        update_best_node = true;
      }
    } else if (!best_is_available &&
               critical_resource_utilization < best_utilization_score) {
      // Pick the best feasible node by critical resource utilization.
      update_best_node = true;
    }

    if (update_best_node) {
      RAY_LOG(ERROR) << "Best node is now: " << node_id
                     << " with utilization: " << critical_resource_utilization
                     << " and it is " << (is_available ? "availabile" : "not available");
      best_node_id = node_id;
      best_utilization_score = critical_resource_utilization;
      best_is_available = is_available;
    }
  }

  return best_node_id;
}

}  // namespace raylet_scheduling_policy

}  // namespace ray
