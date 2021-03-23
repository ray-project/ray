#include <vector>

#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {
namespace raylet_scheduling_policy {

int64_t HybridPolicy(const TaskRequest &task_request, const int64_t local_node_id,
                     const absl::flat_hash_map<int64_t, Node> &nodes,
                     float hybrid_threshold);
}
}  // namespace ray
