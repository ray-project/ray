#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

class GcsServer;

struct LoadStats {
  double cpu_load = 0.0;
  double memory_load = 0.0;
  int64_t num_tasks = 0;
  int64_t num_workers = 0;
};

struct Migration {
  std::string source_node;
  std::string target_node;
  std::vector<std::string> task_ids;
};

class LoadBalancer {
 public:
  LoadBalancer(GcsServer &gcs_server, instrumented_io_context &io_context);
  ~LoadBalancer() = default;

  // Start/stop the load balancer
  Status Start();
  Status Stop();

  // Update load stats for a node
  Status UpdateLoadStats(const std::string &node_id, const LoadStats &stats);

  // Get load stats for a node
  Status GetLoadStats(const std::string &node_id, LoadStats *stats);

  // Get load stats for all nodes
  Status GetAllLoadStats(std::vector<std::pair<std::string, LoadStats>> *stats);

  // Balance load across nodes
  Status BalanceLoad();

 private:
  // Execute a migration plan
  Status ExecuteMigration(const Migration &migration);

  // Reference to GCS server
  GcsServer &gcs_server_;

  // IO context
  instrumented_io_context &io_context_;

  // Load stats for each node
  std::unordered_map<std::string, LoadStats> load_stats_;

  // Load threshold for triggering migrations (as a fraction)
  const double load_threshold_ = 0.2;  // 20% deviation from average
};

}  // namespace gcs
}  // namespace ray 