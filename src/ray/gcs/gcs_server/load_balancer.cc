#include "ray/gcs/gcs_server/load_balancer.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"

namespace ray {
namespace gcs {

LoadBalancer::LoadBalancer(GcsServer &gcs_server,
                          instrumented_io_context &io_context)
    : gcs_server_(gcs_server), io_context_(io_context) {}

Status LoadBalancer::Start() {
  // Initialize load stats
  load_stats_.clear();
  return Status::OK();
}

Status LoadBalancer::Stop() {
  // Clear load stats
  load_stats_.clear();
  return Status::OK();
}

Status LoadBalancer::UpdateLoadStats(const std::string &node_id,
                                   const LoadStats &stats) {
  // Update load stats for the node
  load_stats_[node_id] = stats;
  return Status::OK();
}

Status LoadBalancer::GetLoadStats(const std::string &node_id,
                                LoadStats *stats) {
  auto it = load_stats_.find(node_id);
  if (it == load_stats_.end()) {
    return Status::NotFound("Node not found");
  }

  *stats = it->second;
  return Status::OK();
}

Status LoadBalancer::GetAllLoadStats(std::vector<std::pair<std::string, LoadStats>> *stats) {
  stats->clear();
  stats->reserve(load_stats_.size());

  for (const auto &entry : load_stats_) {
    stats->emplace_back(entry.first, entry.second);
  }

  return Status::OK();
}

Status LoadBalancer::BalanceLoad() {
  // Get all load stats
  std::vector<std::pair<std::string, LoadStats>> stats;
  RAY_RETURN_NOT_OK(GetAllLoadStats(&stats));

  if (stats.empty()) {
    return Status::OK();
  }

  // Calculate average load
  double total_cpu_load = 0.0;
  double total_memory_load = 0.0;
  int64_t total_tasks = 0;
  int64_t total_workers = 0;

  for (const auto &entry : stats) {
    total_cpu_load += entry.second.cpu_load;
    total_memory_load += entry.second.memory_load;
    total_tasks += entry.second.num_tasks;
    total_workers += entry.second.num_workers;
  }

  double avg_cpu_load = total_cpu_load / stats.size();
  double avg_memory_load = total_memory_load / stats.size();
  double avg_tasks = static_cast<double>(total_tasks) / stats.size();
  double avg_workers = static_cast<double>(total_workers) / stats.size();

  // Find overloaded and underloaded nodes
  std::vector<std::string> overloaded_nodes;
  std::vector<std::string> underloaded_nodes;

  for (const auto &entry : stats) {
    bool is_overloaded = false;
    bool is_underloaded = false;

    // Check CPU load
    if (entry.second.cpu_load > avg_cpu_load * (1 + load_threshold_)) {
      is_overloaded = true;
    } else if (entry.second.cpu_load < avg_cpu_load * (1 - load_threshold_)) {
      is_underloaded = true;
    }

    // Check memory load
    if (entry.second.memory_load > avg_memory_load * (1 + load_threshold_)) {
      is_overloaded = true;
    } else if (entry.second.memory_load < avg_memory_load * (1 - load_threshold_)) {
      is_underloaded = true;
    }

    // Check task count
    if (entry.second.num_tasks > avg_tasks * (1 + load_threshold_)) {
      is_overloaded = true;
    } else if (entry.second.num_tasks < avg_tasks * (1 - load_threshold_)) {
      is_underloaded = true;
    }

    // Check worker count
    if (entry.second.num_workers > avg_workers * (1 + load_threshold_)) {
      is_overloaded = true;
    } else if (entry.second.num_workers < avg_workers * (1 - load_threshold_)) {
      is_underloaded = true;
    }

    if (is_overloaded) {
      overloaded_nodes.push_back(entry.first);
    } else if (is_underloaded) {
      underloaded_nodes.push_back(entry.first);
    }
  }

  // Generate migration plan
  std::vector<Migration> migrations;
  for (const auto &overloaded_node : overloaded_nodes) {
    for (const auto &underloaded_node : underloaded_nodes) {
      Migration migration;
      migration.source_node = overloaded_node;
      migration.target_node = underloaded_node;
      // TODO: Implement task selection for migration
      migrations.push_back(migration);
    }
  }

  // Execute migrations
  for (const auto &migration : migrations) {
    RAY_RETURN_NOT_OK(ExecuteMigration(migration));
  }

  return Status::OK();
}

Status LoadBalancer::ExecuteMigration(const Migration &migration) {
  auto source_it = load_stats_.find(migration.source_node);
  auto target_it = load_stats_.find(migration.target_node);

  if (source_it == load_stats_.end() || target_it == load_stats_.end()) {
    return Status::NotFound("Node not found");
  }

  // TODO: Implement actual task migration logic
  // For now, we just update the load stats to simulate migration

  // Simulate migration by moving 10% of the load
  double cpu_load_delta = source_it->second.cpu_load * 0.1;
  double memory_load_delta = source_it->second.memory_load * 0.1;
  int64_t tasks_delta = source_it->second.num_tasks / 10;
  int64_t workers_delta = source_it->second.num_workers / 10;

  // Update source node stats
  source_it->second.cpu_load -= cpu_load_delta;
  source_it->second.memory_load -= memory_load_delta;
  source_it->second.num_tasks -= tasks_delta;
  source_it->second.num_workers -= workers_delta;

  // Update target node stats
  target_it->second.cpu_load += cpu_load_delta;
  target_it->second.memory_load += memory_load_delta;
  target_it->second.num_tasks += tasks_delta;
  target_it->second.num_workers += workers_delta;

  return Status::OK();
}

}  // namespace gcs
}  // namespace ray 