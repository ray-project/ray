---
myst:
  html_meta:
    description: "Prometheus metrics Ray Train exports for controller state, worker group startup, and checkpoint timing, viewable in the Ray dashboard."
---

(train-metrics)=

# Ray Train metrics

Ray Train exports Prometheus metrics that you can use to monitor Ray Train runs, such as the Ray Train controller state, worker group start times, and checkpointing times. The Ray dashboard shows these metrics in the Ray Train Grafana dashboard. For more information, see the {ref}`Ray dashboard documentation <observability-getting-started>`.

The Ray Train dashboard also shows a subset of Ray Core metrics that are useful for monitoring training but aren't in the following table. For more information about these metrics, see the {ref}`System metrics documentation <system-metrics>`.

The dashboard's **Data Ingestion** row builds on {ref}`Ray Data metrics <monitoring-your-workload>` to show how much time each training worker spends waiting on data, broken down by data loading stage and by rank. For a step-by-step workflow that uses those panels to find data loading bottlenecks and stragglers, see {ref}`train-debugging-data-loading-bottlenecks`.

The following table lists the Prometheus metrics that Ray Train emits:

```{list-table} Ray Train metrics
:header-rows: 1

* - Prometheus metric
  - Labels
  - Description
* - `ray_train_controller_state`
  - `ray_train_run_name`, `ray_train_run_id`, `ray_train_controller_state`
  - Current state of the Ray Train controller.
* - `ray_train_worker_group_start_total_time_s`
  - `ray_train_run_name`, `ray_train_run_id`
  - Total time to start the worker group.
* - `ray_train_worker_group_shutdown_total_time_s`
  - `ray_train_run_name`, `ray_train_run_id`
  - Total time to shut down the worker group.
* - `ray_train_report_total_blocked_time_s`
  - `ray_train_run_name`, `ray_train_run_id`, `ray_train_worker_world_rank`, `ray_train_worker_actor_id`
  - Cumulative time in seconds to report a checkpoint to storage.
```
