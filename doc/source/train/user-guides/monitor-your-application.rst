.. _train-metrics:

Ray Train Metrics
-----------------
Ray Train exports Prometheus metrics including the Ray Train controller state, worker group start times, checkpointing times and more. You can use these metrics to monitor Ray Train runs.
The Ray dashboard displays these metrics in the Ray Train Grafana Dashboard. See :ref:`Ray Dashboard documentation<observability-getting-started>` for more information.

The Ray Train dashboard also displays a subset of Ray Core metrics that are useful for monitoring training but are not listed in the table below.
For more information about these metrics, see the :ref:`System Metrics documentation<system-metrics>`.

The following table lists the Prometheus metrics emitted by Ray Train:

.. list-table:: Train Metrics
    :header-rows: 1

    * - Prometheus Metric
      - Labels
      - Description
    * - `ray_train_controller_state`
      - `ray_train_run_name`, `ray_train_run_id`, `ray_train_controller_state`
      - Current state of the Ray Train controller.
    * - `ray_train_worker_group_start_total_time_s`
      - `ray_train_run_name`, `ray_train_run_id`
      - Total time taken to start the worker group.
    * - `ray_train_worker_group_shutdown_total_time_s`
      - `ray_train_run_name`, `ray_train_run_id`
      - Total time taken to shut down the worker group.
    * - `ray_train_report_total_blocked_time_s`
      - `ray_train_run_name`, `ray_train_run_id`, `ray_train_worker_world_rank`, `ray_train_worker_actor_id`
      - Cumulative time in seconds to report a checkpoint to storage.