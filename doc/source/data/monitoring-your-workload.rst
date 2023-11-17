.. _monitoring-your-workload:

Monitoring Your Workload
========================

This section helps you debug and monitor the execution of your Ray Data :class:`Dataset <ray.data.Dataset>` by viewing the:

* Ray dashboard job detail page
* Ray dashboard metrics
* Ray Data logs

Ray Data Dashboard
------------------

Ray Data emits Prometheus metrics in real-time while a Dataset is executing. These metrics are tagged by both dataset and operator, and are displayed in multiple views across the Ray dashboard.

Ray dashboard job detail page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For an overview of all datasets that have been running on your cluster, see the Ray Data Overview in the :ref:`job detail page <dash-jobs-view>`. This table will appear once the first dataset starts executing on the cluster, and shows dataset details such as:

* execution progress (measured in blocks)
* execution state (running, failed, or finished)
* dataset runtime
* certain dataset-level metrics

.. image:: images/data-overview-table.png
   :align: center

For a more fine-grained overview, each dataset row in the table can also be expanded to display the same details for individual operators.

.. image:: images/data-overview-table-expanded.png
   :align: center

Ray dashboard
~~~~~~~~~~~~~

For an even finer view, see the Ray Data section in the :ref:`Metrics tab <dash-metrics-view>`. This section contains time-series views of all metrics emitted by Ray Data, grouped by operator.

The metrics recorded are:

* Bytes spilled by objects from object store to disk
* Bytes of objects allocated in object store
* Bytes of objects freed in object store
* Current total bytes of objects in object store
* Logical CPUs allocated to dataset operators
* Logical GPUs allocated to dataset operators
* Bytes outputted by dataset operators
* Rows outputted by dataset operators
* Time spent generating blocks
* Time user code is blocked during iteration.
* Time spent in user code during iteration.

.. image:: images/data-dashboard.png
   :align: center

To learn more about the Ray dashboard, including detailed setup instructions, see :ref:`Ray Dashboard <observability-getting-started>`.

Ray Data logs
-------------
During execution, Ray Data will periodically log status updates to `ray-data.log`. 

At the end of every scheduling loop, Ray Data will log execution progress of every operator in the DAG. For more frequent updates, `RAY_DATA_TRACE_SCHEDULING=1` can be set so that the progress is logged after each task is dispatched.

.. code-block:: text

   Execution Progress:
   0: - Input: 0 active, 0 queued, 0.0 MiB objects, Blocks Outputted: 200/200
   1: - ReadRange->MapBatches(<lambda>): 10 active, 190 queued, 381.47 MiB objects, Blocks Outputted: 100/200

When an operator completes, the metrics for that operator will also be logged.

.. code-block:: text

   Operator InputDataBuffer[Input] -> TaskPoolMapOperator[ReadRange->MapBatches(<lambda>)] completed. Operator Metrics:
   {'num_inputs_received': 20, 'bytes_inputs_received': 46440, 'num_inputs_processed': 20, 'bytes_inputs_processed': 46440, 'num_outputs_generated': 20, 'bytes_outputs_generated': 800, 'rows_outputs_generated': 100, 'num_outputs_taken': 20, 'bytes_outputs_taken': 800, 'num_outputs_of_finished_tasks': 20, 'bytes_outputs_of_finished_tasks': 800, 'num_tasks_submitted': 20, 'num_tasks_running': 0, 'num_tasks_have_outputs': 20, 'num_tasks_finished': 20, 'obj_store_mem_alloc': 800, 'obj_store_mem_freed': 46440, 'obj_store_mem_cur': 0, 'obj_store_mem_peak': 23260, 'obj_store_mem_spilled': 0, 'block_generation_time': 1.191296085, 'cpu_usage': 0, 'gpu_usage': 0, 'ray_remote_args': {'num_cpus': 1, 'scheduling_strategy': 'SPREAD'}}

