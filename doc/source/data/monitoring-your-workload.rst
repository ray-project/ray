.. _monitoring-your-workload:

Monitoring Your Workload
========================

This section helps you debug and monitor the execution of your :class:`~ray.data.Dataset` by viewing the:

* :ref:`Ray Data Overview`
* :ref:`Ray Dashboard Metrics`
* :ref:`Ray Data Logs`

Ray Data Dashboard
------------------

Ray Data emits Prometheus metrics in real-time while a Dataset is executing. These metrics are tagged by both dataset and operator, and are displayed in multiple views across the Ray dashboard.

.. note::
   Most metrics are only available for physical operators that use the map operation. For example, physical operators created by :meth:`~ray.data.Dataset.map_batches`, :meth:`~ray.data.Dataset.map`, and :meth:`~ray.data.Dataset.flat_map`.

.. _Ray Data Overview:

Ray Data overview
~~~~~~~~~~~~~~~~~

For an overview of all datasets that have been running on your cluster, see the Ray Data Overview in the :ref:`jobs view <dash-jobs-view>`. This table appears once the first dataset starts executing on the cluster, and shows dataset details such as:

* execution progress (measured in blocks)
* execution state (running, failed, or finished)
* dataset start/end time
* dataset-level metrics (for example, sum of rows processed over all operators)

.. image:: images/data-overview-table.png
   :align: center

For a more fine-grained overview, each dataset row in the table can also be expanded to display the same details for individual operators.

.. image:: images/data-overview-table-expanded.png
   :align: center

.. tip::

    To evaluate a dataset-level metric where it's not appropriate to sum the values of all the individual operators, it may be more useful to look at the operator-level metrics of the last operator. For example, to calculate a dataset's throughput, use the "Rows Outputted" of the dataset's last operator, because the dataset-level metric contains the sum of rows outputted over all operators.

.. _Ray Dashboard Metrics:

Ray dashboard metrics
~~~~~~~~~~~~~~~~~~~~~

For a time-series view of these metrics, see the Ray Data section in the :ref:`Metrics view <dash-metrics-view>`. This section contains time-series graphs of all metrics emitted by Ray Data. Execution metrics are grouped by dataset and operator, and iteration metrics are grouped by dataset.

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

.. _Ray Data Logs:

Ray Data logs
-------------
During execution, Ray Data periodically logs updates to `ray-data.log`.

Every five seconds, Ray Data logs the execution progress of every operator in the dataset. For more frequent updates, set `RAY_DATA_TRACE_SCHEDULING=1` so that the progress is logged after each task is dispatched.

.. code-block:: text

   Execution Progress:
   0: - Input: 0 active, 0 queued, 0.0 MiB objects, Blocks Outputted: 200/200
   1: - ReadRange->MapBatches(<lambda>): 10 active, 190 queued, 381.47 MiB objects, Blocks Outputted: 100/200

When an operator completes, the metrics for that operator are also logged.

.. code-block:: text

   Operator InputDataBuffer[Input] -> TaskPoolMapOperator[ReadRange->MapBatches(<lambda>)] completed. Operator Metrics:
   {'num_inputs_received': 20, 'bytes_inputs_received': 46440, 'num_inputs_processed': 20, 'bytes_inputs_processed': 46440, 'num_outputs_generated': 20, 'bytes_outputs_generated': 800, 'rows_outputs_generated': 100, 'num_outputs_taken': 20, 'bytes_outputs_taken': 800, 'num_outputs_of_finished_tasks': 20, 'bytes_outputs_of_finished_tasks': 800, 'num_tasks_submitted': 20, 'num_tasks_running': 0, 'num_tasks_have_outputs': 20, 'num_tasks_finished': 20, 'obj_store_mem_alloc': 800, 'obj_store_mem_freed': 46440, 'obj_store_mem_cur': 0, 'obj_store_mem_peak': 23260, 'obj_store_mem_spilled': 0, 'block_generation_time': 1.191296085, 'cpu_usage': 0, 'gpu_usage': 0, 'ray_remote_args': {'num_cpus': 1, 'scheduling_strategy': 'SPREAD'}}

