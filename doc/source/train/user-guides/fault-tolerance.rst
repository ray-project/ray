.. _:: ../doc_code:

.. _train-fault-tolerance:

Handling Failures and Node Preemption
=====================================

Ray Train provides fault tolerance at three levels:

1. **Worker process fault tolerance** handles errors that happen to one or more Train worker processes while they are executing the user defined training function.
2. **Worker node fault tolerance** handles node failures that may occur during training.
3. **Job driver fault tolerance** handles the case where Ray Train driver process crashes, and training needs to be kicked off again, possibly from a new cluster.

This user guide covers how to configure and use these fault tolerance mechanisms.

.. _train-worker-fault-tolerance:

Worker Process and Node Fault Tolerance
---------------------------------------

**Worker process failures** are errors that occur within the user defined training function of a training worker,
such as GPU out-of-memory (OOM) errors, cloud storage access errors, or other runtime errors.

**Node failures** are errors that bring down the entire node, including node preemption, OOM, network partitions, or other hardware failures.
This section covers worker node failures. Recovery from head node failures is discussed in the :ref:`next section <train-cluster-fault-tolerance>`.

Ray Train can be configured to automatically recover from worker process and worker node failures.
When a failure is detected, all the workers are shut down, new nodes are added if necessary, and a new set of workers is started.
The restarted training worker processes can resume training by loading the latest checkpoint.

In order to retain progress upon recovery, your training function
should implement logic for both :ref:`saving <train-dl-saving-checkpoints>`
*and* :ref:`loading checkpoints <train-dl-loading-checkpoints>`.
Otherwise, the training will just start from scratch.

Each recovery from a worker process or node failure is considered a retry. The
number of retries is configurable through the ``max_failures`` attribute of the
:class:`~ray.train.FailureConfig` argument set in the :class:`~ray.train.RunConfig`
passed to the ``Trainer``. By default, worker fault tolerance is disabled with ``max_failures=0``.

.. literalinclude:: ../doc_code/fault_tolerance.py
    :language: python
    :start-after: __failure_config_start__
    :end-before: __failure_config_end__

Altogether, this is what an example Torch training script with worker fault tolerance looks like:

.. literalinclude:: ../doc_code/fault_tolerance.py
    :language: python
    :start-after: __worker_fault_tolerance_start__
    :end-before: __worker_fault_tolerance_end__


Which checkpoint will be restored?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Train will populate :func:`ray.train.get_checkpoint() <ray.train.get_checkpoint>` with the latest available
:ref:`checkpoint reported to Ray Train <train-checkpointing>`.
The :class:`~ray.train.Checkpoint` object returned by this method has the
:meth:`~ray.train.Checkpoint.as_directory` and :meth:`~ray.train.Checkpoint.to_directory` methods
to download the checkpoint from the :class:`RunConfig(storage_path) <ray.train.RunConfig>` to local disk.

.. note::
    :meth:`~ray.train.Checkpoint.as_directory` and :meth:`~ray.train.Checkpoint.to_directory`
    will only download the checkpoint once per node even if there are multiple workers on the node.
    The workers share the same checkpoint directory on local disk.

Illustrated Example
~~~~~~~~~~~~~~~~~~~

Consider the following example of a cluster containing a CPU head node and 2 GPU worker nodes.
There are 4 GPU training workers running on the 2 worker nodes.
The :ref:`storage path has been configured <persistent-storage-guide>` to use cloud storage, which is where checkpoints are saved.

.. figure:: ../images/fault_tolerance/worker_node_failure.png
    :align: left

    (Left) Training has been running for some time, and the latest checkpoint has been saved to cloud storage.

    (Right) One of the worker GPU nodes fails due to a hardware fault. Ray Train detects this failure and shuts down all the workers.
    Since the number of failures detected so far is less than the configured ``max_failures``, Ray Train will attempt to restart training,
    rather than exiting and raising an error.

.. figure:: ../images/fault_tolerance/worker_node_replacement.png
    :align: left

    (Left) Same as the previous figure.

    (Right) Ray Train has requested a new worker node to join the cluster and is waiting for it to come up.

.. figure:: ../images/fault_tolerance/resume_from_checkpoint.png
    :align: left

    (Left) Same as the previous figure.

    (Right) The new worker node has joined the cluster.
    Ray Train restarts all the worker processes and provides them with the latest checkpoint.
    The workers download the checkpoint from storage and use it to resume training.


.. _train-restore-guide:
.. _train-cluster-fault-tolerance:


Job Driver Fault Tolerance
--------------------------

Job driver fault tolerance is to handle cases where the Ray Train driver process is interrupted.
The Ray Train driver process is the process that calls ``trainer.fit()`` and is usually located on the head node of the cluster.

The driver process may be interrupted due to one of the following reasons:

- The run is manually interrupted by a user (e.g., Ctrl+C).
- The node where the driver process is running (head node) crashes (e.g., out of memory, out of disk).
- The entire cluster goes down (e.g., network error affecting all nodes).

In these cases, the Ray Train driver (which calls ``trainer.fit()``) needs to be launched again.
The relaunched Ray Train driver needs to find a minimal amount of run state in order to pick up where the previous run left off.
This state includes the latest reported checkpoints, which are located at the :ref:`storage path <persistent-storage-guide>`.
Ray Train fetches the latest checkpoint information from storage and passes it to the newly launched worker processes to resume training.

To find this run state, Ray Train relies on passing in the **same** :ref:`RunConfig(storage_path, name) <ray.train.RunConfig>` pair as the previous run.
If the ``storage_path`` or ``name`` do not match, Ray Train will not be able to find the previous run state and will start a new run from scratch.

.. warning::
    If ``name`` is reused unintentionally, Ray Train will fetch the previous run state, even if the user is trying to start a new run.
    Therefore, always pass a unique run name when launching a new run. In other words, ``name`` should be a unique identifier for a training job

.. note::
    Job driver crashes and interrupts do NOT count toward the ``max_failures`` limit of :ref:`worker fault tolerance <train-worker-fault-tolerance>`.


Illustrated Example
~~~~~~~~~~~~~~~~~~~

.. figure:: ../images/fault_tolerance/head_node_failure.png
    :align: center

    ABCDEFG

.. figure:: ../images/fault_tolerance/cluster_failure.png
    :align: center

    ABCDEFG

.. figure:: ../images/fault_tolerance/cluster_restart.png
    :align: center

    ABCDEFG