.. _tune-lifecycle:

How does Tune work?
===================

This page provides an overview of Tune's inner workings.

.. tip:: Before you continue, be sure to have read :ref:`the Tune Key Concepts page <tune-60-seconds>`.

Definitions
-----------

**Trainable**

The function passed to tune.run.

**Trial**

Logical representation of a single hyperparameter configuration. Each trial is associated with an instance of a Trainable.

**Driver/worker process**

The driver process is the python process that calls ``tune.run`` (which calls ``ray.init()`` underneath the hood).

The Tune driver process runs on the node where you run your script (which calls ``tune.run``), while Ray Tune trainable "actors" run on any node (either on the same node or on worker nodes (distributed Ray only)).

**Ray Actors**

Tune uses Ray Actors as worker processes to evaluate multiple Trainables in parallel.

:ref:`Ray Actors <actor-guide>` allow you to parallelize an instance of a class in Python. When you instantiate a class that is a Ray actor, Ray will start a instance of that class on a separate process either on the same machine (or another distributed machine, if running a Ray cluster). This actor can then asynchronously execute method calls and maintain its own internal state.

What happens in ``tune.run``?
-----------------------------

When calling the following:

.. code-block:: python

    space = {"x": tune.uniform(0, 1)}
    tune.run(my_trainable, config=space, num_samples=10)

The provided function/trainable is evaluated multiple times in parallel with different hyperparameters (sampled from ``uniform(0, 1)``).

Every Tune run consists of "driver process" and many "worker processes". As mentioned in the Definitions section, the driver process (Tune Driver) is the python process in which you call ``tune.run``.

The driver spawns parallel worker processes (:ref:`Ray actors <actor-guide>`)
that are responsible for evaluating each trial using its hyperparameter configuration and the provided trainable (see the `trial executor source code <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial_executor.py>`__).

While the Trainable is executing (:ref:`trainable-execution`), the Tune Driver communicates with each actor via actor methods to receive intermediate training results and pause/stop actors (see :ref:`trial-lifecycle`).

When the Trainable terminates (or is stopped), the actor is also terminated.

.. _trainable-execution:

The execution of a trainable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tune uses :ref:`Ray actors <actor-guide>` to parallelize the evaluation of multiple hyperparameter configurations. Each actor is a Python process that executes an instance of the user-provided Trainable.

The definition of the user-provided Trainable will be :ref:`serialized via cloudpickle <serialization-guide>`) and sent to each actor process. Each Ray actor will start an instance of the Trainable to be executed.

If the Trainable is a class, it will be executed iteratively by calling ``train/step``. After each invocation, the driver is notified that a "result dict" is ready. The driver will then pull the result via ``ray.get``.

If the trainable is a callable or a function, it will be executed on the Ray actor process on a separate execution thread. Whenever ``tune.report`` is called, the execution thread is paused and waits for the driver to pull a result (see `function_runner.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/function_runner.py>`__. After pulling, the actor’s execution thread will automatically resume.


Resource Management in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before running a trial, the Tune Driver will check whether there are available resources on the cluster (see :ref:`resource-requirements`). It will compare the available resources with the resources required by the trial.

If there is space on the cluster, then the Tune Driver will start a Ray actor (worker). This actor will be scheduled and executed on some node where the resources are available.

See :ref:`tune-parallelism` for more information.

.. _trial-lifecycle:

Lifecycle of a trial
--------------------

A trial's life cycle consists of 6 stages:

* **Initialization** (generation): A trial is first generated as a hyperparameter sample, and its parameters are configured according to what was provided in tune.run. Trials are then placed into a queue to be executed (with status PENDING).

* **PENDING**: A pending trial is a trial to be executed on the machine. Every trial is configured with resource values. Whenever the trial’s resource values are available, tune will run the trial (by starting a ray actor holding the config and the training function.

* **RUNNING**: A running trial is assigned a Ray Actor. There can be multiple running trials in parallel. See the :ref:`trainable execution <trainable-execution>` section for more details.

* **ERRORED**: If a running trial throws an exception, Tune will catch that exception and mark the trial as errored. Note that exceptions can be propagated from an actor to the main Tune driver process. If max_retries is set, Tune will set the trial back into "PENDING" and later start it from the last checkpoint.

* **TERMINATED**: A trial is terminated if it is stopped by a Stopper/Scheduler. If using the Function API, the trial is also terminated when the function stops.

* **PAUSED**: A trial can be paused by a Trial scheduler. This means that the trial’s actor will be stopped. A paused trial can later be resumed from the most recent checkpoint.
