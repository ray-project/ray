.. _train-tune:

Hyperparameter Tuning with Ray Tune
===================================

.. info::
    This user guide shows how to integrate Ray Train and Ray Tune to tune over distributed hyperparameter runs
    for the revamped Ray Train V2 available starting from Ray 2.43 by enabling the environment variable ``RAY_TRAIN_V2_ENABLED=1``.
    This user guide assumes that the environment variable has been enabled.
    Please see <link to bottom section> for information about the migration timeline and a link to the old user guide.


Ray Train can be used together with Ray Tune to do hyperparameter sweeps of distributed training runs.
This is often useful when you want to do a small sweep over critical hyperparameters,
before launching a run with the best performing hyperparameters on all available cluster resources for a long duration.

Quickstart
----------

In the example below:

* :class:`~ray.tune.Tuner` launches the tuning job, which runs a bunch of ``train_driver_fn`` trials with different hyperparameter configurations.
* ``train_driver_fn``, which instantiates a ``TorchTrainer`` (or some other framework trainer), takes in a hyperparameter configuration and launches the distributed training job.
* :class:`~ray.train.ScalingConfig` defines the number of training workers and resources per worker for a single Ray Train run.
* ``train_fn_per_worker`` is the Python code that executes on each distributed training worker for a trial.

.. .. literalinclude:: ../doc_code/tuner.py
..     :language: python
..     :start-after: __basic_start__
..     :end-before: __basic_end__


Configuring Resources for Multiple Trials
----------------------------------------

Ray Tune launches multiple trials which run a user-defined function in a remote Ray actor, where each trial gets a different sampled hyperparameter configuration.
Typically, these Tune trials do work computation directly inside the Ray actor. For example, each trial could request 1 GPU and do some single-process model
training within the remote actor itself. When using Ray Train inside Ray Tune functions, the Tune trial is actually not doing extensive computation inside this actor
-- instead it just acts as a driver process to launch and monitor the Ray Train workers running elsewhere.

.. figure:: ../images/train_tune_interop.jpg
    :align: center

    Example of Ray Train runs being launched from within Ray Tune trials.


.. figure:: ../images/train_without_tune.jpg
    :align: center

    A single Ray Train run to showcase how using Ray Tune just adds a layer of hierarchy to this tree of processes.



Set ``max_concurrent_trials`` to limit the number of Ray Train driver processes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Train runs can only start when resources for all workers can be acquired at once.
This means that multiple Tune trials spawning Train runs will be competing for the logical resources available in the Ray cluster.

If there is a limiting cluster resource such as GPUs, then it won't be possible to run training for all hyperparameter configurations concurrently.
Since the cluster only has enough resources for a handful of trials to run concurrently,
set :class:`tune.TuneConfig(max_concurrent_trials) <ray.tune.TuneConfig>` on the Tuner to limit the number of “in-flight” Train runs so that no trial is being starved of resources.

As a concrete example, consider a fixed sized cluster with 128 CPUs and 8 GPUs.

* The Tuner(param_space) sweeps over 4 hyperparameter configurations with a grid search: ``param_space={“train_loop_config”: {“batch_size”: tune.grid_search([8, 16, 32, 64])}}``
* Each Ray Train run is configured to train with 4 GPU workers: ``ScalingConfig(num_workers=4, use_gpu=True)``. Since there are only 8 GPUs, only 2 Train runs can acquire their full set of resources at a time.
* However, since there are many CPUs available in the cluster, the 4 total Ray Tune trials (which default to requesting 1 CPU) can be launched immediately.
  This results in 2 extra Ray Tune trial processes being launched, even though their inner Ray Train run just waits for resources until one of the other trials finishes.
  This introduces some spammy log messages when Train waits for resources, and this could also lead to an excessive number of Ray Tune trial processes if the total number of hyperparameter configurations is large.
* To fix this issue, set ``Tuner(tune_config=tune.TuneConfig(max_concurrent_trials=2))``. Now, only two Ray Tune trial processes will be running at a time.
  This number can be calculated based on the limiting cluster resource and the amount of that resources required by each trial.


Advanced: Set Tune function resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default Ray Tune function launches with 1 CPU. Ray Tune will just schedule these functions to run anywhere on the cluster that has free logical CPU resources.

**Recommendation:** If you are launching longer-running training jobs or using spot instances, these Tune functions which act as the Ray Train driver process should be run on “safe nodes” that are at lower risk of going down. For example, they should not be scheduled to run on preemptible spot instances and should not be colocated with training workers. This could be the head node or a dedicated CPU node in your cluster.

This is because the Ray Train driver process is responsible for handling fault tolerance of the worker processes, which are more likely to error. Nodes that are running Train workers can crash due to spot preemption or other errors that come up due to the user-defined model training code.

* If a Train worker node dies, the Ray Train driver process that is still alive on a different node can gracefully handle the error.
* On the other hand, if the driver process dies, then all Ray Train workers will ungracefully exit and some of the run state may not be committed fully.

One way to achieve this behavior is to set custom resources on certain node types and configure the Tune functions to request those resources.

.. .. literalinclude:: ../doc_code/tuner.py
..     :language: python
..     :start-after: __basic_start__
..     :end-before: __basic_end__


Checkpoints
-----------

Both Ray Train and Ray Tune provide utilities to help upload and track checkpoints via the ray.train.report and ray.tune.report APIs. <Link to Ray Train checkpointing user guide.>

If the Ray Train workers report checkpoints, saving another Ray Tune checkpoint at the Train driver level is not needed because it does not hold any extra training state. The Ray Train driver process will already periodically snapshot its status to the configured storage_path, which is further described in the next section on fault tolerance.

In order to access the checkpoints from the Tuner output, you can append the checkpoint path as a metric. The provided TuneReportCallback does this by propagating reported Ray Train results over to Ray Tune, where the checkpoint path is attached as a separate metric.

Advanced: Fault Tolerance
~~~~~~~~~~~~~~~~~~~~~~~~~

In the event that the Ray Tune trials running the Ray Train driver process crash, you can enable trial fault tolerance on the Ray Tune side to re-launch the Train jobs to automatically recover.

If a Ray Train worker crashes, the Ray Train driver will handle that and restart training as long as fault tolerance is configured. <Link to the fault tolerance user guide.>


.. .. literalinclude:: ../doc_code/tuner.py
..     :language: python
..     :start-after: __basic_start__
..     :end-before: __basic_end__

Advanced: Using Ray Tune Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Tune callbacks should be passed into the :class:`ray.tune.RunConfig(callbacks) <ray.tune.RunConfig>` at the Tuner level. 

For Ray Train users that depend on behavior of built-in or custom Ray Tune callbacks, it's possible to use them by running Ray Train as a single trial Tune run
and passing in the callbacks to the Tuner.

If any callback functionality depends on reported metrics, make sure to pass the :class:`ray.tune.integration.ray_train.TuneReportCallback` to the trainer callbacks,
which propagates results to the Tuner. 

.. .. literalinclude:: ../doc_code/tuner.py
..     :language: python
..     :start-after: __basic_start__
..     :end-before: __basic_end__

Deprecation of the ``Tuner(trainer)`` API + Migration Guide
-----------------------------------------------------------

The old Tuner(trainer) API is deprecated in favor of the new usage pattern described in this user guide. The reasons for this change include (1) decoupling Ray Train and Ray Tune to have better separation of responsibilities and (2) improving the configuration user experience.

Find more context regarding this deprecation in the REP <link> and see the migration guide <link> for steps to migrate off the old API.

Please see <link to old API user guide> for the old API user guide.
