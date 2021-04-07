=============================
User Guide & Configuring Tune
=============================

These pages will demonstrate the various features and configurations of Tune.


.. tip:: Before you continue, be sure to have read :ref:`tune-60-seconds`.

This document provides an overview of the core concepts as well as some of the configurations for running Tune.

.. _tune-parallelism:

Resources (Parallelism, GPUs, Distributed)
------------------------------------------

.. tip:: To run everything sequentially, use :ref:`Ray Local Mode <tune-debugging>`.

Parallelism is determined by ``resources_per_trial`` (defaulting to 1 CPU, 0 GPU per trial) and the resources available to Tune (``ray.cluster_resources()``).

By default, Tune automatically runs N concurrent trials, where N is the number of CPUs (cores) on your machine.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 4 concurrent trials at a time.
    tune.run(trainable, num_samples=10)

You can override this parallelism with ``resources_per_trial``. Here you can
specify your resource requests using either a dictionary or a
:class:`PlacementGroupFactory <ray.tune.utils.placement_groups.PlacementGroupFactory>`
object. In any case, Ray Tune will try to start a placement group for each trial.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 2 concurrent trials at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2})

    # If you have 4 CPUs on your machine, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 4})

    # Fractional values are also supported, (i.e., {"cpu": 0.5}).
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 0.5})


Tune will allocate the specified GPU and CPU from ``resources_per_trial`` to each individual trial.
Even if the trial cannot be scheduled right now, Ray Tune will still try to start
the respective placement group. If not enough resources are available, this will trigger
:ref:`autoscaling behavior<cluster-index>` if you're using the Ray cluster launcher.

If your trainable function starts more remote workers, you will need to pass placement groups
factory objects to request these resources. See the
:class:`PlacementGroupFactory documentation <ray.tune.utils.placement_groups.PlacementGroupFactory>`
for further information.

Using GPUs
~~~~~~~~~~

To leverage GPUs, you must set ``gpu`` in ``tune.run(resources_per_trial)``. This will automatically set ``CUDA_VISIBLE_DEVICES`` for each trial.

.. code-block:: python

    # If you have 8 GPUs, this will run 8 trials at once.
    tune.run(trainable, num_samples=10, resources_per_trial={"gpu": 1})

    # If you have 4 CPUs on your machine and 1 GPU, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2, "gpu": 1})

You can find an example of this in the :doc:`Keras MNIST example </tune/examples/tune_mnist_keras>`.

.. warning:: If 'gpu' is not set, ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.

**Troubleshooting**: Occasionally, you may run into GPU memory issues when running a new trial. This may be
due to the previous trial not cleaning up its GPU state fast enough. To avoid this,
you can use ``tune.utils.wait_for_gpu`` - see :ref:`docstring <tune-util-ref>`.


Concurrent samples
~~~~~~~~~~~~~~~~~~

If using a :ref:`search algorithm <tune-search-alg>`, you may want to limit the number of trials that are being evaluated. For example, you may want to serialize the evaluation of trials to do sequential optimization.

In this case, ``ray.tune.suggest.ConcurrencyLimiter`` to limit the amount of concurrency:

.. code-block:: python

    algo = BayesOptSearch(utility_kwargs={
        "kind": "ucb",
        "kappa": 2.5,
        "xi": 0.0
    })
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()

See :ref:`limiter` for more details.



Distributed Tuning
~~~~~~~~~~~~~~~~~~

.. tip:: This section covers how to run Tune across multiple machines. See :ref:`Distributed Training <tune-dist-training>` for guidance in tuning distributed training jobs.

To attach to a Ray cluster, simply run ``ray.init`` before ``tune.run``. See :ref:`start-ray-cli` for more information about ``ray.init``:

.. code-block:: python

    # Connect to an existing distributed Ray cluster
    ray.init(address=<ray_address>)
    tune.run(trainable, num_samples=100, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 2, "GPU": 1}]))

Read more in the Tune :ref:`distributed experiments guide <tune-distributed>`.

.. _tune-dist-training:

Tune Distributed Training
~~~~~~~~~~~~~~~~~~~~~~~~~

To tune distributed training jobs, Tune provides a set of ``DistributedTrainableCreator`` for different training frameworks.
Below is an example for tuning distributed TensorFlow jobs:

.. code-block:: python

    # Please refer to full example in tf_distributed_keras_example.py
    from ray.tune.integration.tensorflow import DistributedTrainableCreator
    tf_trainable = DistributedTrainableCreator(
        train_mnist,
        use_gpu=args.use_gpu,
        num_workers=2)
    tune.run(tf_trainable,
             num_samples=1)

Read more about tuning :ref:`distributed PyTorch <tune-ddp-doc>`, :ref:`TensorFlow <tune-dist-tf-doc>` and :ref:`Horovod <tune-integration-horovod>` jobs.


.. _tune-default-search-space:

Search Space (Grid/Random)
--------------------------

You can specify a grid search or sampling distribution via the dict passed into ``tune.run(config=)``.

.. code-block:: python

    parameters = {
        "qux": tune.sample_from(lambda spec: 2 + 2),
        "bar": tune.grid_search([True, False]),
        "foo": tune.grid_search([1, 2, 3]),
        "baz": "asd",  # a constant value
    }

    tune.run(trainable, config=parameters)

By default, each random variable and grid search point is sampled once. To take multiple random samples, add ``num_samples: N`` to the experiment config. If `grid_search` is provided as an argument, the grid will be repeated ``num_samples`` of times.

.. code-block:: python
   :emphasize-lines: 13

    # num_samples=10 repeats the 3x3 grid search 10 times, for a total of 90 trials
    tune.run(
        my_trainable,
        name="my_trainable",
        config={
            "alpha": tune.uniform(100),
            "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal()),
            "nn_layers": [
                tune.grid_search([16, 64, 256]),
                tune.grid_search([16, 64, 256]),
            ],
        },
        num_samples=10
    )

Note that search spaces may not be interoperable across different search algorithms. For example, for many search algorithms, you will not be able to use a ``grid_search`` parameter. Read about this in the :ref:`Search Space API <tune-search-space>` page.

.. _tune-autofilled-metrics:

Auto-filled Metrics
-------------------

You can log arbitrary values and metrics in both training APIs:

.. code-block:: python

    def trainable(config):
        for i in range(num_epochs):
            ...
            tune.report(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)

    class Trainable(tune.Trainable):
        def step(self):
            ...
            # don't call report here!
            return dict(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)

During training, Tune will automatically log the below metrics in addition to the user-provided values. All of these can be used as stopping conditions or passed as a parameter to Trial Schedulers/Search Algorithms.

* ``config``: The hyperparameter configuration
* ``date``: String-formatted date and time when the result was processed
* ``done``: True if the trial has been finished, False otherwise
* ``episodes_total``: Total number of episodes (for RLLib trainables)
* ``experiment_id``: Unique experiment ID
* ``experiment_tag``: Unique experiment tag (includes parameter values)
* ``hostname``: Hostname of the worker
* ``iterations_since_restore``: The number of times ``tune.report()/trainable.train()`` has been
  called after restoring the worker from a checkpoint
* ``node_ip``: Host IP of the worker
* ``pid``: Process ID (PID) of the worker process
* ``time_since_restore``: Time in seconds since restoring from a checkpoint.
* ``time_this_iter_s``: Runtime of the current training iteration in seconds (i.e.
  one call to the trainable function or to ``_train()`` in the class API.
* ``time_total_s``: Total runtime in seconds.
* ``timestamp``: Timestamp when the result was processed
* ``timesteps_since_restore``: Number of timesteps since restoring from a checkpoint
* ``timesteps_total``: Total number of timesteps
* ``training_iteration``: The number of times ``tune.report()`` has been
  called
* ``trial_id``: Unique trial ID

All of these metrics can be seen in the ``Trial.last_result`` dictionary.

.. _tune-checkpoint:

Checkpointing
-------------

When running a hyperparameter search, Tune can automatically and periodically save/checkpoint your model. This allows you to:

 * save intermediate models throughout training
 * use pre-emptible machines (by automatically restoring from last checkpoint)
 * Pausing trials when using Trial Schedulers such as HyperBand and PBT.

To use Tune's checkpointing features, you must expose a ``checkpoint_dir`` argument in the function signature, and call ``tune.checkpoint_dir``:

.. code-block:: python

    import os
    import time
    from ray import tune

    def train_func(config, checkpoint_dir=None):
        start = 0
        if checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
                state = json.loads(f.read())
                start = state["step"] + 1

        for step in range(start, 100):
            time.sleep(1)

            # Obtain a checkpoint directory
            with tune.checkpoint_dir(step=step) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"step": step}))

            tune.report(hello="world", ray="tune")

    tune.run(train_func)

In this example, checkpoints will be saved by training iteration to ``local_dir/exp_name/trial_name/checkpoint_<step>``.

You can restore a single trial checkpoint by using ``tune.run(restore=<checkpoint_dir>)`` By doing this, you can change whatever experiments' configuration such as the experiment's name:

.. code-block:: python

    # Restored previous trial from the given checkpoint
    tune.run(
        "PG",
        name="RestoredExp", # The name can be different.
        stop={"training_iteration": 10}, # train 5 more iterations than previous
        restore="~/ray_results/Original/PG_<xxx>/checkpoint_5/checkpoint-5",
        config={"env": "CartPole-v0"},
    )


.. _tune-distributed-checkpointing:

Distributed Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~

On a multinode cluster, Tune automatically creates a copy of all trial checkpoints on the head node. This requires the Ray cluster to be started with the :ref:`cluster launcher <cluster-cloud>` and also requires rsync to be installed.

Note that you must use the ``tune.checkpoint_dir`` API to trigger syncing.

If you are running Ray Tune on Kubernetes, you should usually use a
:func:`DurableTrainable <ray.tune.durable>` or a shared filesystem for checkpoint sharing.
Please :ref`see here for best practices for running Tune on Kubernetes <tune-kubernetes>`.

If you do not use the cluster launcher, you should set up a NFS or global file system and
disable cross-node syncing:

.. code-block:: python

    sync_config = tune.SyncConfig(sync_to_driver=False)
    tune.run(func, sync_config=sync_config)


Stopping and resuming a tuning run
----------------------------------
Ray Tune periodically checkpoints the experiment state so that it can be
restarted when it fails or stops. The checkpointing period is
dynamically adjusted so that at least 95% of the time is used for handling
training results and scheduling.

If you send a SIGINT signal to the process running ``tune.run()`` (which is
usually what happens when you press Ctrl+C in the console), Ray Tune shuts
down training gracefully and saves a final experiment-level checkpoint. You
can then call ``tune.run()`` with ``resume=True`` to continue this run in
the future:

.. code-block:: python
    :emphasize-lines: 14

    tune.run(
        train,
        # ...
        name="my_experiment"
    )

    # This is interrupted e.g. by sending a SIGINT signal
    # Next time, continue the run like so:

    tune.run(
        train,
        # ...
        name="my_experiment",
        resume=True
    )

You will have to pass a ``name`` if you are using ``resume=True`` so that
Ray Tune can detect the experiment folder (which is usually stored at e.g.
``~/ray_results/my_experiment``). If you forgot to pass a name in the first
call, you can still pass the name when you resume the run. Please note that
in this case it is likely that your experiment name has a date suffix, so if you
ran ``tune.run(my_trainable)``, the ``name`` might look like something like this:
``my_trainable_2021-01-29_10-16-44``.

You can see which name you need to pass by taking a look at the results table
of your original tuning run:

.. code-block::
    :emphasize-lines: 5

    == Status ==
    Memory usage on this node: 11.0/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 1/16 CPUs, 0/0 GPUs, 0.0/4.69 GiB heap, 0.0/1.61 GiB objects
    Result logdir: /Users/ray/ray_results/my_trainable_2021-01-29_10-16-44
    Number of trials: 1/1 (1 RUNNING)


Handling Large Datasets
-----------------------

You often will want to compute a large object (e.g., training data, model weights) on the driver and use that object within each trial.

Tune provides a wrapper function ``tune.with_parameters()`` that allows you to broadcast large objects to your trainable.
Objects passed with this wrapper will be stored on the Ray object store and will be automatically fetched
and passed to your trainable as a parameter.

.. code-block:: python

    from ray import tune

    import numpy as np

    def f(config, data=None):
        pass
        # use data

    data = np.random.random(size=100000000)

    tune.run(tune.with_parameters(f, data=data))

.. _tune-stopping:

Stopping Trials
---------------

You can control when trials are stopped early by passing the ``stop`` argument to ``tune.run``.
This argument takes, a dictionary, a function, or a :class:`Stopper <ray.tune.stopper.Stopper>` class
as an argument.

If a dictionary is passed in, the keys may be any field in the return result of ``tune.report`` in the Function API or ``step()`` (including the results from ``step`` and auto-filled metrics).

In the example below, each trial will be stopped either when it completes 10 iterations OR when it reaches a mean accuracy of 0.98. These metrics are assumed to be **increasing**.

.. code-block:: python

    # training_iteration is an auto-filled metric by Tune.
    tune.run(
        my_trainable,
        stop={"training_iteration": 10, "mean_accuracy": 0.98}
    )

For more flexibility, you can pass in a function instead. If a function is passed in, it must take ``(trial_id, result)`` as arguments and return a boolean (``True`` if trial should be stopped and ``False`` otherwise).

.. code-block:: python


    def stopper(trial_id, result):
        return result["mean_accuracy"] / result["training_iteration"] > 5

    tune.run(my_trainable, stop=stopper)

Finally, you can implement the :class:`Stopper <ray.tune.stopper.Stopper>` abstract class for stopping entire experiments. For example, the following example stops all trials after the criteria is fulfilled by any individual trial, and prevents new ones from starting:

.. code-block:: python

    from ray.tune import Stopper

    class CustomStopper(Stopper):
        def __init__(self):
            self.should_stop = False

        def __call__(self, trial_id, result):
            if not self.should_stop and result['foo'] > 10:
                self.should_stop = True
            return self.should_stop

        def stop_all(self):
            """Returns whether to stop trials and prevent new ones from starting."""
            return self.should_stop

    stopper = CustomStopper()
    tune.run(my_trainable, stop=stopper)


Note that in the above example the currently running trials will not stop immediately but will do so once their current iterations are complete.

Ray Tune comes with a set of out-of-the-box stopper classes. See the :ref:`Stopper <tune-stoppers>` documentation.

.. _tune-logging:

Logging
-------

Tune by default will log results for Tensorboard, CSV, and JSON formats. If you need to log something lower level like model weights or gradients, see :ref:`Trainable Logging <trainable-logging>`.

**Learn more about logging and customizations here**: :ref:`loggers-docstring`.

Tune will log the results of each trial to a subfolder under a specified local dir, which defaults to ``~/ray_results``.

.. code-block:: bash

    # This logs to 2 different trial folders:
    # ~/ray_results/trainable_name/trial_name_1 and ~/ray_results/trainable_name/trial_name_2
    # trainable_name and trial_name are autogenerated.
    tune.run(trainable, num_samples=2)

You can specify the ``local_dir`` and ``trainable_name``:

.. code-block:: python

    # This logs to 2 different trial folders:
    # ./results/test_experiment/trial_name_1 and ./results/test_experiment/trial_name_2
    # Only trial_name is autogenerated.
    tune.run(trainable, num_samples=2, local_dir="./results", name="test_experiment")

To specify custom trial folder names, you can pass use the ``trial_name_creator`` argument
to `tune.run`.  This takes a function with the following signature:

.. code-block:: python

    def trial_name_string(trial):
        """
        Args:
            trial (Trial): A generated trial object.

        Returns:
            trial_name (str): String representation of Trial.
        """
        return str(trial)

    tune.run(
        MyTrainableClass,
        name="example-experiment",
        num_samples=1,
        trial_name_creator=trial_name_string
    )

See the documentation on Trials: :ref:`trial-docstring`.

.. _tensorboard:

Tensorboard (Logging)
---------------------

Tune automatically outputs Tensorboard files during ``tune.run``. To visualize learning in tensorboard, install tensorboardX:

.. code-block:: bash

    $ pip install tensorboardX

Then, after you run an experiment, you can visualize your experiment with TensorBoard by specifying the output directory of your results.

.. code-block:: bash

    $ tensorboard --logdir=~/ray_results/my_experiment

If you are running Ray on a remote multi-user cluster where you do not have sudo access, you can run the following commands to make sure tensorboard is able to write to the tmp directory:

.. code-block:: bash

    $ export TMPDIR=/tmp/$USER; mkdir -p $TMPDIR; tensorboard --logdir=~/ray_results

.. image:: ../ray-tune-tensorboard.png

If using TF2, Tune also automatically generates TensorBoard HParams output, as shown below:

.. code-block:: python

    tune.run(
        ...,
        config={
            "lr": tune.grid_search([1e-5, 1e-4]),
            "momentum": tune.grid_search([0, 0.9])
        }
    )

.. image:: ../images/tune-hparams.png

Console Output
--------------

User-provided fields will be outputted automatically on a best-effort basis. You can use a :ref:`Reporter <tune-reporter-doc>` object to customize the console output.

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 4/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 4 (4 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
    | Trial name           | status   | loc                 |    param1 | param2 |    acc | total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+----------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.1289 |        7.54952 |    15 |
    | MyTrainable_a8263fc6 | RUNNING  | 10.234.98.164:31117 | 0.929276  | 0.158  | 0.4865 |        7.0501  |    14 |
    | MyTrainable_a8267914 | RUNNING  | 10.234.98.164:31111 | 0.068426  | 0.0319 | 0.9585 |        7.0477  |    14 |
    | MyTrainable_a826b7bc | RUNNING  | 10.234.98.164:31112 | 0.729127  | 0.0748 | 0.1797 |        7.05715 |    14 |
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+



Uploading Results
-----------------

If an upload directory is provided, Tune will automatically sync results from the ``local_dir`` to the given directory, natively supporting standard S3/gsutil/HDFS URIs.

.. code-block:: python

    tune.run(
        MyTrainableClass,
        local_dir="~/ray_results",
        sync_config=tune.SyncConfig(upload_dir="s3://my-log-dir")
    )

You can customize this to specify arbitrary storages with the ``sync_to_cloud`` argument in ``tune.SyncConfig``. This argument supports either strings with the same replacement fields OR arbitrary functions.

.. code-block:: python

    tune.run(
        MyTrainableClass,
        sync_config=tune.SyncConfig(
            upload_dir="s3://my-log-dir",
            sync_to_cloud=custom_sync_str_or_func
        )
    )

If a string is provided, then it must include replacement fields ``{source}`` and ``{target}``, like ``s3 sync {source} {target}``. Alternatively, a function can be provided with the following signature:

.. code-block:: python

    def custom_sync_func(source, target):
        # do arbitrary things inside
        sync_cmd = "s3 {source} {target}".format(
            source=source,
            target=target)
        sync_process = subprocess.Popen(sync_cmd, shell=True)
        sync_process.wait()

By default, syncing occurs every 300 seconds. To change the frequency of syncing, set the ``TUNE_CLOUD_SYNC_S`` environment variable in the driver to the desired syncing period.

Note that uploading only happens when global experiment state is collected, and the frequency of this is determined by the ``TUNE_GLOBAL_CHECKPOINT_S`` environment variable. So the true upload period is given by ``max(TUNE_CLOUD_SYNC_S, TUNE_GLOBAL_CHECKPOINT_S)``.


.. _tune-docker:

Using Tune with Docker
----------------------
Tune automatically syncs files and checkpoints between different remote
containers as needed.

To make this work in your Docker cluster, e.g. when you are using the Ray autoscaler
with docker containers, you will need to pass a
``DockerSyncer`` to the ``sync_to_driver`` argument of ``tune.SyncConfig``.

.. code-block:: python

    from ray.tune.integration.docker import DockerSyncer
    sync_config = tune.SyncConfig(
        sync_to_driver=DockerSyncer)

    tune.run(train, sync_config=sync_config)


.. _tune-kubernetes:

Using Tune with Kubernetes
--------------------------
Ray Tune automatically synchronizes files and checkpoints between different remote nodes as needed.
This usually happens via SSH, but this can be a :ref:`performance bottleneck <tune-bottlenecks>`,
especially when running many trials in parallel.

Instead you should use shared storage for checkpoints so that no additional synchronization across nodes
is necessary. There are two main options.

First, you can use a :func:`DurableTrainable <ray.tune.durable>` to store your
logs and checkpoints on cloud storage, such as AWS S3 or Google Cloud Storage:

.. code-block:: python

    from ray import tune

    tune.run(
        tune.durable(train_fn),
        # ...,
        sync_config=tune.SyncConfig(
            sync_to_driver=False,
            upload_dir="s3://your-s3-bucket/durable-trial/"
        )
    )

Second, you can set up a shared file system like NFS. If you do this, disable automatic trial syncing:

.. code-block:: python

    from ray import tune

    tune.run(
        train_fn,
        # ...,
        local_dir="/path/to/shared/storage",
        sync_config=tune.SyncConfig(
            # Do not sync to driver because we are on shared storage
            sync_to_driver=False
        )
    )


Lastly, if you still want to use ssh for trial synchronization, but are not running
on the Ray cluster launcher, you might need to pass a
``KubernetesSyncer`` to the ``sync_to_driver`` argument of ``tune.SyncConfig``.
You have to specify your Kubernetes namespace explicitly:

.. code-block:: python

    from ray.tune.integration.kubernetes import NamespacedKubernetesSyncer
    sync_config = tune.SyncConfig(
        sync_to_driver=NamespacedKubernetesSyncer("ray")
    )

    tune.run(train, sync_config=sync_config)


Please note that we strongly encourage you to use one of the other two options instead, as they will
result in less overhead and don't require pods to SSH into each other.


.. _tune-log_to_file:

Redirecting stdout and stderr to files
--------------------------------------
The stdout and stderr streams are usually printed to the console. For remote actors,
Ray collects these logs and prints them to the head process.

However, if you would like to collect the stream outputs in files for later
analysis or troubleshooting, Tune offers an utility parameter, ``log_to_file``,
for this.

By passing ``log_to_file=True`` to ``tune.run()``, stdout and stderr will be logged
to ``trial_logdir/stdout`` and ``trial_logdir/stderr``, respectively:

.. code-block:: python

    tune.run(
        trainable,
        log_to_file=True)

If you would like to specify the output files, you can either pass one filename,
where the combined output will be stored, or two filenames, for stdout and stderr,
respectively:

.. code-block:: python

    tune.run(
        trainable,
        log_to_file="std_combined.log")

    tune.run(
        trainable,
        log_to_file=("my_stdout.log", "my_stderr.log"))

The file names are relative to the trial's logdir. You can pass absolute paths,
too.

If ``log_to_file`` is set, Tune will automatically register a new logging handler
for Ray's base logger and log the output to the specified stderr output file.

.. _tune-callbacks:

Callbacks
---------

Ray Tune supports callbacks that are called during various times of the training process.
Callbacks can be passed as a parameter to ``tune.run()``, and the submethod will be
invoked automatically.

This simple callback just prints a metric each time a result is received:

.. code-block:: python

    from ray import tune
    from ray.tune import Callback


    class MyCallback(Callback):
        def on_trial_result(self, iteration, trials, trial, result, **info):
            print(f"Got result: {result['metric']}")


    def train(config):
        for i in range(10):
            tune.report(metric=i)


    tune.run(
        train,
        callbacks=[MyCallback()])

For more details and available hooks, please :ref:`see the API docs for Ray Tune callbacks <tune-callbacks-docs>`.


.. _tune-debugging:

Debugging
---------

By default, Tune will run hyperparameter evaluations on multiple processes. However, if you need to debug your training process, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process with ``local_mode`` by calling the following before ``tune.run``.

.. code-block:: python

    ray.init(local_mode=True)

Local mode with multiple configuration evaluations will interleave computation, so it is most naturally used when running a single configuration evaluation.

Note that ``local_mode`` has some known issues, so please read :ref:`these tips <local-mode-tips>` for more info.

Stopping after the first failure
--------------------------------

By default, ``tune.run`` will continue executing until all trials have terminated or errored. To stop the entire Tune run as soon as **any** trial errors:

.. code-block:: python

    tune.run(trainable, fail_fast=True)

This is useful when you are trying to setup a large hyperparameter experiment.

Environment variables
---------------------
Some of Ray Tune's behavior can be configured using environment variables.
These are the environment variables Ray Tune currently considers:

* **TUNE_CLUSTER_SSH_KEY**: SSH key used by the Tune driver process to connect
  to remote cluster machines for checkpoint syncing. If this is not set,
  ``~/ray_bootstrap_key.pem`` will be used.
* **TUNE_DISABLE_AUTO_CALLBACK_LOGGERS**: Ray Tune automatically adds a CSV and
  JSON logger callback if they haven't been passed. Setting this variable to
  `1` disables this automatic creation. Please note that this will most likely
  affect analyzing your results after the tuning run.
* **TUNE_DISABLE_AUTO_CALLBACK_SYNCER**: Ray Tune automatically adds a
  Syncer callback to sync logs and checkpoints between different nodes if none
  has been passed. Setting this variable to `1` disables this automatic creation.
  Please note that this will most likely affect advanced scheduling algorithms
  like PopulationBasedTraining.
* **TUNE_DISABLE_AUTO_INIT**: Disable automatically calling ``ray.init()`` if
  not attached to a Ray session.
* **TUNE_DISABLE_DATED_SUBDIR**: Ray Tune automatically adds a date string to experiment
  directories when the name is not specified explicitly or the trainable isn't passed
  as a string. Setting this environment variable to ``1`` disables adding these date strings.
* **TUNE_DISABLE_STRICT_METRIC_CHECKING**: When you report metrics to Tune via
  ``tune.report()`` and passed a ``metric`` parameter to ``tune.run()``, a scheduler,
  or a search algorithm, Tune will error
  if the metric was not reported in the result. Setting this environment variable
  to ``1`` will disable this check.
* **TUNE_DISABLE_SIGINT_HANDLER**: Ray Tune catches SIGINT signals (e.g. sent by
  Ctrl+C) to gracefully shutdown and do a final checkpoint. Setting this variable
  to ``1`` will disable signal handling and stop execution right away. Defaults to
  ``0``.
* **TUNE_FUNCTION_THREAD_TIMEOUT_S**: Time in seconds the function API waits
  for threads to finish after instructing them to complete. Defaults to ``2``.
* **TUNE_GLOBAL_CHECKPOINT_S**: Time in seconds that limits how often Tune's
  experiment state is checkpointed. If not set this will default to ``10``.
* **TUNE_MAX_LEN_IDENTIFIER**: Maximum length of trial subdirectory names (those
  with the parameter values in them)
* **TUNE_MAX_PENDING_TRIALS_PG**: Maximum number of pending trials when placement groups are used. Defaults
  to ``auto``, which will be updated to ``1000`` for random/grid search and ``1`` for any other search algorithms.
* **TUNE_PLACEMENT_GROUP_AUTO_DISABLED**: Ray Tune automatically uses placement groups
  instead of the legacy resource requests. Setting this to 1 enables legacy placement.
* **TUNE_PLACEMENT_GROUP_CLEANUP_DISABLED**: Ray Tune cleans up existing placement groups
  with the ``_tune__`` prefix in their name before starting a run. This is used to make sure
  that scheduled placement groups are removed when multiple calls to ``tune.run()`` are
  done in the same script. You might want to disable this if you run multiple Tune runs in
  parallel from different scripts. Set to 1 to disable.
* **TUNE_PLACEMENT_GROUP_PREFIX**: Prefix for placement groups created by Ray Tune. This prefix is used
  e.g. to identify placement groups that should be cleaned up on start/stop of the tuning run. This is
  initialized to a unique name at the start of the first run.
* **TUNE_PLACEMENT_GROUP_RECON_INTERVAL**: How often to reconcile placement groups. Reconcilation is
  used to make sure that the number of requested placement groups and pending/running trials are in sync.
  In normal circumstances these shouldn't differ anyway, but reconcilation makes sure to capture cases when
  placement groups are manually destroyed. Reconcilation doesn't take much time, but it can add up when
  running a large number of short trials. Defaults to every ``5`` (seconds).
* **TUNE_PLACEMENT_GROUP_WAIT_S**: Default time the trial executor waits for placement
  groups to be placed before continuing the tuning loop. Setting this to a float
  will block for that many seconds. This is mostly used for testing purposes. Defaults
  to -1, which disables blocking.
* **TUNE_RESULT_DIR**: Directory where Ray Tune trial results are stored. If this
  is not set, ``~/ray_results`` will be used.
* **TUNE_RESULT_BUFFER_LENGTH**: Ray Tune can buffer results from trainables before they are passed
  to the driver. Enabling this might delay scheduling decisions, as trainables are speculatively
  continued. Setting this to ``0`` disables result buffering. Defaults to 1000 (results).
* **TUNE_RESULT_BUFFER_MAX_TIME_S**: Similarly, Ray Tune buffers results up to ``number_of_trial/10`` seconds,
  but never longer than this value. Defaults to 100 (seconds).
* **TUNE_RESULT_BUFFER_MIN_TIME_S**: Additionally, you can specify a minimum time to buffer results. Defaults to 0.
* **TUNE_SYNCER_VERBOSITY**: Amount of command output when using Tune with Docker Syncer. Defaults to 0.
* **TUNE_TRIAL_STARTUP_GRACE_PERIOD**: Amount of time after starting a trial that Ray Tune checks for successful
  trial startups. After the grace period, Tune will block until a result from a running trial is received. Can
  be disabled by setting this to lower or equal to 0.
* **TUNE_WARN_THRESHOLD_S**: Threshold for logging if an Tune event loop operation takes too long. Defaults to 0.5 (seconds).
* **TUNE_STATE_REFRESH_PERIOD**: Frequency of updating the resource tracking from Ray. Defaults to 10 (seconds).


There are some environment variables that are mostly relevant for integrated libraries:

* **SIGOPT_KEY**: SigOpt API access key.
* **WANDB_API_KEY**: Weights and Biases API key. You can also use ``wandb login``
  instead.


Further Questions or Issues?
----------------------------

.. include:: /_help.rst
