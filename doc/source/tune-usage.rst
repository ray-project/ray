.. _tune-user-guide:

Tune User Guide
===============

The basic Tune API [``tune.run(Trainable)``] has two main parts: a :ref:`Training API <guide-training-api>` and :ref:`tune.run <guide-running-tune>`.

.. _guide-training-api:

Training API
------------

Training can be done with either a **Class API** (``tune.Trainable``) or **function-based API** (``track.log``). Here is an example ``tune.Trainable`` that you can use to dry-run Tune:

.. code-block:: python

    from ray import tune

    class trainable(tune.Trainable):
        def _setup(self, config):
            if config["print_me"]:
                print(config["print_me"])

        def _train(self):
            # run one step of training code.
            # important: this method is called repeatedly!
            result_dict = {"accuracy": 0.5, "f1": 0.1, ...}
            return result_dict

    tune.run(trainable, config={"print_me": "hello-world"}, stop={"training_iteration": 200})

The **function-based API** is for fast prototyping but has limited functionality. Here is a **function-based API** example:

.. code-block:: python

    from ray import tune
    import time

    def trainable(config):
        if config["print_me"]:
            print(config["print_me"])

        for i in range(200):
            time.sleep(1)
            result_dict = {"accuracy": 0.5, "f1": 0.1, ...}
            tune.track.log(**result_dict)

    tune.run(trainable, config={"print_me": "hello-world"})

To read more, check out the :ref:`Trainable API docs<trainable-docs>`.

.. _guide-running-tune:

Running Tune
------------

Use ``tune.run`` to generate and execute your hyperparameter sweep:

.. code-block:: python

    tune.run(trainable)

    # Run a total of 10 evaluations of the Trainable. Tune runs in
    # parallel and automatically determines concurrency.
    tune.run(trainable, num_samples=10)

This function will report status on the command line until all Trials stop:

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 4/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 4 (4 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+--------+--------+------------------+-------+
    | Trial name           | status   | loc                 |    param1 | param2 | param3 |    acc |   loss |   total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+--------+--------+------------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.4328 | 0.1289 | 1.8572 |          7.54952 |    15 |
    | MyTrainable_a8263fc6 | RUNNING  | 10.234.98.164:31117 | 0.929276  | 0.158  | 0.3417 | 0.4865 | 1.6307 |          7.0501  |    14 |
    | MyTrainable_a8267914 | RUNNING  | 10.234.98.164:31111 | 0.068426  | 0.0319 | 0.1147 | 0.9585 | 1.9603 |          7.0477  |    14 |
    | MyTrainable_a826b7bc | RUNNING  | 10.234.98.164:31112 | 0.729127  | 0.0748 | 0.1784 | 0.1797 | 1.7161 |          7.05715 |    14 |
    +----------------------+----------+---------------------+-----------+--------+--------+--------+--------+------------------+-------+

All results reported by the trainable will be logged locally to a unique directory per experiment, e.g. ``~/ray_results/example-experiment`` in the above example. On a cluster, incremental results will be synced to local disk on the head node. All results will have `autofilled metrics <tune-usage.html#auto-filled-results>`__ in addition to your own user-defined metrics.

Trial Parallelism
~~~~~~~~~~~~~~~~~

Tune automatically runs N concurrent trials, where N is the number of CPUs (cores) on your machine. By default, Tune assumes that each trial will only require 1 CPU. You can override this with ``resources_per_trial``:

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 4 concurrent trials at a time.
    tune.run(trainable, num_samples=10)

    # If you have 4 CPUs on your machine, this will run 2 concurrent trials at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2})

    # If you have 4 CPUs on your machine, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 4})

To leverage GPUs, you can set ``gpu`` in ``resources_per_trial``.  A trial will only be executed if there are resources available. See the section on `resource allocation <tune-usage.html#resource-allocation-using-gpus>`_, which provides more details about GPU usage and trials that are distributed:

.. code-block:: python

    # If you have 4 CPUs on your machine and 1 GPU, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2, "gpu": 1})


To attach to a Ray cluster or use ``ray.init`` manual resource overrides, simply run ``ray.init`` before ``tune.run``:

.. code-block:: python

    # Setup a local ray cluster and override resources. This will run 50 trials in parallel:
    ray.init(num_cpus=100)
    tune.run(trainable, num_samples=100, resources_per_trial={"cpu": 2})

    # Connect to an existing distributed Ray cluster
    ray.init(address=<ray_redis_address>)
    tune.run(trainable, num_samples=100, resources_per_trial={"cpu": 2, "gpu": 1})

.. tip:: To run everything sequentially, use `Ray Local Mode <tune-usage.html#debugging>`_.


Analyzing Results
-----------------

Tune provides an ``ExperimentAnalysis`` object for analyzing results from ``tune.run``.

.. code-block:: python

    analysis = tune.run(
        trainable,
        name="example-experiment",
        num_samples=10,
    )

You can use the ``ExperimentAnalysis`` object to obtain the best configuration of the experiment:

.. code-block:: python

    >>> print("Best config is", analysis.get_best_config(metric="mean_accuracy"))
    Best config is: {'lr': 0.011537575723482687, 'momentum': 0.8921971713692662}


See the full documentation for the ``Analysis`` object: :ref:`exp-analysis-docstring`.


Grid Search/Random Search
-------------------------

.. warning:: If you use a Search Algorithm, you may not be able to specify lambdas or grid search with this
    interface, as the search algorithm may require a different search space declaration.

You can specify a grid search or random search via the dict passed into ``tune.run(config=)``.

.. code-block:: python

    tune.run(
        trainable,
        config={
            "qux": tune.sample_from(lambda spec: 2 + 2),
            "bar": tune.grid_search([True, False]),
            "foo": tune.grid_search([1, 2, 3]),
            "baz": "asd",
        }
    )

Read about this in the :ref:`Grid/Random Search API <tune-grid-random>` page.

Custom Trial Names
------------------

To specify custom trial names, you can pass use the ``trial_name_creator`` argument
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

An example can be found in `logging_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py>`__.

Sampling Multiple Times
-----------------------

By default, each random variable and grid search point is sampled once. To take multiple random samples, add ``num_samples: N`` to the experiment config. If `grid_search` is provided as an argument, the grid will be repeated `num_samples` of times.

.. code-block:: python
   :emphasize-lines: 12

    tune.run(
        my_trainable,
        name="my_trainable",
        config={
            "alpha": tune.sample_from(lambda spec: np.random.uniform(100)),
            "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal()),
            "nn_layers": [
                tune.grid_search([16, 64, 256]),
                tune.grid_search([16, 64, 256]),
            ],
        },
        num_samples=10
    )

E.g. in the above, ``num_samples=10`` repeats the 3x3 grid search 10 times, for a total of 90 trials, each with randomly sampled values of ``alpha`` and ``beta``.


Resource Allocation (Using GPUs)
--------------------------------

Tune will allocate the specified GPU and CPU ``resources_per_trial`` to each individual trial (defaulting to 1 CPU per trial). Under the hood, Tune runs each trial as a Ray actor, using Ray's resource handling to allocate resources and place actors. A trial will not be scheduled unless at least that amount of resources is available in the cluster, preventing the cluster from being overloaded.

Fractional values are also supported, (i.e., ``"gpu": 0.2``). You can find an example of this in the `Keras MNIST example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_keras.py>`__.

If GPU resources are not requested, the ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.
Otherwise, it will be set to the GPUs in the list (this is managed by Ray).

Advanced Resource Allocation
----------------------------

Trainables can themselves be distributed. If your trainable function / class creates further Ray actors or tasks that also consume CPU / GPU resources, you will also want to set ``extra_cpu`` or ``extra_gpu`` to reserve extra resource slots for the actors you will create. For example, if a trainable class requires 1 GPU itself, but will launch 4 actors each using another GPU, then it should set ``"gpu": 1, "extra_gpu": 4``.

.. code-block:: python
   :emphasize-lines: 4-8

    tune.run(
        my_trainable,
        name="my_trainable",
        resources_per_trial={
            "cpu": 1,
            "gpu": 1,
            "extra_gpu": 4
        }
    )

The ``Trainable`` also provides the ``default_resource_requests`` interface to automatically declare the ``resources_per_trial`` based on the given configuration.

.. automethod:: ray.tune.Trainable.default_resource_request
    :noindex:


Trainable (Trial) Checkpointing
-------------------------------

When running a hyperparameter search, Tune can automatically and periodically save/checkpoint your model. Checkpointing is used for

 * saving a model at the end of training
 * modifying a model in the middle of training
 * fault-tolerance in experiments with pre-emptible machines.
 * enables certain Trial Schedulers such as HyperBand and PBT.

To enable checkpointing, you must implement a `Trainable class <tune-usage.html#trainable-api>`__ (Trainable functions are not checkpointable, since they never return control back to their caller).

Checkpointing assumes that the model state will be saved to disk on whichever node the Trainable is running on. You can checkpoint with three different mechanisms: manually, periodically, and at termination.

**Manual Checkpointing**: A custom Trainable can manually trigger checkpointing by returning ``should_checkpoint: True`` (or ``tune.result.SHOULD_CHECKPOINT: True``) in the result dictionary of `_train`. This can be especially helpful in spot instances:

.. code-block:: python

    def _train(self):
        # training code
        result = {"mean_accuracy": accuracy}
        if detect_instance_preemption():
            result.update(should_checkpoint=True)
        return result


**Periodic Checkpointing**: periodic checkpointing can be used to provide fault-tolerance for experiments. This can be enabled by setting ``checkpoint_freq=<int>`` and ``max_failures=<int>`` to checkpoint trials every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        max_failures=5,
    )

**Checkpointing at Termination**: The checkpoint_freq may not coincide with the exact end of an experiment. If you want a checkpoint to be created at the end
of a trial, you can additionally set the ``checkpoint_at_end=True``:

.. code-block:: python
   :emphasize-lines: 5

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        checkpoint_at_end=True,
        max_failures=5,
    )

The checkpoint will be saved at a path that looks like ``local_dir/exp_name/trial_name/checkpoint_x/``, where the x is the number of iterations so far when the checkpoint is saved. To restore the checkpoint, you can use the ``restore`` argument and specify a checkpoint file. By doing this, you can change whatever experiments' configuration such as the experiment's name, the training iteration or so:

.. code-block:: python

    # Restored previous trial from the given checkpoint
    tune.run(
        "PG",
        name="RestoredExp", # The name can be different.
        stop={"training_iteration": 10}, # train 5 more iterations than previous
        restore="~/ray_results/Original/PG_<xxx>/checkpoint_5/checkpoint-5",
        config={"env": "CartPole-v0"},
    )

.. _tune-fault-tol:

Fault Tolerance
---------------

Tune will automatically restart trials in case of trial failures/error (if ``max_failures != 0``), both in the single node and distributed setting.

Tune will restore trials from the latest checkpoint, where available. In the distributed setting, if using the autoscaler with ``rsync`` enabled, Tune will automatically sync the trial folder with the driver. For example, if a node is lost while a trial (specifically, the corresponding Trainable actor of the trial) is still executing on that node and a checkpoint of the trial exists, Tune will wait until available resources are available to begin executing the trial again.

If the trial/actor is placed on a different node, Tune will automatically push the previous checkpoint file to that node and restore the remote trial actor state, allowing the trial to resume from the latest checkpoint even after failure.

Take a look at an example: :ref:`tune-distributed-spot`.

Recovering From Failures
~~~~~~~~~~~~~~~~~~~~~~~~

Tune automatically persists the progress of your entire experiment (a ``tune.run`` session), so if an experiment crashes or is otherwise cancelled, it can be resumed by passing one of True, False, "LOCAL", "REMOTE", or "PROMPT" to ``tune.run(resume=...)``. Note that this only works if trial checkpoints are detected, whether it be by manual or periodic checkpointing.

**Settings:**

 - The default setting of ``resume=False`` creates a new experiment.
 - ``resume="LOCAL"`` and ``resume=True`` restore the experiment from ``local_dir/[experiment_name]``.
 - ``resume="REMOTE"`` syncs the upload dir down to the local dir and then restores the experiment from ``local_dir/experiment_name``.
 - ``resume="PROMPT"`` will cause Tune to prompt you for whether you want to resume. You can always force a new experiment to be created by changing the experiment name.

Note that trials will be restored to their last checkpoint. If trial checkpointing is not enabled, unfinished trials will be restarted from scratch.

E.g.:

.. code-block:: python

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        local_dir="~/path/to/results",
        resume=True
    )

Upon a second run, this will restore the entire experiment state from ``~/path/to/results/my_experiment_name``. Importantly, any changes to the experiment specification upon resume will be ignored. For example, if the previous experiment has reached its termination, then resuming it with a new stop criterion makes no effect: the new experiment will terminate immediately after initialization. If you want to change the configuration, such as training more iterations, you can do so restore the checkpoint by setting ``restore=<path-to-checkpoint>`` - note that this only works for a single trial.

.. warning::

    This feature is still experimental, so any provided Trial Scheduler or Search Algorithm will not be preserved. Only ``FIFOScheduler`` and ``BasicVariantGenerator`` will be supported.


Handling Large Datasets
-----------------------

You often will want to compute a large object (e.g., training data, model weights) on the driver and use that object within each trial. Tune provides a ``pin_in_object_store`` utility function that can be used to broadcast such large objects. Objects pinned in this way will never be evicted from the Ray object store while the driver process is running, and can be efficiently retrieved from any task via ``get_pinned_object``.

.. code-block:: python

    import ray
    from ray import tune
    from ray.tune.utils import pin_in_object_store, get_pinned_object

    import numpy as np

    ray.init()

    # X_id can be referenced in closures
    X_id = pin_in_object_store(np.random.random(size=100000000))

    def f(config, reporter):
        X = get_pinned_object(X_id)
        # use X

    tune.run(f)

Custom Stopping Criteria
------------------------

You can control when trials are stopped early by passing the ``stop`` argument to ``tune.run``. This argument takes either a dictionary or a function.

If a dictionary is passed in, the keys may be any field in the return result of ``tune.track.log`` in the Function API or ``train()`` (including the results from ``_train`` and auto-filled metrics).

In the example below, each trial will be stopped either when it completes 10 iterations OR when it reaches a mean accuracy of 0.98. Note that `training_iteration` is an auto-filled metric by Tune.

.. code-block:: python

    tune.run(
        my_trainable,
        stop={"training_iteration": 10, "mean_accuracy": 0.98}
    )

For more flexibility, you can pass in a function instead. If a function is passed in, it must take ``(trial_id, result)`` as arguments and return a boolean (``True`` if trial should be stopped and ``False`` otherwise).

.. code-block:: python


    def stopper(trial_id, result):
        return result["mean_accuracy"] / result["training_iteration"] > 5

    tune.run(my_trainable, stop=stopper)

Finally, you can implement the ``Stopper`` abstract class for stopping entire experiments. For example, the following example stops all trials after the criteria is fulfilled by any individual trial, and prevents new ones from starting:

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

Auto-Filled Results
-------------------

During training, Tune will automatically fill certain fields if not already provided. All of these can be used as stopping conditions or in the Scheduler/Search Algorithm specification.

.. literalinclude:: ../../python/ray/tune/result.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

The following fields will automatically show up on the console output, if provided:

1. ``episode_reward_mean``
2. ``mean_loss``
3. ``mean_accuracy``
4. ``timesteps_this_iter`` (aggregated into ``timesteps_total``).


TensorBoard
-----------

To visualize learning in tensorboard, install tensorboardX:

.. code-block:: bash

    $ pip install tensorboardX

Then, after you run a experiment, you can visualize your experiment with TensorBoard by specifying the output directory of your results. Note that if you running Ray on a remote cluster, you can forward the tensorboard port to your local machine through SSH using ``ssh -L 6006:localhost:6006 <address>``:

.. code-block:: bash

    $ tensorboard --logdir=~/ray_results/my_experiment

If you are running Ray on a remote multi-user cluster where you do not have sudo access, you can run the following commands to make sure tensorboard is able to write to the tmp directory:

.. code-block:: bash

    $ export TMPDIR=/tmp/$USER; mkdir -p $TMPDIR; tensorboard --logdir=~/ray_results

.. image:: ray-tune-tensorboard.png

If using TF2, Tune also automatically generates TensorBoard HParams output, as shown below:

.. code-block:: python

    tune.run(
        ...,
        config={
            "lr": tune.grid_search([1e-5, 1e-4]),
            "momentum": tune.grid_search([0, 0.9])
        }
    )

.. image:: images/tune-hparams.png


Logging
-------

You can pass in your own logging mechanisms to output logs in custom formats as follows:

.. code-block:: python

    from ray.tune.logger import DEFAULT_LOGGERS

    tune.run(
        MyTrainableClass,
        name="experiment_name",
        loggers=DEFAULT_LOGGERS + (CustomLogger1, CustomLogger2)
    )

These loggers will be called along with the default Tune loggers. All loggers must inherit the Logger interface (:ref:`logger-interface`). Tune enables default loggers for Tensorboard, CSV, and JSON formats. You can also check out `logger.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/logger.py>`__ for implementation details. An example can be found in `logging_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py>`__. See the :ref:`Logging API <logger-interface>`.

Uploading/Syncing
-----------------

Tune automatically syncs the trial folder on remote nodes back to the head node. This requires the ray cluster to be started with the `autoscaler <autoscaling.html>`__.
By default, local syncing requires rsync to be installed. You can customize the sync command with the ``sync_to_driver`` argument in ``tune.run`` by providing either a function or a string.

If a string is provided, then it must include replacement fields ``{source}`` and ``{target}``, like ``rsync -savz -e "ssh -i ssh_key.pem" {source} {target}``. Alternatively, a function can be provided with the following signature:

.. code-block:: python

    def custom_sync_func(source, target):
        sync_cmd = "rsync {source} {target}".format(
            source=source,
            target=target)
        sync_process = subprocess.Popen(sync_cmd, shell=True)
        sync_process.wait()

    tune.run(
        MyTrainableClass,
        name="experiment_name",
        sync_to_driver=custom_sync_func,
    )

When syncing results back to the driver, the source would be a path similar to ``ubuntu@192.0.0.1:/home/ubuntu/ray_results/trial1``, and the target would be a local path.
This custom sync command would be also be used in node failures, where the source argument would be the path to the trial directory and the target would be a remote path. The `sync_to_driver` would be invoked to push a checkpoint to new node for a queued trial to resume.

If an upload directory is provided, Tune will automatically sync results to the given directory, natively supporting standard S3/gsutil commands.
You can customize this to specify arbitrary storages with the ``sync_to_cloud`` argument. This argument is similar to ``sync_to_cloud`` in that it supports strings with the same replacement fields and arbitrary functions. See `syncer.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/syncer.py>`__ for implementation details.

.. code-block:: python

    tune.run(
        MyTrainableClass,
        name="experiment_name",
        sync_to_cloud=custom_sync_func,
    )


Debugging
---------

By default, Tune will run hyperparameter evaluations on multiple processes. However, if you need to debug your training process, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process with ``local_mode`` by calling the following before ``tune.run``.

.. code-block:: python

    ray.init(local_mode=True)

Note that some behavior such as writing to files by depending on the current working directory in a Trainable and setting global process variables may not work as expected. Local mode with multiple configuration evaluations will interleave computation, so it is most naturally used when running a single configuration evaluation.


Further Questions or Issues?
----------------------------

You can post questions or issues or feedback through the following channels:

1. `ray-dev@googlegroups.com`_: For discussions about development or any general
   questions and feedback.
2. `StackOverflow`_: For questions about how to use Ray.
3. `GitHub Issues`_: For bug reports and feature requests.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
