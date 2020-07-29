=============================
User Guide & Configuring Tune
=============================

These pages will demonstrate the various features and configurations of Tune.


.. warning:: Before you continue, be sure to have read :ref:`tune-60-seconds`.

This document provides an overview of the core concepts as well as some of the configurations for running Tune.

.. contents:: :local:

.. _tune-parallelism:

Parallelism / GPUs
------------------

.. tip:: To run everything sequentially, use :ref:`Ray Local Mode <tune-debugging>`.

Parallelism is determined by ``resources_per_trial`` (defaulting to 1 CPU, 0 GPU per trial) and the resources available to Tune (``ray.cluster_resources()``).

Tune will allocate the specified GPU and CPU from ``resources_per_trial`` to each individual trial. A trial will not be scheduled unless at least that amount of resources is available, preventing the cluster from being overloaded.

By default, Tune automatically runs N concurrent trials, where N is the number of CPUs (cores) on your machine.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 4 concurrent trials at a time.
    tune.run(trainable, num_samples=10)

You can override this parallelism with ``resources_per_trial``:

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 2 concurrent trials at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2})

    # If you have 4 CPUs on your machine, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 4})

    # Fractional values are also supported, (i.e., {"cpu": 0.5}).
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 0.5})

To leverage GPUs, you must set ``gpu`` in ``resources_per_trial``. This will automatically set ``CUDA_VISIBLE_DEVICES`` for each trial.

.. code-block:: python

    # If you have 8 GPUs, this will run 8 trials at once.
    tune.run(trainable, num_samples=10, resources_per_trial={"gpu": 1})

    # If you have 4 CPUs on your machine and 1 GPU, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2, "gpu": 1})

You can find an example of this in the :doc:`Keras MNIST example </tune/examples/tune_mnist_keras>`.

.. warning:: If 'gpu' is not set, ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.

To attach to a Ray cluster, simply run ``ray.init`` before ``tune.run``:

.. code-block:: python

    # Connect to an existing distributed Ray cluster
    ray.init(address=<ray_address>)
    tune.run(trainable, num_samples=100, resources_per_trial={"cpu": 2, "gpu": 1})

.. _tune-default-search-space:

Search Space (Grid/Random)
--------------------------

.. warning:: If you use a Search Algorithm, you will need to use a different search space API.

You can specify a grid search or random search via the dict passed into ``tune.run(config=)``.

.. code-block:: python

    parameters = {
        "qux": tune.sample_from(lambda spec: 2 + 2),
        "bar": tune.grid_search([True, False]),
        "foo": tune.grid_search([1, 2, 3]),
        "baz": "asd",  # a constant value
    }

    tune.run(trainable, config=parameters)

By default, each random variable and grid search point is sampled once. To take multiple random samples, add ``num_samples: N`` to the experiment config. If `grid_search` is provided as an argument, the grid will be repeated `num_samples` of times.

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

Read about this in the :ref:`Grid/Random Search API <tune-grid-random>` page.

Reporting Metrics
-----------------

You can log arbitrary values and metrics in both training APIs:

.. code-block:: python

    def trainable(config):
        num_epochs = 100
        for i in range(num_epochs):
            accuracy = model.train()
            metric_1 = f(model)
            metric_2 = model.get_loss()
            tune.report(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)

    class Trainable(tune.Trainable):
        ...

        def step(self):  # this is called iteratively
            accuracy = self.model.train()
            metric_1 = f(self.model)
            metric_2 = self.model.get_loss()
            # don't call report here!
            return dict(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)

During training, Tune will automatically log the below metrics in addition to the user-provided values. All of these can be used as stopping conditions or passed as a parameter to Trial Schedulers/Search Algorithms.

.. literalinclude:: ../../../python/ray/tune/result.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

.. _tune-checkpoint:

Checkpointing
-------------

When running a hyperparameter search, Tune can automatically and periodically save/checkpoint your model. Checkpointing is used for

 * saving a model throughout training
 * fault-tolerance when using pre-emptible machines.
 * Pausing trials when using Trial Schedulers such as HyperBand and PBT.

Checkpointing assumes that the model state will be saved to disk on whichever node the Trainable is running on.

To use Tune's checkpointing features, you must expose a ``checkpoint`` argument in the function signature, and call ``tune.make_checkpoint_dir`` and ``tune.save_checkpoint``:

.. code-block:: python

        import time
        from ray import tune

        def train_func(config, checkpoint=None):
            start = 0
            if checkpoint:
                with open(checkpoint) as f:
                    state = json.loads(f.read())
                    start = state["step"] + 1

            for iter in range(start, 100):
                time.sleep(1)

                # Obtain a checkpoint directory
                checkpoint_dir = tune.make_checkpoint_dir(step=step)
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"step": start}))
                tune.save_checkpoint(path)

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

.. _tune-stopping:

Stopping Trials
---------------

You can control when trials are stopped early by passing the ``stop`` argument to ``tune.run``. This argument takes either a dictionary or a function.

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


Note that in the above example the currently running trials will not stop immediately but will do so once their current iterations are complete. See the :ref:`tune-stop-ref` documentation.

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

The following fields will automatically show up on the console output, if provided:

1. ``episode_reward_mean``
2. ``mean_loss``
3. ``mean_accuracy``
4. ``timesteps_this_iter`` (aggregated into ``timesteps_total``).

Below is an example of the console output:

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

You can use a :ref:`Reporter <tune-reporter-doc>` object to customize the console output.


Uploading Results
-----------------

If an upload directory is provided, Tune will automatically sync results from the ``local_dir`` to the given directory, natively supporting standard S3/gsutil URIs.

.. code-block:: python

    tune.run(
        MyTrainableClass,
        local_dir="~/ray_results",
        upload_dir="s3://my-log-dir"
    )

You can customize this to specify arbitrary storages with the ``sync_to_cloud`` argument in ``tune.run``. This argument supports either strings with the same replacement fields OR arbitrary functions.

.. code-block:: python

    tune.run(
        MyTrainableClass,
        upload_dir="s3://my-log-dir",
        sync_to_cloud=custom_sync_str_or_func,
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

By default, syncing occurs every 300 seconds. To change the frequency of syncing, set the ``TUNE_CLOUD_SYNC_S`` environment variable in the driver to the desired syncing period. Note that uploading only happens when global experiment state is collected, and the frequency of this is determined by the ``global_checkpoint_period`` argument. So the true upload period is given by ``max(TUNE_CLOUD_SYNC_S, global_checkpoint_period)``.

.. _tune-debugging:

Debugging
---------

By default, Tune will run hyperparameter evaluations on multiple processes. However, if you need to debug your training process, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process with ``local_mode`` by calling the following before ``tune.run``.

.. code-block:: python

    ray.init(local_mode=True)

Local mode with multiple configuration evaluations will interleave computation, so it is most naturally used when running a single configuration evaluation.

Stopping after the first failure
--------------------------------

By default, ``tune.run`` will continue executing until all trials have terminated or errored. To stop the entire Tune run as soon as **any** trial errors:

.. code-block:: python

    tune.run(trainable, fail_fast=True)

This is useful when you are trying to setup a large hyperparameter experiment.


Further Questions or Issues?
----------------------------

You can post questions or issues or feedback through the following channels:

1. `StackOverflow`_: For questions about how to use Ray.
2. `GitHub Issues`_: For bug reports and feature requests.

.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues