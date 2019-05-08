Tune User Guide
===============

Tune Overview
-------------

.. image:: images/tune-api.svg

Tune schedules a number of *trials* in a cluster. Each trial runs a user-defined Python function or class and is parameterized either by a *config* variation from Tune's Variant Generator or a user-specified **search algorithm**. The trials are scheduled and managed by a **trial scheduler**.

More information about Tune's `search algorithms can be found here <tune-searchalg.html>`__. More information about Tune's `trial schedulers can be found here <tune-schedulers.html>`__.

Start by installing, importing, and initializing Ray.

.. code-block:: python

    import ray
    import ray.tune as tune

    ray.init()


Experiment Configuration
------------------------

This section will cover the main steps needed to modify your code to run Tune: using the `Training API <tune-usage.html#training-api>`__ and  `executing your Tune experiment <tune-usage.html#specifying-experiments>`__.

You can checkout out our `examples page <tune-examples.html>`__ for more code examples.

Training API
~~~~~~~~~~~~

Training can be done with either the **function-based API** or **Trainable API**.

**Python functions** will need to have the following signature:

.. code-block:: python

    def trainable(config, reporter):
        """
        Args:
            config (dict): Parameters provided from the search algorithm
                or variant generation.
            reporter (Reporter): Handle to report intermediate metrics to Tune.
        """

        while True:
            # ...
            reporter(**kwargs)

The reporter will allow you to report metrics used for scheduling, search, or early stopping.

Tune will run this function on a separate thread in a Ray actor process. Note that this API is not checkpointable, since the thread will never return control back to its caller. The reporter documentation can be `found here <tune-package-ref.html#ray.tune.function_runner.StatusReporter>`__.

.. note::
    If you have a lambda function that you want to train, you will need to first register the function: ``tune.register_trainable("lambda_id", lambda x: ...)``. You can then use ``lambda_id`` in place of ``my_trainable``.

**Python classes** passed into Tune will need to subclass ``ray.tune.Trainable``. The Trainable interface `can be found here <tune-package-ref.html#ray.tune.Trainable>`__.

Both the Trainable and function-based API will have `autofilled metrics <tune-usage.html#auto-filled-results>`__ in addition to the metrics reported.

See the `experiment specification <tune-usage.html#specifying-experiments>`__ section on how to specify and execute your training.


Launching an Experiment
~~~~~~~~~~~~~~~~~~~~~~~

Tune provides a ``run`` function that generates and runs the trials.

.. autofunction:: ray.tune.run
    :noindex:

This function will report status on the command line until all Trials stop:

::

    == Status ==
    Using FIFO scheduling algorithm.
    Resources used: 4/8 CPUs, 0/0 GPUs
    Result logdir: ~/ray_results/my_experiment
     - train_func_0_lr=0.2,momentum=1:  RUNNING [pid=6778], 209 s, 20604 ts, 7.29 acc
     - train_func_1_lr=0.4,momentum=1:  RUNNING [pid=6780], 208 s, 20522 ts, 53.1 acc
     - train_func_2_lr=0.6,momentum=1:  TERMINATED [pid=6789], 21 s, 2190 ts, 100 acc
     - train_func_3_lr=0.2,momentum=2:  RUNNING [pid=6791], 208 s, 41004 ts, 8.37 acc
     - train_func_4_lr=0.4,momentum=2:  RUNNING [pid=6800], 209 s, 41204 ts, 70.1 acc
     - train_func_5_lr=0.6,momentum=2:  TERMINATED [pid=6809], 10 s, 2164 ts, 100 acc


Custom Trial Names
~~~~~~~~~~~~~~~~~~

To specify custom trial names, you can pass use the ``trial_name_creator`` argument
to `tune.run`.  This takes a function with the following signature, and
be sure to wrap it with `tune.function`:

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
        name="hyperband_test",
        num_samples=1,
        trial_name_creator=tune.function(trial_name_string)
    )

An example can be found in `logging_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py>`__.


Training Features
-----------------

Tune Search Space (Default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~


You can use ``tune.grid_search`` to specify an axis of a grid search. By default, Tune also supports sampling parameters from user-specified lambda functions, which can be used independently or in combination with grid search.

.. note::
    If you specify an explicit Search Algorithm such as any SuggestionAlgorithm, you may not be able to specify lambdas or grid search with this interface, as the search algorithm may require a different search space declaration.

The following shows grid search over two nested parameters combined with random sampling from two lambda functions, generating 9 different trials. Note that the value of ``beta`` depends on the value of ``alpha``, which is represented by referencing ``spec.config.alpha`` in the lambda function. This lets you specify conditional parameter distributions.

.. code-block:: python
   :emphasize-lines: 4-11

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
        }
    )


.. note::
    Use ``tune.sample_from(...)`` to sample from a function during trial variant generation. If you need to pass a literal function in your config, use ``tune.function(...)`` to escape it.

For more information on variant generation, see `basic_variant.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/suggest/basic_variant.py>`__.

Sampling Multiple Times
~~~~~~~~~~~~~~~~~~~~~~~

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


Using GPUs (Resource Allocation)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tune will allocate the specified GPU and CPU ``resources_per_trial`` to each individual trial (defaulting to 1 CPU per trial). Under the hood, Tune runs each trial as a Ray actor, using Ray's resource handling to allocate resources and place actors. A trial will not be scheduled unless at least that amount of resources is available in the cluster, preventing the cluster from being overloaded.

Fractional values are also supported, (i.e., ``"gpu": 0.2``). You can find an example of this in the `Keras MNIST example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_keras.py>`__.

If GPU resources are not requested, the ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.
Otherwise, it will be set to the GPUs in the list (this is managed by Ray).


If your trainable function / class creates further Ray actors or tasks that also consume CPU / GPU resources, you will also want to set ``extra_cpu`` or ``extra_gpu`` to reserve extra resource slots for the actors you will create. For example, if a trainable class requires 1 GPU itself, but will launch 4 actors each using another GPU, then it should set ``"gpu": 1, "extra_gpu": 4``.

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


Trial Checkpointing
~~~~~~~~~~~~~~~~~~~

To enable checkpointing, you must implement a `Trainable class <tune-usage.html#training-api>`__ (Trainable functions are not checkpointable, since they never return control back to their caller). The easiest way to do this is to subclass the pre-defined ``Trainable`` class and implement its ``_train``, ``_save``, and ``_restore`` abstract methods `(example) <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__. Implementing this interface is required to support resource multiplexing in  Trial Schedulers such as HyperBand and PBT.

For TensorFlow model training, this would look something like this `(full tensorflow example) <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_ray_hyperband.py>`__:

.. code-block:: python

    class MyClass(Trainable):
        def _setup(self, config):
            self.saver = tf.train.Saver()
            self.sess = ...
            self.iteration = 0

        def _train(self):
            self.sess.run(...)
            self.iteration += 1

        def _save(self, checkpoint_dir):
            return self.saver.save(
                self.sess, checkpoint_dir + "/save",
                global_step=self.iteration)

        def _restore(self, path):
            return self.saver.restore(self.sess, path)


Additionally, checkpointing can be used to provide fault-tolerance for experiments. This can be enabled by setting ``checkpoint_freq=N`` and ``max_failures=M`` to checkpoint trials every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python
   :emphasize-lines: 4,5

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        max_failures=5,
    )

The checkpoint_freq may not coincide with the exact end of an experiment. If you want a checkpoint to be created at the end
of a trial, you can additionally set the checkpoint_at_end to True. An example is shown below:

.. code-block:: python
   :emphasize-lines: 5

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        checkpoint_at_end=True,
        max_failures=5,
    )


Recovering From Failures (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tune automatically persists the progress of your experiments, so if an experiment crashes or is otherwise cancelled, it can be resumed with ``resume=True``. The default setting of ``resume=False`` creates a new experiment, and ``resume="prompt"`` will cause Tune to prompt you for whether you want to resume. You can always force a new experiment to be created by changing the experiment name.

Note that trials will be restored to their last checkpoint. If trial checkpointing is not enabled, unfinished trials will be restarted from scratch.

E.g.:

.. code-block:: python

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        local_dir="~/path/to/results",
        resume=True
    )


Upon a second run, this will restore the entire experiment state from ``~/path/to/results/my_experiment_name``. Importantly, any changes to the experiment specification upon resume will be ignored.

This feature is still experimental, so any provided Trial Scheduler or Search Algorithm will not be preserved. Only ``FIFOScheduler`` and ``BasicVariantGenerator`` will be supported.


Handling Large Datasets
-----------------------

You often will want to compute a large object (e.g., training data, model weights) on the driver and use that object within each trial. Tune provides a ``pin_in_object_store`` utility function that can be used to broadcast such large objects. Objects pinned in this way will never be evicted from the Ray object store while the driver process is running, and can be efficiently retrieved from any task via ``get_pinned_object``.

.. code-block:: python

    import ray
    from ray import tune
    from ray.tune.util import pin_in_object_store, get_pinned_object

    import numpy as np

    ray.init()

    # X_id can be referenced in closures
    X_id = pin_in_object_store(np.random.random(size=100000000))

    def f(config, reporter):
        X = get_pinned_object(X_id)
        # use X

    tune.run(f)


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

.. code-block:: bash

    Example_0:  TERMINATED [pid=68248], 179 s, 2 iter, 60000 ts, 94 rew


Logging, Analyzing, and Visualizing Results
-------------------------------------------

All results reported by the trainable will be logged locally to a unique directory per experiment, e.g. ``~/ray_results/my_experiment`` in the above example. On a cluster, incremental results will be synced to local disk on the head node.

Tune provides an ``ExperimentAnalysis`` object for analyzing results which can be used by providing the directory path as follows:

.. code-block:: python

    from ray.tune.analysis import ExperimentAnalysis

    ea = ExperimentAnalysis("~/ray_results/my_experiment")
    trials_dataframe = ea.dataframe()

You can check out `experiment_analysis.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/analysis/experiment_analysis.py>`__ for more interesting analysis operations.

To visualize learning in tensorboard, install TensorFlow:

.. code-block:: bash

    $ pip install tensorflow

Then, after you run a experiment, you can visualize your experiment with TensorBoard by specifying the output directory of your results. Note that if you running Ray on a remote cluster, you can forward the tensorboard port to your local machine through SSH using ``ssh -L 6006:localhost:6006 <address>``:

.. code-block:: bash

    $ tensorboard --logdir=~/ray_results/my_experiment

.. image:: ray-tune-tensorboard.png

To use rllab's VisKit (you may have to install some dependencies), run:

.. code-block:: bash

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

.. image:: ray-tune-viskit.png


Custom Loggers
~~~~~~~~~~~~~~

You can pass in your own logging mechanisms to output logs in custom formats as follows:

.. code-block:: python

    from ray.tune.logger import DEFAULT_LOGGERS

    tune.run(
        MyTrainableClass
        name="experiment_name",
        loggers=DEFAULT_LOGGERS + (CustomLogger1, CustomLogger2)
    )

These loggers will be called along with the default Tune loggers. All loggers must inherit the `Logger interface <tune-package-ref.html#ray.tune.logger.Logger>`__.

Tune has default loggers for Tensorboard, CSV, and JSON formats.

You can also check out `logger.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/logger.py>`__ for implementation details.

An example can be found in `logging_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py>`__.

Custom Sync/Upload Commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If an upload directory is provided, Tune will automatically sync results to the given
directory with standard S3/gsutil commands. You can customize the upload command by
providing either a function or a string.

If a string is provided, then it must include replacement fields ``{local_dir}`` and
``{remote_dir}``, like ``"aws s3 sync {local_dir} {remote_dir}"``.

Alternatively, a function can be provided with the following signature (and must
be wrapped with ``tune.function``):

.. code-block:: python

    def custom_sync_func(local_dir, remote_dir):
        sync_cmd = "aws s3 sync {local_dir} {remote_dir}".format(
            local_dir=local_dir,
            remote_dir=remote_dir)
        sync_process = subprocess.Popen(sync_cmd, shell=True)
        sync_process.wait()

    tune.run(
        MyTrainableClass,
        name="experiment_name",
        sync_function=tune.function(custom_sync_func)
    )


Tune Client API
---------------

You can interact with an ongoing experiment with the Tune Client API. The Tune Client API is organized around REST, which includes resource-oriented URLs, accepts form-encoded requests, returns JSON-encoded responses, and uses standard HTTP protocol.

To allow Tune to receive and respond to your API calls, you have to start your experiment with ``with_server=True``:

.. code-block:: python

    tune.run(..., with_server=True, server_port=4321)

The easiest way to use the Tune Client API is with the built-in TuneClient. To use TuneClient, verify that you have the ``requests`` library installed:

.. code-block:: bash

    $ pip install requests

Then, on the client side, you can use the following class. If on a cluster, you may want to forward this port (e.g. ``ssh -L <local_port>:localhost:<remote_port> <address>``) so that you can use the Client on your local machine.

.. autoclass:: ray.tune.web_server.TuneClient
    :members:

For an example notebook for using the Client API, see the `Client API Example <https://github.com/ray-project/ray/tree/master/python/ray/tune/TuneClient.ipynb>`__.

The API also supports curl. Here are the examples for getting trials (``GET /trials/[:id]``):

.. code-block:: bash

    curl http://<address>:<port>/trials
    curl http://<address>:<port>/trials/<trial_id>

And stopping a trial (``PUT /trials/:id``):

.. code-block:: bash

    curl -X PUT http://<address>:<port>/trials/<trial_id>


Tune CLI (Experimental)
-----------------------

``tune`` has an easy-to-use command line interface (CLI) to manage and monitor your experiments on Ray. To do this, verify that you have the ``tabulate`` library installed:

.. code-block:: bash

    $ pip install tabulate

Here are a few examples of command line calls.

- ``tune list-trials``: List tabular information about trials within an experiment. Empty columns will be dropped by default. Add the ``--sort`` flag to sort the output by specific columns. Add the ``--filter`` flag to filter the output in the format ``"<column> <operator> <value>"``. Add the ``--output`` flag to write the trial information to a specific file (CSV or Pickle). Add the ``--columns`` and ``--result-columns`` flags to select specific columns to display.

.. code-block:: bash

    $ tune list-trials [EXPERIMENT_DIR] --output note.csv

    +------------------+-----------------------+------------+
    | trainable_name   | experiment_tag        | trial_id   |
    |------------------+-----------------------+------------|
    | MyTrainableClass | 0_height=40,width=37  | 87b54a1d   |
    | MyTrainableClass | 1_height=21,width=70  | 23b89036   |
    | MyTrainableClass | 2_height=99,width=90  | 518dbe95   |
    | MyTrainableClass | 3_height=54,width=21  | 7b99a28a   |
    | MyTrainableClass | 4_height=90,width=69  | ae4e02fb   |
    +------------------+-----------------------+------------+
    Dropped columns: ['status', 'last_update_time']
    Please increase your terminal size to view remaining columns.
    Output saved at: note.csv

    $ tune list-trials [EXPERIMENT_DIR] --filter "trial_id == 7b99a28a"

    +------------------+-----------------------+------------+
    | trainable_name   | experiment_tag        | trial_id   |
    |------------------+-----------------------+------------|
    | MyTrainableClass | 3_height=54,width=21  | 7b99a28a   |
    +------------------+-----------------------+------------+
    Dropped columns: ['status', 'last_update_time']
    Please increase your terminal size to view remaining columns.

- ``tune list-experiments``: List tabular information about experiments within a project. Empty columns will be dropped by default. Add the ``--sort`` flag to sort the output by specific columns. Add the ``--filter`` flag to filter the output in the format ``"<column> <operator> <value>"``. Add the ``--output`` flag to write the trial information to a specific file (CSV or Pickle). Add the ``--columns`` flag to select specific columns to display.

.. code-block:: bash

    $ tune list-experiments [PROJECT_DIR] --output note.csv

    +----------------------+----------------+------------------+---------------------+
    | name                 |   total_trials |   running_trials |   terminated_trials |
    |----------------------+----------------+------------------+---------------------|
    | pbt_test             |             10 |                0 |                   0 |
    | test                 |              1 |                0 |                   0 |
    | hyperband_test       |              1 |                0 |                   1 |
    +----------------------+----------------+------------------+---------------------+
    Dropped columns: ['error_trials', 'last_updated']
    Please increase your terminal size to view remaining columns.
    Output saved at: note.csv

    $ tune list-experiments [PROJECT_DIR] --filter "total_trials <= 1" --sort name

    +----------------------+----------------+------------------+---------------------+
    | name                 |   total_trials |   running_trials |   terminated_trials |
    |----------------------+----------------+------------------+---------------------|
    | hyperband_test       |              1 |                0 |                   1 |
    | test                 |              1 |                0 |                   0 |
    +----------------------+----------------+------------------+---------------------+
    Dropped columns: ['error_trials', 'last_updated']
    Please increase your terminal size to view remaining columns.


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
