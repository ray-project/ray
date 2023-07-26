.. _train-dl-guide:


Configuring Training
--------------------

With Ray Train, you can execute a training function (``train_func``) in a
distributed manner by calling ``Trainer.fit``. To pass arguments
into the training function, you can expose a single ``config`` dictionary parameter:

.. code-block:: diff

    -def train_func():
    +def train_func(config):

Then, you can pass in the config dictionary as an argument to ``Trainer``:

.. code-block:: diff

    +config = {} # This should be populated.
     trainer = TorchTrainer(
         train_func,
    +    train_loop_config=config,
         scaling_config=ScalingConfig(num_workers=2)
     )

Putting this all together, you can run your training function with different
configurations. As an example:

.. code-block:: python

    from ray.air import session, ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        for i in range(config["num_epochs"]):
            session.report({"epoch": i})

    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 2},
        scaling_config=ScalingConfig(num_workers=2)
    )
    result = trainer.fit()
    print(result.metrics["num_epochs"])
    # 1

A primary use-case for ``config`` is to try different hyperparameters. To
perform hyperparameter tuning with Ray Train, please refer to the
:ref:`Ray Tune integration <train-tune>`.

.. TODO add support for with_parameters

.. _train-result-object:

Accessing Training Results
--------------------------

.. TODO(ml-team) Flesh this section out.

The return of a ``Trainer.fit`` is a :py:class:`~ray.air.result.Result` object, containing
information about the training run. You can access it to obtain saved checkpoints,
metrics and other relevant data.

For example, you can:

* Print the metrics for the last training iteration:

.. code-block:: python

    from pprint import pprint

    pprint(result.metrics)
    # {'_time_this_iter_s': 0.001016855239868164,
    #  '_timestamp': 1657829125,
    #  '_training_iteration': 2,
    #  'config': {},
    #  'date': '2022-07-14_20-05-25',
    #  'done': True,
    #  'episodes_total': None,
    #  'epoch': 1,
    #  'experiment_id': '5a3f8b9bf875437881a8ddc7e4dd3340',
    #  'experiment_tag': '0',
    #  'hostname': 'ip-172-31-43-110',
    #  'iterations_since_restore': 2,
    #  'node_ip': '172.31.43.110',
    #  'pid': 654068,
    #  'time_since_restore': 3.4353830814361572,
    #  'time_this_iter_s': 0.00809168815612793,
    #  'time_total_s': 3.4353830814361572,
    #  'timestamp': 1657829125,
    #  'timesteps_since_restore': 0,
    #  'timesteps_total': None,
    #  'training_iteration': 2,
    #  'trial_id': '4913f_00000',
    #  'warmup_time': 0.003167867660522461}

* View the dataframe containing the metrics from all iterations:

.. code-block:: python

    print(result.metrics_dataframe)

* Obtain the :py:class:`~ray.air.checkpoint.Checkpoint`, used for resuming training, prediction and serving.

.. code-block:: python

    result.checkpoint  # last saved checkpoint
    result.best_checkpoints  # N best saved checkpoints, as configured in run_config
    result.error  # returns the Exception if training failed.


See :class:`the Result docstring <ray.air.result.Result>` for more details.

.. _train-log-dir:

Log Directory Structure
~~~~~~~~~~~~~~~~~~~~~~~

Each ``Trainer`` will have a local directory created for logs and checkpoints.

You can obtain the path to the directory by accessing the ``log_dir`` attribute
of the :py:class:`~ray.air.result.Result` object returned by ``Trainer.fit()``.

.. code-block:: python

    print(result.log_dir)
    # '/home/ubuntu/ray_results/TorchTrainer_2022-06-13_20-31-06/checkpoint_000003'

.. _train-datasets:

Distributed Data Ingest with Ray Data and Ray Train
-------------------------------------------------------

:ref:`Ray Data <data>` is the recommended way to work with large datasets in Ray Train. Ray Data provides automatic loading, sharding, and streamed ingest of Data across multiple Train workers.
To get started, pass in one or more datasets under the ``datasets`` keyword argument for Trainer (e.g., ``Trainer(datasets={...})``).

Here's a simple code overview of the Ray Data integration:

.. code-block:: python

    from ray.air import session

    # Datasets can be accessed in your train_func via ``get_dataset_shard``.
    def train_func(config):
        train_data_shard = session.get_dataset_shard("train")
        validation_data_shard = session.get_dataset_shard("validation")
        ...

    # Random split the dataset into 80% training data and 20% validation data.
    dataset = ray.data.read_csv("...")
    train_dataset, validation_dataset = dataset.train_test_split(
        test_size=0.2, shuffle=True,
    )

    trainer = TorchTrainer(
        train_func,
        datasets={"train": train_dataset, "validation": validation_dataset},
        scaling_config=ScalingConfig(num_workers=8),
    )
    trainer.fit()

For more details on how to configure data ingest for Train, please refer to :ref:`air-ingest`.

.. _train-monitoring:

Logging, Checkpointing and Callbacks in Ray Train
-------------------------------------------------

Ray Train has mechanisms to easily collect intermediate results from the training workers during the training run
and also has a :ref:`Callback interface <train-callbacks>` to perform actions on these intermediate results (such as logging, aggregations, etc.).
You can use either the :ref:`built-in callbacks <air-builtin-callbacks>` that Ray AIR provides,
or implement a :ref:`custom callback <train-custom-callbacks>` for your use case. The callback API
is shared with Ray Tune.

.. _train-checkpointing:

Ray Train also provides a way to save :ref:`Checkpoints <checkpoint-api-ref>` during the training process. This is
useful for:

1. :ref:`Integration with Ray Tune <train-tune>` to use certain Ray Tune
   schedulers.
2. Running a long-running training job on a cluster of pre-emptible machines/pods.
3. Persisting trained model state to later use for serving/inference.
4. In general, storing any model artifacts.

Reporting intermediate results and handling checkpoints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray AIR provides a *Session* API for reporting intermediate
results and checkpoints from the training function (run on distributed workers) up to the
``Trainer`` (where your python script is executed) by calling ``session.report(metrics)``.
The results will be collected from the distributed workers and passed to the driver to
be logged and displayed.

.. warning::

    Only the results from rank 0 worker will be used. However, in order to ensure
    consistency, ``session.report()`` has to be called on each worker. If you
    want to aggregate results from multiple workers, see :ref:`train-aggregating-results`.

The primary use-case for reporting is for metrics (accuracy, loss, etc.) at
the end of each training epoch.

.. code-block:: python

    from ray.air import session

    def train_func():
        ...
        for i in range(num_epochs):
            result = model.train(...)
            session.report({"result": result})

The session concept exists on several levels: The execution layer (called `Tune Session`) and the Data Parallel training layer
(called `Train Session`).
The following figure shows how these two sessions look like in a Data Parallel training scenario.

.. image:: ../ray-air/images/session.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1g0pv8gqgG29aPEPTcd4BC0LaRNbW1sAkv3H6W1TCp0c/edit

