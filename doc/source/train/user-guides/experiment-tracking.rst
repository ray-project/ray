.. _train-experiment-tracking-native:

===================
Experiment Tracking
===================

Most experiment tracking libraries work out-of-the-box with Ray Train.
This guide provides instructions on how to set up the code so that your favorite experiment tracking libraries
can work for distributed training with Ray Train. The end of the guide has common errors to aid in debugging
the setup.

The following pseudo code demonstrates how to use the native experiment tracking library calls
inside of Ray Train:

.. testcode::
    :skipif: True

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func():
        # Training code and native experiment tracking library calls go here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

Ray Train lets you use native experiment tracking libraries by customizing the tracking
logic inside the :ref:`train_func<train-overview-training-function>` function.
In this way, you can port your experiment tracking logic to Ray Train with minimal changes.

Getting Started
===============

Let's start by looking at some code snippets.

The following examples uses Weights & Biases (W&B) and MLflow but it's adaptable to other frameworks.

.. tab-set::

    .. tab-item:: W&B

        .. testcode::
            :skipif: True

            import ray
            from ray import train
            import wandb

            # Step 1
            # This ensures that all ray worker processes have `WANDB_API_KEY` set.
            ray.init(runtime_env={"env_vars": {"WANDB_API_KEY": "your_api_key"}})

            def train_func():
                # Step 1 and 2
                if train.get_context().get_world_rank() == 0:
                    wandb.init(
                        name=...,
                        project=...,
                        # ...
                    )

                # ...
                loss = optimize()
                metrics = {"loss": loss}

                # Step 3
                if train.get_context().get_world_rank() == 0:
                    wandb.log(metrics)

                # ...

                # Step 4
                # Make sure that all loggings are uploaded to the W&B backend.
                if train.get_context().get_world_rank() == 0:
                    wandb.finish()

    .. tab-item:: MLflow

        .. testcode::
            :skipif: True

            from ray import train
            import mlflow

            # Run the following on the head node:
            # $ databricks configure --token
            # mv ~/.databrickscfg YOUR_SHARED_STORAGE_PATH
            # This function assumes `databricks_config_file` is specified in the Trainer's `train_loop_config`.
            def train_func(config):
                # Step 1 and 2
                os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
                mlflow.set_tracking_uri("databricks")
                mlflow.set_experiment_id(...)
                mlflow.start_run()

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the results from the first worker to MLflow
                to avoid duplication

                # Step 3
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

.. tip::

    A major difference between distributed and non-distributed training is that in distributed training,
    multiple processes are running in parallel and under certain setups they have the same results. If all
    of them report results to the tracking backend, you may get duplicated results. To address that,
    Ray Train lets you apply logging logic to only the rank 0 worker with the following method:
    :meth:`ray.train.get_context().get_world_rank() <ray.train.context.TrainContext.get_world_rank>`.

    .. testcode::
        :skipif: True

        from ray import train
        def train_func():
            ...
            if train.get_context().get_world_rank() == 0:
                # Add your logging logic only for rank0 worker.
            ...

The interaction with the experiment tracking backend within the :ref:`train_func<train-overview-training-function>`
has 4 logical steps:

#. Set up the connection to a tracking backend
#. Configure and launch a run
#. Log metrics
#. Finish the run

More details about each step follows.

Step 1: Connect to your tracking backend
----------------------------------------

First, decide which tracking backend to use: W&B, MLflow, TensorBoard, Comet, etc.
If applicable, make sure that you properly set up credentials on each training worker.

.. tab-set::

    .. tab-item:: W&B

        W&B offers both *online* and *offline* modes.

        **Online**

        For *online* mode, because you log to W&B's tracking service, ensure that you set the credentials
        inside of :ref:`train_func<train-overview-training-function>`. See :ref:`Set up credentials<set-up-credentials>`
        for more information.

        .. testcode::
            :skipif: True

            # This is equivalent to `os.environ["WANDB_API_KEY"] = "your_api_key"`
            wandb.login(key="your_api_key")

        **Offline**

        For *offline* mode, because you log towards a local file system,
        point the offline directory to a shared storage path that all nodes can write to.
        See :ref:`Set up a shared file system<set-up-shared-file-system>` for more information.

        .. testcode::
            :skipif: True

            os.environ["WANDB_MODE"] = "offline"
            wandb.init(dir="some_shared_storage_path/wandb")

    .. tab-item:: MLflow

        MLflow offers both *local* and *remote* (for example, to Databrick's MLflow service) modes.

        **Local**

        For *local* mode, because you log to a local file
        system, point offline directory to a shared storage path. that all nodes can write
        to. See :ref:`Set up a shared file system<set-up-shared-file-system>` for more information.

        .. testcode::
            :skipif: True

            mlflow.start_run(tracking_uri="file:some_shared_storage_path/mlruns")

        **Remote, hosted by Databricks**

        Ensure that all nodes have access to the Databricks config file.
        See :ref:`Set up credentials<set-up-credentials>` for more information.

        .. testcode::
            :skipif: True

            # The MLflow client looks for a Databricks config file
            # at the location specified by `os.environ["DATABRICKS_CONFIG_FILE"]`.
            os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
            mlflow.set_tracking_uri("databricks")
            mlflow.start_run()

.. _set-up-credentials:

Set up credentials
~~~~~~~~~~~~~~~~~~

Refer to each tracking library's API documentation on setting up credentials.
This step usually involves setting an environment variable or accessing a config file.

The easiest way to pass an environment variable credential to training workers is through
:ref:`runtime environments <runtime-environments>`, where you initialize with the following code:

.. testcode::
    :skipif: True

    import ray
    # This makes sure that training workers have the same env var set
    ray.init(runtime_env={"env_vars": {"SOME_API_KEY": "your_api_key"}})

For accessing the config file, ensure that the config file is accessible to all nodes.
One way to do this is by setting up a shared storage. Another way is to save a copy in each node.

.. _set-up-shared-file-system:

Set up a shared file system
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set up a network filesystem accessible to all nodes in the cluster.
For example, AWS EFS or Google Cloud Filestore.

Step 2: Configure and start the run
-----------------------------------

This step usually involves picking an identifier for the run and associating it with a project.
Refer to the tracking libraries' documentation for semantics.

.. To conveniently link back to Ray Train run, you may want to log the persistent storage path
.. of the run as a config.

..
    .. testcode::

       def train_func():
            if ray.train.get_context().get_world_rank() == 0:
                   wandb.init(..., config={"ray_train_persistent_storage_path": "TODO: fill in when API stablizes"})

.. tip::

    When performing **fault-tolerant training** with auto-restoration, use a
    consistent ID to configure all tracking runs that logically belong to the same training run.


Step 3: Log metrics
-------------------

You can customize how to log parameters, metrics, models, or media contents, within
:ref:`train_func<train-overview-training-function>`, just as in a non-distributed training script.
You can also use native integrations that a particular tracking framework has with
specific training frameworks. For example, ``mlflow.pytorch.autolog()``,
``lightning.pytorch.loggers.MLFlowLogger``, etc.

Step 4: Finish the run
----------------------

This step ensures that all logs are synced to the tracking service. Depending on the implementation of
various tracking libraries, sometimes logs are first cached locally and only synced to the tracking
service in an asynchronous fashion.
Finishing the run makes sure that all logs are synced by the time training workers exit.

.. tab-set::

    .. tab-item:: W&B

        .. testcode::
            :skipif: True

            # https://docs.wandb.ai/ref/python/finish
            wandb.finish()

    .. tab-item:: MLflow

        .. testcode::
            :skipif: True

            # https://mlflow.org/docs/1.2.0/python_api/mlflow.html
            mlflow.end_run()

    .. tab-item:: Comet

        .. testcode::
            :skipif: True

            # https://www.comet.com/docs/v2/api-and-sdk/python-sdk/reference/Experiment/#experimentend
            Experiment.end()

Examples
========

The following are runnable examples for PyTorch and PyTorch Lightning.

PyTorch
-------

.. dropdown:: Log to W&B

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking//torch_exp_tracking_wandb.py
            :emphasize-lines: 15, 16, 17, 21, 22, 51, 52, 54, 55
            :language: python
            :start-after: __start__

.. dropdown:: Log to file-based MLflow

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/torch_exp_tracking_mlflow.py
        :emphasize-lines: 22, 23, 24, 25, 54, 55, 57, 58, 64
        :language: python
        :start-after: __start__
        :end-before: __end__

PyTorch Lightning
-----------------

You can use the native Logger integration in PyTorch Lightning with W&B, CometML, MLFlow,
and Tensorboard, while using Ray Train's TorchTrainer.

The following example walks you through the process. The code here is runnable.

.. dropdown:: W&B

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
        :language: python
        :start-after: __model_dl_start__

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_wandb.py
        :language: python
        :start-after: __lightning_experiment_tracking_wandb_start__

.. dropdown:: MLflow

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
        :language: python
        :start-after: __model_dl_start__

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_mlflow.py
        :language: python
        :start-after: __lightning_experiment_tracking_mlflow_start__
        :end-before: __lightning_experiment_tracking_mlflow_end__

.. dropdown:: Comet

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
        :language: python
        :start-after: __model_dl_start__

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_comet.py
        :language: python
        :start-after: __lightning_experiment_tracking_comet_start__

.. dropdown:: TensorBoard

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
        :language: python
        :start-after: __model_dl_start__

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_tensorboard.py
        :language: python
        :start-after: __lightning_experiment_tracking_tensorboard_start__
        :end-before: __lightning_experiment_tracking_tensorboard_end__

Common Errors
=============

Missing Credentials
-------------------

**I have already called `wandb login` cli, but am still getting**

.. code-block:: none

    wandb: ERROR api_key not configured (no-tty). call wandb.login(key=[your_api_key]).

This is probably due to wandb credentials are not set up correctly
on worker nodes. Make sure that you run ``wandb.login``
or pass ``WANDB_API_KEY`` to each training function.
See :ref:`Set up credentials <set-up-credentials>` for more details.

Missing Configurations
----------------------

**I have already run `databricks configure`, but am still getting**

.. code-block:: none

    databricks_cli.utils.InvalidConfigurationError: You haven't configured the CLI yet!

This is usually caused by running ``databricks configure`` which
generates ``~/.databrickscfg`` only on head node. Move this file to a shared
location or copy it to each node.
See :ref:`Set up credentials <set-up-credentials>` for more details.
