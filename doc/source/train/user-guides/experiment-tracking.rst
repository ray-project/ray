.. _train-experiment-tracking-native:

===================
Experiment Tracking
===================

.. note::
    This guide is relevant for all DataParallelTrainer, including TorchTrainer and TensorflowTrainer.

Most experiment tracking libraries work out-of-the-box with Ray Train. 
This guide provides instructions on how to set up the code so that your favorite experiment tracking libraries 
can work for distributed data parallel training with Ray Train. We will conclude the session with debugging
tips.

Before we begin, the following is roughly how you can use the native experiment tracking lirary calls 
inside of Ray Train. 

.. code-block:: python

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Training code and native lexperiment tracking library calls go here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

Ray Train lets you use native experiment tracking libraries by customizing the tracking 
logic inside the ``train_func`` function. In this way, you can port your experiment tracking 
logic to Ray Train with minimal changes. 

Log distributed training experiments
====================================

In this section, we break the interaction with experiment tracking backend into 4 logical steps:
- Set up to connect to a tracking backend
- Configure and launch a run
- Log
- Finish the run

Let's dive into each one of them.

.. note::

    For some scenarios, every Worker generates an identical copy and saving a single copy is sufficient. 
    Ray Train lets you apply logic to only the rank 0 worker with the following method:
    :meth:`context.get_world_rank() <ray.train.context.TrainContext.get_world_rank>`.

Step 1: Set up necessary components to be able to connect to the tracking backend of your choice
------------------------------------------------------------------------------------------------

First, you should choose which tracking backend to use: W&B, MLflow, TensorBoard etc.

Some of them offer to operate under either online or offline mode. They have different considerations when
setting up.
For online mode, you log towards a tracking service that is running. Usually you need credentials to access the service.
Under this mode, you need to ensure that all nodes and worker processes have access to credentials.
For offline mode, you log towards local file directory. Usually no credentials are needed. You need to instead
ensure that there is a shared file system where all nodes can write to.

.. tabs::

    .. tab:: Wandb

        .. tabs::

            .. tab:: online mode

                Make sure that :code:`wandb.login(key="your_api_key")` 
                or :code:`os.environ["WANDB_API_KEY"] = "your_api_key"` is called inside your ``train_func``.

            .. tab:: offline mode

                Make sure that :code:`os.environ["WANDB_MODE"] = "offline"` is set in ``train_func``.

                Set Wandb directory to point to a shared storage path: :code:`wandb.init(dir="some_shared_storage_path/wandb")` 

    .. tab:: MLflow

        .. tabs::

            .. tab:: online mode (hosted by Databricks)
                
                Start the run with :code:`mlflow.start_run(tracking_uri="databricks")`

                Make sure that all nodes have access to ``databrickscfg`` file.

            .. tab:: offline mode

                Start the run by setting tracking uri to a shared storage path: 
                :code:`mlflow.start_run(tracking_uri="file:some_shared_storage_path/mlruns")`

    .. tab:: TensorBoard (offline)
        
        Set up ``SummaryWriter`` to write to a shared storage path: :code:`writer = SummaryWriter("some_shared_storage_path/runs")`

Step 2: Initialize the run 
--------------------------

Ray Train provides a training context that provides access to training identifiers. For example, 

* Training ID (:meth:`context.get_trial_id() <ray.train.context.TrainContext.get_trial_id>`) 
* Training Name (:meth:`context.get_trial_name() <ray.train.context.TrainContext.get_trial_name>`)

.. tip::
    
    When performing **fault-tolerant training** with auto-restoration, be sure 
    to specify a unique ID for the Loggers, so that the new workers report to
    the same run after restoration.

    For example:
    
    - `WandbLogger(id=UNIQUE_ID)`
    - `CometLogger(experiment_key=UNIQUE_ID)`
    - `MLFlowLogger(run_id=UNIQUE_ID)`

Step 3: Log
-----------

You can customize when and where to log parameters, metrics, models, or media contents. 
You can also use some native integrations that these tracking frameworks have with 
specific training frameworks, for example ``mlflow.pytorch.autolog()``, 
``lightning.pytorch.loggers.MLFlowLogger`` etc. 

Step 4: Finish the run
----------------------

For frameworks that require a call to mark a run as finished, include the appropriate call.
For example, ``wandb.finish()``.

Conceptual code snippets
========================

Let's see how the above works with some code.

The following session uses Wandb and MLflow but it is adaptable to other frameworks.

.. tabs::

    .. tab:: Wandb(online)

        .. code-block:: python
            
            from ray import train
            import wandb

            # Assumes you are passing API key through config
            def train_func(config):
                if train.get_context().get_world_rank() == 0:
                    wandb.login(key=config["wandb_api_key"])

                    wandb.init(
                        id=..., # or train.get_context().get_trial_id(),
                        name=..., # or train.get_context().get_trial_name(),
                        group=..., # or train.get_context().get_experiment_name(),
                        # ...
                    )

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to wandb to avoid dup
                if train.get_context().get_world_rank() == 0:
                    wandb.log(metrics)

                # ...

                if train.get_context().get_world_rank() == 0:
                    wandb.finish()

    .. tab:: Wandb(offline)

        .. code-block:: python
            
            from ray import train
            import wandb

            def train_func(config):
                os.environ["WANDB_MODE"] = "offline"
                if train.get_context().get_world_rank() == 0:
                    wandb.init(
                        dir="...",  # some shared storage path like "/mnt/cluster_storage"
                        id=..., # or train.get_context().get_trial_id(),
                        name=..., # or train.get_context().get_trial_name(),
                        group=..., # or train.get_context().get_experiment_name(),
                        # ...
                    )

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to wandb to avoid dup
                if train.get_context().get_world_rank() == 0:
                    wandb.log(metrics)

                # ...

                if train.get_context().get_world_rank() == 0:
                    wandb.finish()

    .. tab:: MLflow(online)

        .. code-block:: python
            
            from ray import train
            import mlflow

            # Run the following on the head node:
            # $ databricks configure --token
            # mv ~/.databrickscfg YOUR_SHARED_STORAGE_PATH
            # This function assumes `databricks_config_file` in config
            def train_func(config):
                os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
                mlflow.set_tracking_uri("databricks")
                mlflow.set_experiment_id(...)
                mlflow.start_run()

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the results from the first worker to mlflow to avoid duplication
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

    .. tab:: MLflow(offline)

        .. code-block:: python
            
            from ray import train
            import mlflow

            # Assumes you are passing a save dir through config
            def train_func(config):
                save_dir = config["save_dir"]
                if train.get_context().get_world_rank() == 0:
                    # mlflow works the best if this is a folder dedicated to mlruns.
                    mlflow.set_tracking_uri(f"file:{save_dir}")
                    mlflow.set_experiment("my_experiment")
                    mlflow.start_run()

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to mlflow to avoid dup
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

Runnable code
=============

PyTorch
-------

.. tabs::

    .. tab:: Log to Wandb (online)

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking//torch_exp_tracking_wandb.py
            :emphasize-lines: 17, 18, 19, 48, 49, 51, 52, 57
            :language: python
            :start-after: __start__

    .. tab:: Log to file based MLflow (offline)

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/torch_exp_tracking_mlflow.py
            :emphasize-lines: 21, 22, 54, 55, 61
            :language: python
            :start-after: __start__


PyTorch Lightning
-----------------

The native Logger integration in PyTorch Lightning with W&B, CometML, MLFlow, 
and Tensorboard can still be used seamlessly with Ray Train TorchTrainer.

The following example will walk you through how. The code here is runnable. 
There is a common shared piece of setting up a dummy model and dataloader
just for demonstration purposes.
        
.. dropdown:: Define your model and dataloader (Dummy ones for demonestration purposes)

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
        :language: python

**Define the training loop that logs**

.. tabs::

    .. tab:: wandb

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_wandb.py
            :language: python
            :start-after: __lightning_experiment_tracking_wandb_start__

    .. tab:: comet

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_comet.py
            :language: python
            :start-after: __lightning_experiment_tracking_comet_start__

    .. tab:: mlflow

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_mlflow.py
            :language: python
            :start-after: __lightning_experiment_tracking_mlflow_start__

    .. tab:: tensorboard
        
        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_tensorboard.py
            :language: python
            :start-after: __lightning_experiment_tracking_tensorboard_start__

Common Errors
=============

**I have already called `wandb login` cli, but still getting 
"wandb: ERROR api_key not configured (no-tty). 
call wandb.login(key=[your_api_key])."**

This is probably due to wandb credentials are not set up correctly
on worker nodes. Make sure that you run ``wandb.login`` inside each
training function. You can take a look at the example above.

**"databricks_cli.utils.InvalidConfigurationError: 
You haven't configured the CLI yet!"**

This is usually caused by running ``databricks configure`` which 
generates ``~/.databrickscfg`` only on head node. Move this file to a shared
location that can be accessed by all nodes.
