.. _train-experiment-tracking-native:

===================
Experiment Tracking
===================

.. note::
    This guide is relevant for all trainers in which a custom training loop is defined. 
    This includes :class:`TorchTrainer <ray.train.torch.TorchTrainer>` and 
    :class:`TensorflowTrainer <ray.train.tensorflow.TensorflowTrainer>`

Most experiment tracking libraries work out-of-the-box with Ray Train. 
This guide provides instructions on how to set up the code so that your favorite experiment tracking libraries 
can work for distributed training with Ray Train. We will conclude the session with debugging
tips.

Before we begin, the following is roughly how you can use the native experiment tracking library calls 
inside of Ray Train. 

.. code-block:: python

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Training code and native experiment tracking library calls go here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

Ray Train lets you use native experiment tracking libraries by customizing the tracking 
logic inside the ``train_func`` function. In this way, you can port your experiment tracking 
logic to Ray Train with minimal changes. 

Getting Started
===============

Let's start by looking at some code snippets.

The following session uses Wandb and MLflow but it is adaptable to other frameworks.

.. tabs::

    .. tab:: Wandb

        .. code-block:: python
            
            import ray
            from ray import train
            import wandb

            # This ensures that all ray worker processes have `WANDB_API_KEY` set.
            ray.init(runtime_env={"env_vars": {"WANDB_API_KEY": "your_api_key"}})

            def train_func(config):
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
                # Make sure that all loggings are uploaded to Wandb backend.
                if train.get_context().get_world_rank() == 0:
                    wandb.finish()

    .. tab:: MLflow

        .. code-block:: python
            
            from ray import train
            import mlflow

            # Run the following on the head node:
            # $ databricks configure --token
            # mv ~/.databrickscfg YOUR_SHARED_STORAGE_PATH
            # This function assumes `databricks_config_file` in config
            def train_func(config):
                # Step 1 and 2
                os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
                mlflow.set_tracking_uri("databricks")
                mlflow.set_experiment_id(...)
                mlflow.start_run()

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the results from the first worker to mlflow to avoid duplication

                # Step 3
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

.. tip::

    A major difference between distributed and non-distributed training is that in distributed training, 
    multiple processes are running in parallel and under certain setups they have the same results. If all 
    of them are reported to the tracking backend, there may be duplicated results. To address that,  
    Ray Train lets you apply logging logic to only the rank 0 worker with the following method:
    :meth:`context.get_world_rank() <ray.train.context.TrainContext.get_world_rank>`.

    .. code-block:: python

        from ray import train
        def train_func(config):
            ...
            if train.get_context().get_world_rank() == 0:
                # do your logging logic only for rank0 worker.
            ...

The interaction with experiment tracking backend within the ``train_func`` can be broken 
into 4 logical steps:

- Set up to connect to a tracking backend
- Configure and launch a run
- Log
- Finish the run

Let's dive into each one of them.

Step 1: Set up necessary components to connect to the tracking backend of your choice
-------------------------------------------------------------------------------------

First, you should choose which tracking backend to use: W&B, MLflow, TensorBoard etc.

Some of them operate under either online or offline mode, each with different considerations when
being set up.
For online mode, you log towards a tracking service that is running. Usually you need credentials to access the service.
Under this mode, you need to ensure that all nodes and worker processes have access to credentials.
For offline mode, you log towards the local file directory. Usually no credentials are needed. You need to instead
ensure that there is a shared file system where all nodes can write to.

.. tabs::

    .. tab:: Wandb

        **online**

        Ensure that credentials are set inside of ``train_func``.

        .. code-block:: python
            
            # This is equivalent to `os.environ["WANDB_API_KEY"] = "your_api_key"`
            wandb.login(key="your_api_key")

        **offline**

        Ensure that offline directory points to a shared storage path.

        .. code-block:: python

            os.environ["WANDB_MODE"] = "offline"
            wandb.init(dir="some_shared_storage_path/wandb") 

    .. tab:: MLflow
        
        **online (hosted by Databricks)**
            
        Ensure that all nodes have access to the Databricks config file.

        .. code-block:: python

            # MLflow client will look for a Databricks config file 
            # at the location specified by `os.environ["DATABRICKS_CONFIG_FILE"]`.
            os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
            mlflow.set_tracking_uri("databricks")
            mlflow.start_run()

        **offline**

        Ensure that offline directory points to a shared storage path.

        .. code-block:: python

            mlflow.start_run(tracking_uri="file:some_shared_storage_path/mlruns")

Setting up credentials
~~~~~~~~~~~~~~~~~~~~~~

Please refer to each tracking library's API documentation on this.
This usually involves setting some environment variable or accessing some config file.

For setting environment variable, one may pass the value of environment variable through the ``config`` 
argument of ``train_func`` and set the corresponding environment variable in the ``train_func``.

For accessing config file, one needs to ensure that the config file is accessible to all nodes.
One way to do this is by setting up a shared storage. Another way is to save a copy in each node.

Setting up shared file system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This involves setting up a network filesystem accessible to all nodes in the cluster, 
e.g. AWS EFS or Google Cloud Filestore.

Step 2: Configure and start the run 
-----------------------------------

This usually concerns picking an identifier for the run and associating it with a project.
Please refer to tracking libraries' own documentation for semantics. 

.. tip::
    
    When performing **fault-tolerant training** with auto-restoration, make sure that you use a 
    consistent id to configure all tracking runs that logically belong to the same training run.
    One way to acquire an unique id is through 
    :meth:`ray.train.get_context().get_trial_id() <ray.train.TrainContext.get_trial_id>`.

    .. code-block:: python

        import ray
        from ray.train import ScalingConfig, RunConfig, FailureConfig
        from ray.train.torch import TorchTrainer

        def train_func(config):
            if ray.train.get_context().get_world_rank() == 0:
                wandb.init(id=ray.train.get_context().get_trial_id())
            ...

        trainer = TorchTrainer(
            train_func, 
            run_config=RunConfig(failure_config=FailureConfig(max_failures=3))
        )

        trainer.fit()
            

Step 3: Log
-----------

You can customize within ``train_func`` how to log parameters, metrics, models, or media contents 
just as you would with a single process training script. 
You can also use native integrations that a particular tracking framework has with 
specific training frameworks, for example ``mlflow.pytorch.autolog()``, 
``lightning.pytorch.loggers.MLFlowLogger`` etc. 

Step 4: Finish the run
----------------------

This step ensures that all logs are synced to the tracking service. Depending on the implementation of 
various tracking libraries, sometimes logs are first stored locally before they are synced to the tracking 
service. When training is finished, the node that the training worker resided on could be turned down by 
ray autoscaler. Finishing the run makes sure that all logs are synced by the time training workers exit. 

**Wandb**

.. code-block:: python

    # https://docs.wandb.ai/ref/python/finish
    wandb.finish()

**MLflow**

.. code-block:: python

    # https://mlflow.org/docs/1.2.0/python_api/mlflow.html
    mlflow.end_run()

**Comet**

.. code-block:: python

    # https://www.comet.com/docs/v2/api-and-sdk/python-sdk/reference/Experiment/#experimentend
    Experiment.end()

Runnable Code
=============

PyTorch
-------

.. dropdown:: Log to Wandb (online) 

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking//torch_exp_tracking_wandb.py
            :emphasize-lines: 13, 14, 18, 19, 48, 49, 51, 52
            :language: python
            :start-after: __start__

.. dropdown:: Log to file based MLflow (offline)         

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/torch_exp_tracking_mlflow.py
        :emphasize-lines: 22, 23, 54, 55, 57, 58, 64
        :language: python
        :start-after: __start__
        :end-before: __end__

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

.. dropdown:: Wandb

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_wandb.py
            :language: python
            :start-after: __lightning_experiment_tracking_wandb_start__

.. dropdown:: MLflow

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_mlflow.py
            :language: python
            :start-after: __lightning_experiment_tracking_mlflow_start__
            :end-before: __lightning_experiment_tracking_mlflow_end__

.. dropdown:: Comet

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_comet.py
            :language: python
            :start-after: __lightning_experiment_tracking_comet_start__

.. dropdown:: TensorBoard

    .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_tensorboard.py
            :language: python
            :start-after: __lightning_experiment_tracking_tensorboard_start__
            :end-before: __lightning_experiment_tracking_tensorboard_end__

Common Errors
=============

Missing Credentials
-------------------

**I have already called `wandb login` cli, but still getting 
"wandb: ERROR api_key not configured (no-tty). 
call wandb.login(key=[your_api_key])."**

This is probably due to wandb credentials are not set up correctly
on worker nodes. Make sure that you run ``wandb.login`` inside each
training function. You can take a look at the example above.

Missing Configurations
----------------------

**"databricks_cli.utils.InvalidConfigurationError: 
You haven't configured the CLI yet!"**

This is usually caused by running ``databricks configure`` which 
generates ``~/.databrickscfg`` only on head node. Move this file to a shared
location that can be accessed by all nodes.
