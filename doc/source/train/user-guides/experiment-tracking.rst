.. _train-experiment-tracking-native:

==================================
Experiment Tracking with Ray Train
==================================

.. note::
    This guide is relevant for all DataParallelTrainer, including TorchTrainer and TensorflowTrainer.

Most experiment tracking libraries work out-of-the-box with Ray Train. 
In the following guide, you will learn how to set up code so that your favorite experiment tracking libraries 
can work with distributed data parallel training with Ray Train. We will conclude the session with debugging
tips.

Ray Train lets you use native experiment tracking libraries by customizing your tracking 
logic inside ``train_loop_per_worker``. 
This gives you the highest degree of freedom to decide when and where to log params, metrics, 
models or even media contents. 
This also allows you to use some native integrations that these tracking frameworks have with 
specific training frameworks, for example ``mlflow.pytorch.autolog()``, 
``lightning.pytorch.loggers.MLFlowLogger`` etc. 
In this way, you get to port your experiment tracking logic to Ray Train with minimal change. 

How to set up your code to log distributed training experiment
==============================================================

Step 1: Decide a tracking service to use
----------------------------------------

For easy configuration, we recommend the following:

**MLflow:**

- offline mode: file based tracking uri pointing to shared storage path (``mlflow.start_run(tracking_uri=”file:some_shared_storage_path”)``)
- online mode: externally hosted by Databricks(``mlflow.start_run(tracking_uri=”databricks”)``).

**TensorBoard:**

A shared storage location where all nodes can write to.

**W&B:**

- offline mode: directory pointing to shared storage path (``wandb.init(dir="some_shared_storage_path")``)
- online mode: should work out-of-the-box with correct credentials

Step 2: Make sure that all nodes and processes have access to credentials
-------------------------------------------------------------------------

See example below.

Step3: Initialize the run 
-------------------------

Ray Train provides a training context from where you can grab the 
training id (:meth:`context.get_trial_id() <ray.train.context.TrainContext.get_trial_id>`) 
and name (:meth:`context.get_trial_name() <ray.train.context.TrainContext.get_trial_name>`) 
if desired. 

Step4: Log
----------

Dedupe if necessary. Ray Train allows you to only log from rank 0 worker 
(:meth:`context.get_world_rank() <ray.train.context.TrainContext.get_world_rank>`).

Step5: Finish the run
---------------------

Some frameworks requires a call to mark a run as finished. For example, ``wandb.finish()``.

Code Example
============

Let's see how the above works with some code.

In the following session, we will use Wandb and MLflow but the idea should be easily 
adaptable to other frameworks.

Pytorch
-------

Conceptual code snippets
~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. tab:: wandb(online)

        .. code-block:: python
            
            from ray import train
            import wandb

            # assuming passing API key through config
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

    .. tab:: wandb(offline)

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


    .. tab:: file based MLflow (offline)

        .. code-block:: python
            
            from ray import train
            import mlflow

            # assuming passing a save dir through config
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

    .. tab:: MLflow externally hosted by databricks (online)

        .. code-block:: python
            
            from ray import train
            import mlflow

            # on head node, run the following:
            # $ databricks configure --token
            # mv ~/.databrickscfg YOUR_SHARED_STORAGE_PATH
            # This function is assuming `databricks_config_file` in config
            def train_func(config):
                os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
                mlflow.set_tracking_uri("databricks")
                mlflow.set_experiment_id(...)
                mlflow.start_run()

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to mlflow to avoid dup
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

Runnable code
~~~~~~~~~~~~~

.. tabs::

    .. tab:: Log to Wandb (online)

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking//torch_exp_tracking_wandb.py
            :emphasize-lines: 17, 18, 47, 48, 50, 51, 56
            :language: python
            :start-after: __start__

    .. tab:: Log to file based MLflow (offline)

        .. literalinclude:: ../../../../python/ray/train/examples/experiment_tracking/torch_exp_tracking_mlflow.py
            :emphasize-lines: 18, 19, 51, 52, 57
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
        
.. tip::
    
    When performing **fault-tolerant training** with auto-restoration, be sure 
    to specify a unique ID for the Loggers, so that the new workers report to
    the same run after restoration.

    For example:
    
    - `WandbLogger(id=UNIQUE_ID)`
    - `CometLogger(experiment_key=UNIQUE_ID)`
    - `MLFlowLogger(run_id=UNIQUE_ID)`

Common Errors
=============

**I have already called ``wandb login`` cli, but still getting 
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
