.. _train-experiment-tracking-native:

==================================
Experiment Tracking with Ray Train
==================================

.. note::
    This guide is relevant for all DataParallelTrainer, including TorchTrainer and TensorflowTrainer.

Most experiment tracking libraries work out-of-the-box with Ray Train. 
In the following guide, you will learn how to use your favorite experiment tracking libraries 
while doing distributed data parallel training with Ray Train. 

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
MLflow: file based tracking uri (``mlflow.start_run(tracking_uri=”file:some_shared_storage_location”)``)
or externally hosted by Databricks(``mlflow.start_run(tracking_uri=”databricks”)``).
TensorBoard: a shared storage location where all nodes can write to ().
W&B 

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

    .. tab:: wandb

        .. code-block:: python
            
            from ray import train
            import wandb

            # assuming passing API key through config
            def train_func(config):
                os.environ["WANDB_API_KEY"] = config["wandb_api_key"]

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

                wandb.finish()

    .. tab:: file based MLflow

        .. code-block:: python
            
            from ray import train
            import mlflow

            # assuming passing a save dir through config
            def train_func(config):
                save_dir = config["save_dir"]
                mlflow.start_run(tracking_uri=f"file:{save_dir}")

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to mlflow to avoid dup
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

    .. tab:: MLflow externally hosted by databricks

        .. code-block:: python
            
            from ray import train
            import mlflow

            # on head node, run the following:
            # $ databricks configure --token
            # mv ~/.databrickscfg YOUR_SHARED_STORAGE_PATH
            # This function is assuming `databricks_config_file` in config
            def train_func(config):
                os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
                mlflow.start_run(tracking_uri="databricks", experiment_id=...)

                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to mlflow to avoid dup
                if train.get_context().get_world_rank() == 0:
                    mlflow.log_metrics(metrics)

runnable code
~~~~~~~~~~~~~

.. tabs::

    .. tab:: Log to Wandb

        .. literalinclude:: ../doc_code/wandb_torch_mnist.py
            :emphasize-lines: 16, 45, 50, 52, 56
            :language: python
            :start-after: __start__

    .. tab:: Log to file based MLflow

        .. literalinclude:: ../doc_code/mlflow_torch_mnist.py
            :emphasize-lines: 18, 19, 48, 52
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

    .. literalinclude:: ../doc_code/lightning_experiment_tracking.py
        :language: python
        :start-after: __lightning_experiment_tracking_model_data_start__
        :end-before: __lightning_experiment_tracking_model_data_end__

.. tabs::

    .. tab:: wandb

        .. literalinclude:: ../doc_code/lightning_experiment_tracking.py
            :language: python
            :start-after: __lightning_experiment_tracking_wandb_start__
            :end-before: __lightning_experiment_tracking_wandb_end__

    .. tab:: comet

        .. literalinclude:: ../doc_code/lightning_experiment_tracking.py
            :language: python
            :start-after: __lightning_experiment_tracking_comet_start__
            :end-before: __lightning_experiment_tracking_comet_end__

    .. tab:: mlflow

        .. literalinclude:: ../doc_code/lightning_experiment_tracking.py
            :language: python
            :start-after: __lightning_experiment_tracking_mlflow_start__
            :end-before: __lightning_experiment_tracking_mlflow_end__

    .. tab:: tensorboard
        
        .. literalinclude:: ../doc_code/lightning_experiment_tracking.py
            :language: python
            :start-after: __lightning_experiment_tracking_tensorboard_start__
            :end-before: __lightning_experiment_tracking_tensorboard_end__
        
.. tip::
    
    When performing **fault-tolerant training** with auto-restoration, be sure 
    to specify a unique ID for the Loggers, so that the new workers report to
    the same run after restoration.

    For example:
    
    - `WandbLogger(id=UNIQUE_ID)`
    - `CometLogger(experiment_key=UNIQUE_ID)`
    - `MLFlowLogger(run_id=UNIQUE_ID)`
