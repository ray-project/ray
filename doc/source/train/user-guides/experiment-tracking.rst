.. _train-experiment-tracking-native:

==================================
Experiment Tracking with Ray Train
==================================

.. note::
    This guide is relevant for all DataParallelTrainer, including TorchTrainer and TensorflowTrainer.

Most experiment tracking libraries work out-of-the-box with Ray Train. 
In the following guide, you will learn how to use your favorite experiment tracking libraries 
while doing distributed data parallel training with Ray Train. 

The guide will be focusing on TorchTrainer although the ideas should apply to DataParallelTrainer
in general.

Ray Train lets you use native experiment tracking libraries by customizing your tracking 
logic inside ``train_loop_per_worker``. 
This gives you the highest degree of freedom to decide when and where to log params, metrics, 
models or even media contents. 
This also allows you to use some native integrations that these tracking frameworks have with 
specific training frameworks, for example ``mlflow.pytorch.autolog()``, 
``lightning.pytorch.loggers.MLFlowLogger`` etc. 
In this way, you get to port your experiment tracking logic to Ray Train with minimal change. 

Mental picture of experiment tracking in Ray Train
==================================================

There are many experiment tracking frameworks out there. So instead of trying to 
cover every of them with a variety of usages, let’s first look at how to think 
about experiment tracking with Ray Train DataParallelTrainer, and how it may be 
different than running native training code in a single process. This mental 
picture helps you reason about some errors and makes debugging a lot easier.

In Ray Train DataParallelTrainer, interaction with experiment tracking services 
happens inside of ``train_loop_per_worker``. ``train_loop_per_worker`` is run as a separate process on 
an arbitrary node. This is different from single process training in one 
node in the following ways.

``train_loop_per_worker`` needs to have access to the tracking service
----------------------------------------------------------------------

Usually this is trivial for online externally hosted tracking services. 
Other than that, sometimes offline mode is easier to configure. 
For example, for MLflow, you can use file based tracking uri, 
where the uri points to a shared storage location. For Tensorboard, 
you can have SummaryWriter point to a shared storage location as well.

Advanced usage:
Shared storage location for offline mode may not be necessary. 
If you prefer to not use shared storage in this case, 
you can log to local and have the logged file transferred to the head node 
or to the cloud post training. 

``train_loop_per_worker`` needs to have access to credentials
-------------------------------------------------------------

There are mainly two mechanisms when accessing credentials: 
checking some env var value, and checking the content of some config file. 
When applicable, you need to make sure that env var is set in the training 
function and the file is stored on shared storage to be accessible.

Avoid duplications
------------------

There are probably multiple ``train_loop_per_worker`` processes running, 
each corresponding to a Ray Train Worker. For distributed data 
parallel training, all of them should have the same results. 
You may want to only log from rank 0 worker to avoid duplications. 
You may find how to do it in the following code examples.


Code Example
============

In the following session, we will show some examples using Wandb 
and MLflow but the idea should be easily adaptable to other frameworks.

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
                    id=..., # or train.get_context().get_trial_id()
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
            :emphasize-lines: 16, 45, 50
            :language: python
            :start-after: __start__

    .. tab:: Log to file based MLflow

        .. literalinclude:: ../doc_code/mlflow_torch_mnist.py
            :emphasize-lines: 18, 19, 48, 52
            :language: python
            :start-after: __start__


PyTorch Lightning
-----------------

The native Logger integration in PyTorch Lightning with W&B, CometML, MLFlow, and Tensorboard can still be used seamlessly with Ray Train TorchTrainer.

The following example will walk you through how. The code here is runnable. There is a common shared piece of setting up a dummy model and dataloader
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

Legacy APIs
===========

Should I use ``setup_wandb`` and ``setup_mlflow``?
--------------------------------------------------

``setup_wandb`` and ``setup_mlflow`` are just convenient wrappers on top of
Weights&Biases and MLflow's native APIs. To spare you from learning yet another
set of APIs, we recommend you to continue using the native experiment tracking 
directly.
Just remember to guard with ``rank==0`` to avoid duplications whenever applicable.

Words on ``MLflowLoggerCallback`` and ``WandbLoggerCallback``
-------------------------------------------------------------

More seasoned Ray library users may wonder “Wait, what about `MLflowLoggerCallback
` and `WandbLoggerCallback`?” 
These APIs rely on reporting results to Ray Train and then log to experiment training 
frameworks. The usage scenario is not as versatile and requires some changes to the 
tracking logic. As a result, we advise users against using these callbacks when using any
DataParallelTrainer.
