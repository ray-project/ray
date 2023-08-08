.. _train-monitoring:

Experiment Tracking
===================

Most experiment tracking libraries work out-of-the box with Ray Train. This means
you can use the regular tracking library APIs to report your results, models, and
other artifacts.

Ray Train also exposes logging callbacks that automate some of these tasks.

.. _train-monitoring-native:

Using native experiment tracking libraries
------------------------------------------

.. tab-set::

    .. tab-item:: PyTorch

        You can use experiment tracking libraries such as Weights & Biases, Mlflow, or
        Comet directly in your Ray Train training loop.

        There are two things to keep in mind:

        1. Your code is executed in parallel on many workers. However, you often only want to report
        results from one of these workers (usually the first worker - the "rank 0" worker).

        2. When using the native libraries, you should report the results to both Ray Train and
        the experiment tracking library.

        Example:

        .. code-block:: python

            from ray import train

            def train_fn(config):
                context = train.get_context()

                wandb.init(
                    id=context.get_trial_id(),
                    name=context.get_trial_name(),
                    group=context.get_experiment_name(),
                    # ...
                )
                # ...

                loss = optimize()

                metrics = {"loss": loss}
                # Only report the first worker results to wandb
                if context.get_world_rank() == 0:
                    wandb.log(metrics)

                # Also report to Ray Train. Note that this _must_ happen for all workers.
                train.report(metrics)

    .. tab-item:: PyTorch Lightning

        You can keep using the Lightning's native Logger integrations in the worker 
        function. They should work out-of-the box with Ray TorchTrainer.

        Example:

        .. code-block:: python
            
            import pytorch_lightning as pl
            from pytorch_lightning.loggers.wandb import WandbLogger
            from pytorch_lightning.loggers.comet import CometLogger
            from pytorch_lightning.loggers.mlflow import MLFlowLogger

            def train_func_per_worker():
                ...

                wandb_logger = WandbLogger(
                    name="demo-run", 
                    project="demo-project", 
                    id="unique_id",  
                    offline=offline
                )
                
                comet_logger = CometLogger(
                    api_key=YOUR_COMET_API_KEY,
                    experiment_name="demo-experiment",
                    project_name="demo-project,
                    offline=offline,
                )
                
                mlflow_logger = MLFlowLogger(
                    run_name=name,
                    experiment_name=project_name,
                    tracking_uri=f"file:{save_dir}/mlflow",
                )
                
                trainer = pl.Trainer(
                    # ...,
                    logger=[wandb_logger, comet_logger, mlflow_logger],
                )

        .. tip::

            Always make sure to set necessary credentials on each worker.
            Ray Train will not automatically port environment varaibles 
            on the head node to workers.

            Taking Wandb as an example, you can manually set `WANDB_API_KEY`
            on each worker. This allows the ``WandbLogger`` to fetch credentials 
            from environment variables and automatically login to your Wandb account.

            .. code-block:: python
                :emphasize-lines: 5,9
                
                import os
                from pytorch_lightning.loggers.wandb import WandbLogger

                # Grab the api key from the head node
                WANDB_API_KEY = os.environ["WANDB_API_KEY"]

                def train_func_per_worker():
                    # Set environment variable on each worker
                    os.environ["WANDB_API_KEY"] = WANDB_API_KEY
                    wandb_logger = WandbLogger(...)
                    ...

                trainer = TorchTrainer(
                    train_func_per_worker,
                    ...
                )
        
        .. tip::
            
            When performing **fault-tolerant training** with auto-restoration, be sure 
            to specify a unique ID for the Loggers, so that the new workers report to
            the same run after restoration.

            For example:
            
            - `WandbLogger(id=UNIQUE_ID)`
            - `CometLogger(experiment_key=UNIQUE_ID)`
            - `MLFlowLogger(run_id=UNIQUE_ID)`


        
Automatic setup methods
~~~~~~~~~~~~~~~~~~~~~~~
Ray Train provides utilities for common experiment tracking libraries to automatically
setup an experiment with the training run name and ID used by Ray Train. It also
initializes the library in a way that only the rank 0 worker reports the results per
default.

.. code-block:: python

    from ray import train
    from ray.air.integrations.wandb import setup_wandb

    def train_fn(config):
        wandb = setup_wandb(config)

        loss = optimize()

        metrics = {"loss": loss}

        # No need to guard this behind the rank check anymore - Ray Train does
        # this automatically in the returned `wandb` object.
        wandb.log(metrics)

        # Also report to Ray Train.
        train.report(metrics)


Using Ray Train logger callbacks
--------------------------------
Ray Train also exposes logging callbacks that automatically report results to
experiment tracking services. This will use the results reported via the
:func:`~ray.train.report` API.

Logger callbacks provide a simple tracking integration that don't require changes to your
training code. If you need access to more fine grained APIs, use
the :ref:`native integrations directly <train-monitoring-native>`.

Example: Logging to MLflow and TensorBoard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Step 1: Install the necessary packages**

.. code-block:: bash

    $ pip install mlflow
    $ pip install tensorboardX

**Step 2: Run the following training script**

.. literalinclude:: /../../python/ray/train/examples/mlflow_simple_example.py
   :language: python
