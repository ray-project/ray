.. _train-pytorch-lightning:

Getting Started with HuggingFace Transformers
=============================================

This tutorial will walk you through the process of converting an existing HuggingFace Transformers script to use Ray Train.

By the end of this, you will learn how to:

1. Configure your training function to report metrics and save checkpoints.
2. Configure scale and CPU/GPU resource requirements for your training job.
3. Launch your distributed training job with a :class:`~ray.train.torch.TorchTrainer`.

Quickstart
----------

Before we begin, you can expect that the final code will look something like this:

.. code-block:: python

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Your PyTorch Lightning training code here.
    
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. Your `train_func` will be the Python code that is executed on each distributed training worker.
2. Your `ScalingConfig` will define the number of distributed training workers and whether to use GPUs.
3. Your `TorchTrainer` will launch the distributed training job.

Let's compare a HuggingFace Transformers training script with and without Ray Train.

.. tabs::

    .. group-tab:: HuggingFace Transformers

        .. code-block:: python

            # Adapted from HuggingFace tutorial: https://huggingface.co/docs/transformers/training

            from datasets import load_dataset
            from transformers import AutoTokenizer
            from transformers import AutoModelForSequenceClassification
            from transformers import TrainingArguments, Trainer
            import numpy as np
            import evaluate

            # Datasets
            dataset = load_dataset("yelp_review_full")
            tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

            def tokenize_function(examples):
                return tokenizer(examples["text"], padding="max_length", truncation=True)

            tokenized_datasets = dataset.map(tokenize_function, batched=True)

            small_train_dataset = tokenized_datasets["train"].shuffle(seed=42).select(range(1000))
            small_eval_dataset = tokenized_datasets["test"].shuffle(seed=42).select(range(1000))

            # Model
            model = AutoModelForSequenceClassification.from_pretrained(
                "bert-base-cased", num_labels=5
            )

            # Metrics
            metric = evaluate.load("accuracy")

            def compute_metrics(eval_pred):
                logits, labels = eval_pred
                predictions = np.argmax(logits, axis=-1)
                return metric.compute(predictions=predictions, references=labels)

            # HuggingFace Trainer
            training_args = TrainingArguments(
                output_dir="test_trainer", evaluation_strategy="epoch", report_to="none"
            )

            trainer = Trainer(
                model=model,
                args=training_args,
                train_dataset=small_train_dataset,
                eval_dataset=small_eval_dataset,
                compute_metrics=compute_metrics,
            )

            # Start Training
            trainer.train()

                

    .. group-tab:: HuggingFace Transformers + Ray Train

        .. code-block:: python

            from datasets import load_dataset
            from transformers import AutoTokenizer
            from transformers import AutoModelForSequenceClassification
            from transformers import TrainingArguments, Trainer
            import numpy as np
            import evaluate

            import ray.train.huggingface
            from ray.train import ScalingConfig
            from ray.train.torch import TorchTrainer

            # [1] Encapsulate data preprocessing, training, and evaluation 
            # logic in a training function
            def train_func(config):
                # Datasets
                dataset = load_dataset("yelp_review_full")
                tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

                def tokenize_function(examples):
                    return tokenizer(examples["text"], padding="max_length", truncation=True)

                tokenized_ds = dataset.map(tokenize_function, batched=True)

                small_train_ds = tokenized_ds["train"].shuffle(seed=42).select(range(1000))
                small_eval_ds = tokenized_ds["test"].shuffle(seed=42).select(range(1000))

                # Model
                model = AutoModelForSequenceClassification.from_pretrained(
                    "bert-base-cased", num_labels=5
                )

                # Evaluation Metrics
                metric = evaluate.load("accuracy")

                def compute_metrics(eval_pred):
                    logits, labels = eval_pred
                    predictions = np.argmax(logits, axis=-1)
                    return metric.compute(predictions=predictions, references=labels)

                # HuggingFace Trainer
                training_args = TrainingArguments(
                    output_dir="test_trainer", evaluation_strategy="epoch", report_to="none"
                )

                trainer = Trainer(
                    model=model,
                    args=training_args,
                    train_dataset=small_train_ds,
                    eval_dataset=small_eval_ds,
                    compute_metrics=compute_metrics,
                )

                # [2] Report Metrics and Checkpoints to Ray Train
                callback = ray.train.huggingface.RayTrainReportCallback()
                trainer.add_callback(callback)

                # [3] Prepare Transformers Trainer
                trainer = ray.train.huggingface.prepare_trainer(trainer)

                # Start Training
                trainer.train()

            # Define a Ray TorchTrainer that launchs `train_func` on all workers
            ray_trainer = TorchTrainer(
                train_func, scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
            )
            ray_trainer.fit()


Now, let's get started!

Setting up your training function
---------------------------------

First, you'll want to update your training code to support distributed training. 
You can begin by wrapping your code in a function:

.. code-block:: python

    def train_func(config):
        # Your PyTorch Lightning training code here.

This function will be executed on each distributed training worker.


Ray Train will set up your distributed process group on each worker. You only need to 
make a few changes to your Lightning Trainer definition.

.. code-block:: diff

     import pytorch_lightning as pl
    -from pl.strategies import DDPStrategy
    -from pl.plugins.environments import LightningEnvironment
    +import ray.train.lightning 

     def train_func(config):
         ...
         model = MyLightningModule(...)
         datamodule = MyLightningDataModule(...)
        
         trainer = pl.Trainer(
    -        devices=[0,1,2,3],
    -        strategy=DDPStrategy(),
    -        plugins=[LightningEnvironment()],
    +        devices="auto",
    +        accelerator="auto",
    +        strategy=ray.train.lightning.RayDDPStrategy(),
    +        plugins=[ray.train.lightning.RayLightningEnvironment()]
         )
    +    trainer = ray.train.lightning.prepare_trainer(trainer)
        
         trainer.fit(model, datamodule=datamodule)

We will now go over each change.

Configuring distributed strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ray Train offers several subclassed distributed strategies for Lightning. 
These strategies retain the same argument list as their base strategy classes. 
Internally, they configure the root device and the distributed 
sampler arguments.
    
- :class:`~ray.train.lightning.RayDDPStrategy` 
- :class:`~ray.train.lightning.RayFSDPStrategy` 
- :class:`~ray.train.lightning.RayDeepSpeedStrategy` 


.. code-block:: diff

     import pytorch_lightning as pl
    -from pl.strategies import DDPStrategy
    +import ray.train.lightning

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        strategy=DDPStrategy(),
    +        strategy=ray.train.lightning.RayDDPStrategy(),
             ...
         )
         ...

Configuring Ray cluster environment plugin
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ray Train also provides :class:`~ray.train.lightning.RayLightningEnvironment` 
as a specification for Ray Cluster. This utility class configures the worker's 
local, global, and node rank and world size.


.. code-block:: diff

     import pytorch_lightning as pl
    -from pl.plugins.environments import LightningEnvironment
    +import ray.train.lightning

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        plugins=[LightningEnvironment()],
    +        plugins=[ray.train.lightning.RayLightningEnvironment()],
             ...
         )
         ...


Configuring parallel devices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition, Ray TorchTrainer has already configured the correct 
``CUDA_VISIBLE_DEVICES`` for you. One should always use all available 
GPUs by setting ``devices="auto"`` and ``acelerator="auto"``.


.. code-block:: diff

     import pytorch_lightning as pl

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        devices=[0,1,2,3],
    +        devices="auto",
    +        accelerator="auto",
             ...
         )
         ...



Reporting metrics and checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To monitor progress, you can report intermediate metrics and checkpoints
using the :class:`ray.train.lightning.RayTrainReportCallback` utility callback.

                    
.. code-block:: diff

     import pytorch_lightning as pl

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    +        callbacks=[ray.train.lightning.RayTrainReportCallback()],
             ...
         )
         ...

Reporting metrics and checkpoints to Ray Train ensures that you can use Ray Tune and fault-tolerant training. For more details, see :ref:`train-checkpointing` and :ref:`train-fault-tolerance`.


Preparing your Lightning Trainer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, pass your Lightning Trainer into
:meth:`~ray.train.lightning.prepare_trainer` to validate 
your configurations. 


.. code-block:: diff

     import pytorch_lightning as pl
     import ray.train.lightning

     def train_func(config):
         ...
         trainer = pl.Trainer(...)
    +    trainer = ray.train.lightning.prepare_trainer(trainer)
         ...


Configuring scale and GPUs
---------------------------

Outside of your training function, create a :class:`~ray.train.ScalingConfig` object to configure:

1. `num_workers` - The number of distributed training worker processes.
2. `use_gpu` - Whether each worker should use a GPU (or CPU).

.. code-block:: python

    from ray.train import ScalingConfig
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)


For more details, see :ref:`train_scaling_config`.

Launching your training job
---------------------------

Tying this all together, you can now launch a distributed training job 
with a :class:`~ray.train.torch.TorchTrainer`.

.. code-block:: python

    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

Please also refer to :ref:`train-run-config` for more configuration options for `TorchTrainer`.

Accessing training results
--------------------------

After training completes, a :class:`~ray.train.Result` object will be returned which contains
information about the training run, including the metrics and checkpoints reported during training.

.. code-block:: python

    result.metrics     # The metrics reported during training.
    result.checkpoint  # The latest checkpoint reported during training.
    result.log_dir     # The path where logs are stored.
    result.error       # The exception that was raised, if training failed.

.. TODO: Add results guide

Next steps
---------- 

Congratulations! You have successfully converted your PyTorch Lightningtraining script to use Ray Train.

* Head over to the :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :ref:`Examples <train-examples>` for end-to-end examples of how to use Ray Train.
* Dive into the :ref:`API Reference <train-api>` for more details on the classes and methods used in this tutorial.


.. _lightning-trainer-migration-guide:

``LightningTrainer`` Migration Guide
------------------------------------

The `LightningTrainer` was added in Ray 2.4, and exposes a  
`LightningConfigBuilder` to define configurations for `pl.LightningModule` 
and `pl.Trainer`. 

It then instantiates the model and trainer objects and runs a pre-defined 
training loop in a black box.


This version of our LightningTrainer API was constraining and limited 
the users' ability to manage the training functionality.

In Ray 2.7, we're pleased to introduce the newly unified :class:`~ray.train.torch.TorchTrainer` API, which offers 
enhanced transparency, flexibility, and simplicity. This API is more aligned
with standard PyTorch Lightning scripts, ensuring users have better 
control over their native Lightning code.


.. tabs::

    .. group-tab:: LightningTrainer


        .. code-block:: python
            
            from ray.train.lightning import LightningConfigBuilder, LightningTrainer

            config_builder = LightningConfigBuilder()
            config_builder.module(cls=MNISTClassifier, lr=1e-3, feature_dim=128)
            config_builder.checkpointing(monitor="val_accuracy", mode="max", save_top_k=3)
            config_builder.trainer(
                max_epochs=10,
                accelerator="gpu",
                log_every_n_steps=100,
                logger=CSVLogger("./logs"),
            )

            datamodule = MNISTDataModule(batch_size=32)
            config_builder.fit_params(datamodule=datamodule)

            ray_trainer = LightningTrainer(
                lightning_config=config_builder.build(),
                scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
                run_config=RunConfig(
                    checkpoint_config=CheckpointConfig(
                        num_to_keep=3,
                        checkpoint_score_attribute="val_accuracy",
                        checkpoint_score_order="max",
                    ),
                )
            )
            ray_trainer.fit()

                

    .. group-tab:: TorchTrainer

        .. code-block:: python
            
            import pytorch_lightning as pl
            from ray.train.torch import TorchTrainer
            from ray.train.lightning import (
                RayDDPStrategy, 
                RayLightningEnvironment,
                RayTrainReportCallback,
                prepare_trainer
            ) 

            def train_func_per_worker():
                model = MNISTClassifier(lr=1e-3, feature_dim=128)
                datamodule = MNISTDataModule(batch_size=32)

                trainer = pl.Trainer(
                    max_epochs=10,
                    accelerator="gpu",
                    log_every_n_steps=100,
                    logger=CSVLogger("./logs"),
                    # New configurations below
                    devices="auto",
                    strategy=RayDDPStrategy(),
                    plugins=[RayLightningEnvironment()],
                    callbacks=[RayTrainReportCallback()],
                )
                trainer = prepare_trainer(trainer)

                trainer.fit(model, datamodule=datamodule)

            ray_trainer = TorchTrainer(
                train_func_per_worker,
                scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
                run_config=RunConfig(
                    checkpoint_config=CheckpointConfig(
                        num_to_keep=3,
                        checkpoint_score_attribute="val_accuracy",
                        checkpoint_score_order="max",
                    ),
                )
            )

            ray_trainer.fit()