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

This function will be executed on each distributed training worker. Ray Train will set up the distributed 
process group on each worker before entering this function.

Please put all the logics into this function, including dataset construction and preprocessing, 
model initialization, transformers trainer definition and more.

.. note::

    If you are using HuggingFace Datasets or Evaluate, make sure to call ``datasets.load_dataset`` and ``evaluate.load`` 
    inside the training function. We do not recommend passing the loaded datasets and metrics from outside of the training 
    function, because it might cause serialization errors while transferring the objects to the workers.


Reporting metrics and checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To monitor progress, you can report intermediate metrics and checkpoints
using the :class:`ray.train.huggingface.RayTrainReportCallback` utility callback.

                    
.. code-block:: diff

     import transformers
     import ray.train.huggingface

     def train_func(config):
         ...
         trainer = transformers.Trainer(...)
    +    trainer.add_callback(ray.train.huggingface.RayTrainReportCallback())
         ...

Reporting metrics and checkpoints to Ray Train ensures that you can use Ray Tune and :ref:`fault-tolerant training <train-fault-tolerance>`.

Note that `RayTrainReportCallback` only provides a simple implementation. To customize, see :ref:`train-checkpointing`.


Preparing your Transformers Trainer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, pass your Transformers Trainer into
:meth:`~ray.train.huggingface.prepare_trainer` to validate 
your configurations and enable Ray Data Integration. 


.. code-block:: diff

     import transformers
     import ray.train.huggingface

     def train_func(config):
         ...
         trainer = transformers.Trainer(...)
    +    trainer = ray.train.huggingface.prepare_trainer(trainer)
         trainer.train()
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

Congratulations! You have successfully converted your HuggingFace Transformers training script to use Ray Train.

* Head over to the :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :ref:`Examples <train-examples>` for end-to-end examples of how to use Ray Train.
* Dive into the :ref:`API Reference <train-api>` for more details on the classes and methods used in this tutorial.


.. _transformers-trainer-migration-guide:

``TransformersTrainer`` Migration Guide
------------------------------------

The `TransformersTrainer` was added in Ray 2.1. It exposes a `trainer_init_per_worker` interface 
to define `transformers.Trainer`, then runs a pre-defined training loop in a black box.

In Ray 2.7, we're pleased to introduce the newly unified :class:`~ray.train.torch.TorchTrainer` API, 
which offers enhanced transparency, flexibility, and simplicity. This API is more aligned
with standard HuggingFace Transformers scripts, ensuring users have better control over their 
native Transformers training code.


.. tabs::

    .. group-tab:: (Deprecating) TransformersTrainer


        .. code-block:: python
            
            import transformers
            from transformers import AutoConfig, AutoModelForCausalLM
            from datasets import load_dataset

            import ray
            from ray.train.huggingface import TransformersTrainer
            from ray.train import ScalingConfig

            # Dataset
            def preprocess(examples):
                ...

            hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
            processed_ds = hf_datasets.map(preprocess, ...)

            ray_train_ds = ray.data.from_huggingface(processed_ds["train"])
            ray_eval_ds = ray.data.from_huggingface(processed_ds["validation"])

            # Define the Trainer generation function
            def trainer_init_per_worker(train_dataset, eval_dataset, **config):
                MODEL_NAME = "gpt2"
                model_config = AutoConfig.from_pretrained(MODEL_NAME)
                model = AutoModelForCausalLM.from_config(model_config)
                args = transformers.TrainingArguments(
                    output_dir=f"{MODEL_NAME}-wikitext2",
                    evaluation_strategy="epoch",
                    save_strategy="epoch",
                    logging_strategy="epoch",
                    learning_rate=2e-5,
                    weight_decay=0.01,
                    max_steps=100,
                )
                return transformers.Trainer(
                    model=model,
                    args=args,
                    train_dataset=train_dataset,
                    eval_dataset=eval_dataset,
                )

            # Build a Ray HuggingFaceTrainer
            scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
            ray_trainer = TransformersTrainer(
                trainer_init_per_worker=trainer_init_per_worker,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "evaluation": ray_eval_ds},
            )
            result = ray_trainer.fit()
                

    .. group-tab:: TorchTrainer

        .. code-block:: python
            
            import transformers
            from transformers import AutoConfig, AutoModelForCausalLM
            from datasets import load_dataset

            import ray
            from ray.train.huggingface import TransformersTrainer
            from ray.train import ScalingConfig

            # Dataset
            def preprocess(examples):
                ...

            hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
            processed_ds = hf_datasets.map(preprocess, ...)

            ray_train_ds = ray.data.from_huggingface(processed_ds["train"])
            ray_eval_ds = ray.data.from_huggingface(processed_ds["evaluation"])

            # [1] Define the full training function
            def train_func(config):
                MODEL_NAME = "gpt2"
                model_config = AutoConfig.from_pretrained(MODEL_NAME)
                model = AutoModelForCausalLM.from_config(model_config)

                # [2] Build Ray Data iterables
                train_dataset = ray.train.get_dataset_shard("train")
                eval_dataset = ray.train.get_dataset_shard("eval")

                train_iterable_ds = train_dataset.iter_torch_batches(batch_size=8)
                eval_iterable_ds = eval_dataset.iter_torch_batches(batch_size=8)

                args = transformers.TrainingArguments(
                    output_dir=f"{MODEL_NAME}-wikitext2",
                    evaluation_strategy="epoch",
                    save_strategy="epoch",
                    logging_strategy="epoch",
                    learning_rate=2e-5,
                    weight_decay=0.01,
                    max_steps=100,
                )
                
                trainer = transformers.Trainer(
                    model=model,
                    args=args,
                    train_dataset=train_iterable_ds,
                    eval_dataset=eval_iterable_ds,
                )

                # [3] Prepare your trainer
                trainer = ray.train.huggingface.prepare_trainer(trainer)
                trainer.train()

            # Build a Ray TorchTrainer
            scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
            ray_trainer = TorchTrainer(
                train_func,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "evaluation": ray_eval_ds},
            )
            result = ray_trainer.fit()
