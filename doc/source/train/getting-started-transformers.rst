.. _train-pytorch-transformers:

Get Started with Distributed Training using Hugging Face Transformers
=====================================================================

This tutorial shows you how to convert an existing Hugging Face Transformers script to use Ray Train for distributed training.

In this guide, learn how to:

1. Configure a :ref:`training function <train-overview-training-function>` that properly reports metrics and saves checkpoints.
2. Configure :ref:`scaling <train-overview-scaling-config>` and resource requirements for CPUs or GPUs for your distributed training job.
3. Launch a distributed training job with :class:`~ray.train.torch.TorchTrainer`.


Requirements
------------

Install the necessary packages before you begin:

.. code-block:: bash

    pip install "ray[train]" torch "transformers[torch]" datasets evaluate numpy scikit-learn


Quickstart
----------

Here's a quick overview of the final code structure:

.. testcode::
    :skipif: True

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func():
        # Your Transformers training code here
        ...

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

The key components are:

1. `train_func`: Python code that runs on each distributed training worker.
2. :class:`~ray.train.ScalingConfig`: Defines the number of distributed training workers and GPU usage.
3. :class:`~ray.train.torch.TorchTrainer`: Launches and manages the distributed training job.

Code Comparison: Hugging Face Transformers vs. Ray Train Integration
--------------------------------------------------------------------

Compare a standard Hugging Face Transformers script with its Ray Train equivalent:

.. tab-set::

    .. tab-item:: Hugging Face Transformers

        .. This snippet isn't tested because it doesn't use any Ray code.

        .. testcode::
            :skipif: True

            # Adapted from Hugging Face tutorial: https://huggingface.co/docs/transformers/training

            import numpy as np
            import evaluate
            from datasets import load_dataset
            from transformers import (
                Trainer,
                TrainingArguments,
                AutoTokenizer,
                AutoModelForSequenceClassification,
            )

            # Datasets
            dataset = load_dataset("yelp_review_full")
            tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

            def tokenize_function(examples):
                return tokenizer(examples["text"], padding="max_length", truncation=True)

            small_train_dataset = dataset["train"].select(range(100)).map(tokenize_function, batched=True)
            small_eval_dataset = dataset["test"].select(range(100)).map(tokenize_function, batched=True)

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

            # Hugging Face Trainer
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



    .. tab-item:: Hugging Face Transformers + Ray Train

        .. code-block:: python
            :emphasize-lines: 13-15, 21, 67-68, 72, 80-87

            import os

            import numpy as np
            import evaluate
            from datasets import load_dataset
            from transformers import (
                Trainer,
                TrainingArguments,
                AutoTokenizer,
                AutoModelForSequenceClassification,
            )

            import ray.train.huggingface.transformers
            from ray.train import ScalingConfig
            from ray.train.torch import TorchTrainer


            # [1] Encapsulate data preprocessing, training, and evaluation
            # logic in a training function
            # ============================================================
            def train_func():
                # Datasets
                dataset = load_dataset("yelp_review_full")
                tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

                def tokenize_function(examples):
                    return tokenizer(examples["text"], padding="max_length", truncation=True)

                small_train_dataset = (
                    dataset["train"].select(range(100)).map(tokenize_function, batched=True)
                )
                small_eval_dataset = (
                    dataset["test"].select(range(100)).map(tokenize_function, batched=True)
                )

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

                # Hugging Face Trainer
                training_args = TrainingArguments(
                    output_dir="test_trainer",
                    evaluation_strategy="epoch",
                    save_strategy="epoch",
                    report_to="none",
                )

                trainer = Trainer(
                    model=model,
                    args=training_args,
                    train_dataset=small_train_dataset,
                    eval_dataset=small_eval_dataset,
                    compute_metrics=compute_metrics,
                )

                # [2] Report Metrics and Checkpoints to Ray Train
                # ===============================================
                callback = ray.train.huggingface.transformers.RayTrainReportCallback()
                trainer.add_callback(callback)

                # [3] Prepare Transformers Trainer
                # ================================
                trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)

                # Start Training
                trainer.train()


            # [4] Define a Ray TorchTrainer to launch `train_func` on all workers
            # ===================================================================
            ray_trainer = TorchTrainer(
                train_func,
                scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
                # [4a] For multi-node clusters, configure persistent storage that is
                # accessible across all worker nodes
                # run_config=ray.train.RunConfig(storage_path="s3://..."),
            )
            result: ray.train.Result = ray_trainer.fit()

            # [5] Load the trained model
            with result.checkpoint.as_directory() as checkpoint_dir:
                checkpoint_path = os.path.join(
                    checkpoint_dir,
                    ray.train.huggingface.transformers.RayTrainReportCallback.CHECKPOINT_NAME,
                )
                model = AutoModelForSequenceClassification.from_pretrained(checkpoint_path)


Set up a training function
--------------------------

.. include:: ./common/torch-configure-train_func.rst

Ray Train sets up the distributed process group on each worker before entering the training function.
Put all your logic into this function, including:
- Dataset construction and preprocessing
- Model initialization
- Transformers trainer definition

.. note::

    When using Hugging Face Datasets or Evaluate, always call ``datasets.load_dataset`` and ``evaluate.load``
    inside the training function. Don't pass loaded datasets and metrics from outside the training
    function, as this can cause serialization errors when transferring objects to workers.


Report checkpoints and metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To persist checkpoints and monitor training progress, add a
:class:`ray.train.huggingface.transformers.RayTrainReportCallback` utility callback to your Trainer:


.. code-block:: diff

     import transformers
     from ray.train.huggingface.transformers import RayTrainReportCallback

     def train_func():
         ...
         trainer = transformers.Trainer(...)
    +    trainer.add_callback(RayTrainReportCallback())
         ...


Reporting metrics and checkpoints to Ray Train enables integration with Ray Tune and :ref:`fault-tolerant training <train-fault-tolerance>`.
The :class:`ray.train.huggingface.transformers.RayTrainReportCallback` provides a basic implementation, and you can :ref:`customize it <train-dl-saving-checkpoints>` to fit your needs.


Prepare a Transformers Trainer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pass your Transformers Trainer into
:meth:`~ray.train.huggingface.transformers.prepare_trainer` to validate
configurations and enable Ray Data integration:


.. code-block:: diff

     import transformers
     import ray.train.huggingface.transformers

     def train_func():
         ...
         trainer = transformers.Trainer(...)
    +    trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)
         trainer.train()
         ...


.. include:: ./common/torch-configure-run.rst


Next steps
----------

Now that you've converted your Hugging Face Transformers script to use Ray Train:

* Explore :ref:`User Guides <train-user-guides>` to learn about specific tasks
* Browse the :doc:`Examples <examples>` for end-to-end Ray Train applications
* Consult the :ref:`API Reference <train-api>` for detailed information on the classes and methods


.. _transformers-trainer-migration-guide:

TransformersTrainer Migration Guide
-----------------------------------

Ray 2.1 introduced `TransformersTrainer` with a `trainer_init_per_worker` interface
to define `transformers.Trainer` and execute a pre-defined training function.

Ray 2.7 introduced the unified :class:`~ray.train.torch.TorchTrainer` API,
which offers better transparency, flexibility, and simplicity. This API aligns more closely
with standard Hugging Face Transformers scripts, giving you better control over your
training code.


.. tab-set::

    .. tab-item:: (Deprecating) TransformersTrainer

        .. This snippet isn't tested because it contains skeleton code.

        .. testcode::
            :skipif: True

            import transformers
            from transformers import AutoConfig, AutoModelForCausalLM
            from datasets import load_dataset

            import ray
            from ray.train.huggingface import TransformersTrainer
            from ray.train import ScalingConfig


            hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
            # optional: preprocess the dataset
            # hf_datasets = hf_datasets.map(preprocess, ...)

            ray_train_ds = ray.data.from_huggingface(hf_datasets["train"])
            ray_eval_ds = ray.data.from_huggingface(hf_datasets["validation"])

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

            # Build a Ray TransformersTrainer
            scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
            ray_trainer = TransformersTrainer(
                trainer_init_per_worker=trainer_init_per_worker,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "validation": ray_eval_ds},
            )
            result = ray_trainer.fit()


    .. tab-item:: (New API) TorchTrainer

        .. This snippet isn't tested because it contains skeleton code.

        .. testcode::
            :skipif: True

            import transformers
            from transformers import AutoConfig, AutoModelForCausalLM
            from datasets import load_dataset

            import ray
            from ray.train.torch import TorchTrainer
            from ray.train.huggingface.transformers import (
                RayTrainReportCallback,
                prepare_trainer,
            )
            from ray.train import ScalingConfig


            hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
            # optional: preprocess the dataset
            # hf_datasets = hf_datasets.map(preprocess, ...)

            ray_train_ds = ray.data.from_huggingface(hf_datasets["train"])
            ray_eval_ds = ray.data.from_huggingface(hf_datasets["validation"])

            # [1] Define the full training function
            # =====================================
            def train_func():
                MODEL_NAME = "gpt2"
                model_config = AutoConfig.from_pretrained(MODEL_NAME)
                model = AutoModelForCausalLM.from_config(model_config)

                # [2] Build Ray Data iterables
                # ============================
                train_dataset = ray.train.get_dataset_shard("train")
                eval_dataset = ray.train.get_dataset_shard("validation")

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

                # [3] Add Ray Train Report Callback
                # =================================
                trainer.add_callback(RayTrainReportCallback())

                # [4] Prepare your trainer
                # ========================
                trainer = prepare_trainer(trainer)
                trainer.train()

            # Build a Ray TorchTrainer
            scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
            ray_trainer = TorchTrainer(
                train_func,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "validation": ray_eval_ds},
            )
            result = ray_trainer.fit()
