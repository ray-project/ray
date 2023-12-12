.. _train-pytorch-transformers:

Get Started with Distributed Training using Hugging Face Transformers
=====================================================================

This tutorial walks through the process of converting an existing Hugging Face Transformers script to use Ray Train.

Learn how to:

1. Configure a :ref:`training function <train-overview-training-function>` to report metrics and save checkpoints.
2. Configure :ref:`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for your training job.
3. Launch your distributed training job with a :class:`~ray.train.torch.TorchTrainer`.

Quickstart
----------

For reference, the final code follows:

.. testcode::
    :skipif: True

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Your Transformers training code here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. `train_func` is the Python code that executes on each distributed training worker.
2. :class:`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
3. :class:`~ray.train.torch.TorchTrainer` launches the distributed training job.

Compare a Hugging Face Transformers training script with and without Ray Train.

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

            small_train_dataset = dataset["train"].select(range(1000)).map(tokenize_function, batched=True)
            small_eval_dataset = dataset["test"].select(range(1000)).map(tokenize_function, batched=True)

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
            def train_func(config):
                # Datasets
                dataset = load_dataset("yelp_review_full")
                tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

                def tokenize_function(examples):
                    return tokenizer(examples["text"], padding="max_length", truncation=True)

                small_train_dataset = (
                    dataset["train"].select(range(1000)).map(tokenize_function, batched=True)
                )
                small_eval_dataset = (
                    dataset["test"].select(range(1000)).map(tokenize_function, batched=True)
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
                # [4a] If running in a multi-node cluster, this is where you
                # should configure the run's persistent storage that is accessible
                # across all worker nodes.
                # run_config=ray.train.RunConfig(storage_path="s3://..."),
            )
            result: ray.train.Result = ray_trainer.fit()

            # [5] Load the trained model.
            with result.checkpoint.as_directory() as checkpoint_dir:
                checkpoint_path = os.path.join(
                    checkpoint_dir,
                    ray.train.huggingface.transformers.RayTrainReportCallback.CHECKPOINT_NAME,
                )
                model = AutoModelForSequenceClassification.from_pretrained(checkpoint_path)


Set up a training function
--------------------------

First, update your training code to support distributed training.
You can begin by wrapping your code in a :ref:`training function <train-overview-training-function>`:

.. testcode::
    :skipif: True

    def train_func(config):
        # Your Transformers training code here.

This function executes on each distributed training worker. Ray Train sets up the distributed
process group on each worker before entering this function.

Put all the logic into this function, including dataset construction and preprocessing,
model initialization, transformers trainer definition and more.

.. note::

    If you are using Hugging Face Datasets or Evaluate, make sure to call ``datasets.load_dataset`` and ``evaluate.load``
    inside the training function. Don't pass the loaded datasets and metrics from outside of the training
    function, because it might cause serialization errors while transferring the objects to the workers.


Report checkpoints and metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To persist your checkpoints and monitor training progress, add a
:class:`ray.train.huggingface.transformers.RayTrainReportCallback` utility callback to your Trainer.


.. code-block:: diff

     import transformers
     from ray.train.huggingface.transformers import RayTrainReportCallback

     def train_func(config):
         ...
         trainer = transformers.Trainer(...)
    +    trainer.add_callback(RayTrainReportCallback())
         ...


Reporting metrics and checkpoints to Ray Train ensures that you can use Ray Tune and :ref:`fault-tolerant training <train-fault-tolerance>`.
Note that the :class:`ray.train.huggingface.transformers.RayTrainReportCallback` only provides a simple implementation, and you can :ref:`further customize <train-dl-saving-checkpoints>` it.


Prepare a Transformers Trainer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, pass your Transformers Trainer into
:meth:`~ray.train.huggingface.transformers.prepare_trainer` to validate
your configurations and enable Ray Data Integration.


.. code-block:: diff

     import transformers
     import ray.train.huggingface.transformers

     def train_func(config):
         ...
         trainer = transformers.Trainer(...)
    +    trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)
         trainer.train()
         ...


.. include:: ./common/torch-configure-run.rst


Next steps
----------

After you have converted your Hugging Face Transformers training script to use Ray Train:

* See :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :ref:`Examples <train-examples>` for end-to-end examples of how to use Ray Train.
* Dive into the :ref:`API Reference <train-api>` for more details on the classes and methods used in this tutorial.


.. _transformers-trainer-migration-guide:

TransformersTrainer Migration Guide
-----------------------------------

Ray 2.1 introduced the `TransformersTrainer`, which exposes a `trainer_init_per_worker` interface
to define `transformers.Trainer`, then runs a pre-defined training function in a black box.

Ray 2.7 introduced the newly unified :class:`~ray.train.torch.TorchTrainer` API,
which offers enhanced transparency, flexibility, and simplicity. This API aligns more
with standard Hugging Face Transformers scripts, ensuring that you have better control over your
native Transformers training code.


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

            # Build a Ray TransformersTrainer
            scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
            ray_trainer = TransformersTrainer(
                trainer_init_per_worker=trainer_init_per_worker,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "evaluation": ray_eval_ds},
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
            from ray.train.huggingface.transformers import (
                RayTrainReportCallback,
                prepare_trainer,
            )
            from ray.train import ScalingConfig

            # Dataset
            def preprocess(examples):
                ...

            hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
            processed_ds = hf_datasets.map(preprocess, ...)

            ray_train_ds = ray.data.from_huggingface(processed_ds["train"])
            ray_eval_ds = ray.data.from_huggingface(processed_ds["evaluation"])

            # [1] Define the full training function
            # =====================================
            def train_func(config):
                MODEL_NAME = "gpt2"
                model_config = AutoConfig.from_pretrained(MODEL_NAME)
                model = AutoModelForCausalLM.from_config(model_config)

                # [2] Build Ray Data iterables
                # ============================
                train_dataset = ray.train.get_dataset_shard("train")
                eval_dataset = ray.train.get_dataset_shard("evaluation")

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

                # [3] Inject Ray Train Report Callback
                # ====================================
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
                datasets={"train": ray_train_ds, "evaluation": ray_eval_ds},
            )
            result = ray_trainer.fit()
