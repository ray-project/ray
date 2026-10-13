---
myst:
  html_meta:
    description: "Convert a Hugging Face Transformers script to distributed training with Ray Train: TorchTrainer, checkpointing, and multi-GPU ScalingConfig."
---

(train-pytorch-transformers)=

# Get started with distributed training using Hugging Face Transformers

This tutorial shows you how to convert an existing Hugging Face Transformers script to use Ray Train for distributed training.

In this guide, you learn how to do the following:

1. Configure a {ref}`training function <train-overview-training-function>` that reports metrics and saves checkpoints.
1. Configure {ref}`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for your distributed training job.
1. Launch a distributed training job with {class}`~ray.train.torch.TorchTrainer`.


## Requirements

Before you begin, install the required packages:

```bash
pip install "ray[train]" torch "transformers[torch]" datasets evaluate numpy scikit-learn
```


## Quickstart

The final code has the following structure:

```{testcode}
:skipif: True

from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

def train_func():
    # Your Transformers training code here
    ...

scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
trainer = TorchTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```

The code has three key components:

- `train_func`: Python code that runs on each distributed training worker.
- {class}`~ray.train.ScalingConfig`: Defines the number of distributed training workers and GPU usage.
- {class}`~ray.train.torch.TorchTrainer`: Launches and manages the distributed training job.

(code-comparison-hugging-face-transformers-vs-ray-train-integration)=

## Code comparison: Hugging Face Transformers versus Ray Train integration

Compare a standard Hugging Face Transformers script with its Ray Train equivalent:

::::{tab-set}
:::{tab-item} Hugging Face Transformers with Ray Train
```{code-block} python
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
```
:::


:::{tab-item} Hugging Face Transformers
<!-- This snippet isn't tested because it doesn't use any Ray code. -->

```{testcode}
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
```
:::
::::


## Set up a training function

```{include} common/torch-configure-train_func.md
```

Ray Train sets up the distributed process group on each worker before entering the training function. Put all your logic into this function, including the following:

- Dataset construction and preprocessing
- Model initialization
- Transformers `Trainer` definition

:::{note}
When you use Hugging Face Datasets or Evaluate, always call `datasets.load_dataset` and `evaluate.load` inside the training function. Don't pass loaded datasets and metrics in from outside the training function. Doing so can cause serialization errors when the objects transfer to workers.
:::


### Report checkpoints and metrics

To persist checkpoints and monitor training progress, add a {class}`ray.train.huggingface.transformers.RayTrainReportCallback` utility callback to your Transformers `Trainer`:


```diff
 import transformers
 from ray.train.huggingface.transformers import RayTrainReportCallback

 def train_func():
     ...
     trainer = transformers.Trainer(...)
+    trainer.add_callback(RayTrainReportCallback())
     ...
```


Report metrics and checkpoints to Ray Train to integrate with Ray Tune and support {ref}`fault-tolerant training <train-fault-tolerance>`. The {class}`ray.train.huggingface.transformers.RayTrainReportCallback` provides a basic implementation. You can {ref}`customize it <train-dl-saving-checkpoints>` to fit your needs.


### Prepare a Transformers `Trainer`

Pass your Transformers `Trainer` to {meth}`~ray.train.huggingface.transformers.prepare_trainer` to validate its configurations and integrate it with Ray Data:


```diff
 import transformers
 import ray.train.huggingface.transformers

 def train_func():
     ...
     trainer = transformers.Trainer(...)
+    trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)
     trainer.train()
     ...
```


```{include} common/torch-configure-run.md
:heading-offset: 1
```


## Next steps

After you convert your Hugging Face Transformers script to use Ray Train, see the following resources:

* Explore the {ref}`user guides <train-user-guides>` to learn about specific tasks.
* Browse the {doc}`examples <examples>` for end-to-end Ray Train applications.
* See the {ref}`API reference <train-api>` for details on the classes and methods.


(transformers-trainer-migration-guide)=

## `TransformersTrainer` migration guide

Ray 2.1 introduced `TransformersTrainer`, which uses a `trainer_init_per_worker` interface to define a `transformers.Trainer` and run a predefined training function.

Ray 2.7 introduced the unified {class}`~ray.train.torch.TorchTrainer` API. It aligns more closely with standard Hugging Face Transformers scripts and gives you more control over your training code.


::::{tab-set}
:::{tab-item} Deprecated `TransformersTrainer`
<!-- This snippet isn't tested because it contains skeleton code. -->

```{testcode}
:skipif: True

import transformers
from transformers import AutoConfig, AutoModelForCausalLM
from datasets import load_dataset

import ray
from ray.train.huggingface import TransformersTrainer
from ray.train import ScalingConfig
from huggingface_hub import HfFileSystem


# Load datasets using HfFileSystem
path = "hf://datasets/Salesforce/wikitext/wikitext-2-raw-v1/"
fs = HfFileSystem()
# List the parquet files for each split
all_files = [f["name"] for f in fs.ls(path)]
train_files = [f for f in all_files if "train" in f and f.endswith(".parquet")]
validation_files = [f for f in all_files if "validation" in f and f.endswith(".parquet")]
ray_train_ds = ray.data.read_parquet(train_files, filesystem=fs)
ray_eval_ds = ray.data.read_parquet(validation_files, filesystem=fs)

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
```
:::


:::{tab-item} `TorchTrainer`
<!-- This snippet isn't tested because it contains skeleton code. -->

```{testcode}
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
from huggingface_hub import HfFileSystem


# Load datasets using HfFileSystem
path = "hf://datasets/Salesforce/wikitext/wikitext-2-raw-v1/"
fs = HfFileSystem()
# List the parquet files for each split
all_files = [f["name"] for f in fs.ls(path)]
train_files = [f for f in all_files if "train" in f and f.endswith(".parquet")]
validation_files = [f for f in all_files if "validation" in f and f.endswith(".parquet")]
ray_train_ds = ray.data.read_parquet(train_files, filesystem=fs)
ray_eval_ds = ray.data.read_parquet(validation_files, filesystem=fs)

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
```
:::
::::
