---
myst:
  html_meta:
    description: "Launch DeepSpeed training across a Ray cluster with TorchTrainer, using ZeRO optimization stages to train large models efficiently."
---

(train-deepspeed)=

# Get started with DeepSpeed

Use {class}`~ray.train.torch.TorchTrainer` to launch your [DeepSpeed](https://www.deepspeed.ai/) training across a distributed Ray cluster. DeepSpeed is an optimization library for efficient large-scale model training. It uses techniques such as the Zero Redundancy Optimizer (ZeRO).

(benefits-of-using-ray-train-with-deepspeed)=

## Why use Ray Train with DeepSpeed?

Ray Train works with your existing DeepSpeed code and handles all the distributed environment setup for you. You can scale to multiple nodes with minimal code changes, and Ray Train provides built-in checkpoint saving and loading across distributed workers.

## Code example

Use your existing DeepSpeed training code with the Ray Train `TorchTrainer`. The integration is minimal and preserves your DeepSpeed workflow:

```{testcode}
:skipif: True

import deepspeed
import ray
from deepspeed.accelerator import get_accelerator

def train_func():
    # Instantiate your model and dataset
    model = ...
    train_dataset = ...
    eval_dataset = ...
    deepspeed_config = {...} # Your DeepSpeed config
    collate_fn = ...
    num_epochs = ...

    # Prepare everything for distributed training
    model, optimizer, train_dataloader, lr_scheduler = deepspeed.initialize(
        model=model,
        model_parameters=model.parameters(),
        training_data=train_dataset,
        collate_fn=collate_fn,
        config=deepspeed_config,
    )

    # Define the GPU device for the current worker
    device = get_accelerator().device_name(model.local_rank)

    # Start training
    for epoch in range(num_epochs):
        # Training logic that computes `loss`
        ...

        # Report metrics to Ray Train
        ray.train.report(metrics={"loss": loss})

from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(...),
    # If running in a multi-node cluster, this is where you
    # should configure the run's persistent storage that is accessible
    # across all worker nodes.
    # run_config=ray.train.RunConfig(storage_path="s3://..."),
    ...
)
result = trainer.fit()
```


## Complete examples

The following examples show ZeRO-3 training with DeepSpeed. Each example is a full implementation that fine-tunes a Bidirectional Encoder Representations from Transformers (BERT) model on the Microsoft Research Paraphrase Corpus (MRPC) dataset.

Install the requirements:

```bash
pip install deepspeed torch datasets transformers torchmetrics "ray[train]"
```

:::::{tab-set}
::::{tab-item} Example with Ray Data
:::{dropdown} Show code
```{literalinclude} /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer.py
:language: python
:start-after: __deepspeed_torch_basic_example_start__
:end-before: __deepspeed_torch_basic_example_end__
```
:::
::::

::::{tab-item} Example with PyTorch DataLoader
:::{dropdown} Show code
```{literalinclude} /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer_no_raydata.py
:language: python
:start-after: __deepspeed_torch_basic_example_no_raydata_start__
:end-before: __deepspeed_torch_basic_example_no_raydata_end__
```
:::
::::
:::::

:::{tip}
To run DeepSpeed with pure PyTorch, you don't need additional Ray Train utilities such as {meth}`~ray.train.torch.prepare_model` or {meth}`~ray.train.torch.prepare_data_loader` in your training function. Keep using [`deepspeed.initialize()`](https://deepspeed.readthedocs.io/en/latest/initialize.html) to prepare everything for distributed training.
:::


## Fine-tune LLMs with DeepSpeed

For a step-by-step guide to fine-tuning large language models (LLMs) with Ray Train and DeepSpeed, see {doc}`Fine-tune an LLM with Ray Train and DeepSpeed </_collections/train/examples/pytorch/deepspeed_finetune/README>`.


## Run DeepSpeed with other frameworks

Many deep learning frameworks integrate with DeepSpeed, including Lightning, Transformers, and Accelerate. You can run all these combinations in Ray Train.

The following table lists a user guide and an example for each framework.

```{list-table}
:header-rows: 1

* - Framework
  - User guide
  - Example
* - Accelerate
  - {ref}`User guide <train-hf-accelerate>`
  - [Fine-tune Llama-2 series models with DeepSpeed, Accelerate, and Ray Train](https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed)
* - Transformers
  - {ref}`User guide <train-pytorch-transformers>`
  - {doc}`Fine-tune GPT-J-6b with DeepSpeed and Hugging Face Transformers <examples/deepspeed/gptj-deepspeed-fine-tuning>`
* - Lightning
  - {ref}`User guide <train-pytorch-lightning>`
  - {doc}`Fine-tune vicuna-13b with DeepSpeed and PyTorch Lightning <examples/lightning/vicuna-13b-lightning-deepspeed-finetune>`
```


For DeepSpeed configuration options, see the [DeepSpeed documentation](https://www.deepspeed.ai/docs/config-json/).
