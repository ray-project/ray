---
myst:
  html_meta:
    description: "Run Hugging Face Accelerate training on Ray Train, including Accelerate configuration and migration off AccelerateTrainer."
---

(train-hf-accelerate)=

# Get started with distributed training using Hugging Face Accelerate

Use the {class}`~ray.train.torch.TorchTrainer` to launch your [Accelerate](https://huggingface.co/docs/accelerate) training across a distributed Ray cluster.

You only need to run your existing training code with a `TorchTrainer`. The final code looks similar to the following:

```{testcode}
:skipif: True

from accelerate import Accelerator

def train_func():
    # Instantiate the accelerator
    accelerator = Accelerator(...)

    model = ...
    optimizer = ...
    train_dataloader = ...
    eval_dataloader = ...
    lr_scheduler = ...

    # Prepare everything for distributed training
    (
        model,
        optimizer,
        train_dataloader,
        eval_dataloader,
        lr_scheduler,
    ) = accelerator.prepare(
        model, optimizer, train_dataloader, eval_dataloader, lr_scheduler
    )

    # Start training
    ...

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
trainer.fit()
```

:::{tip}
The [`Accelerator`](https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator) object and its [`Accelerator.prepare()`](https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator.prepare) method handle all model and data preparation for distributed training.

Unlike with native PyTorch, don't call any additional Ray Train utilities, such as {meth}`~ray.train.torch.prepare_model` or {meth}`~ray.train.torch.prepare_data_loader`, in your training function.
:::

## Configure Accelerate

In Ray Train, set Accelerate configurations through the [`accelerate.Accelerator`](https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator) object in your training function. The following tabs show starter configurations.

::::{tab-set}
:::{tab-item} DeepSpeed
To run DeepSpeed with Accelerate, create a [`DeepSpeedPlugin`](https://huggingface.co/docs/accelerate/main/en/package_reference/deepspeed) from a dictionary:

```{testcode}
:skipif: True

from accelerate import Accelerator, DeepSpeedPlugin

DEEPSPEED_CONFIG = {
    "fp16": {
        "enabled": True
    },
    "zero_optimization": {
        "stage": 3,
        "offload_optimizer": {
            "device": "cpu",
            "pin_memory": False
        },
        "overlap_comm": True,
        "contiguous_gradients": True,
        "reduce_bucket_size": "auto",
        "stage3_prefetch_bucket_size": "auto",
        "stage3_param_persistence_threshold": "auto",
        "gather_16bit_weights_on_model_save": True,
        "round_robin_gradients": True
    },
    "gradient_accumulation_steps": "auto",
    "gradient_clipping": "auto",
    "steps_per_print": 10,
    "train_batch_size": "auto",
    "train_micro_batch_size_per_gpu": "auto",
    "wall_clock_breakdown": False
}

def train_func():
    # Create a DeepSpeedPlugin from config dict
    ds_plugin = DeepSpeedPlugin(hf_ds_config=DEEPSPEED_CONFIG)

    # Initialize Accelerator
    accelerator = Accelerator(
        ...,
        deepspeed_plugin=ds_plugin,
    )

    # Start training
    ...

from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(...),
    run_config=ray.train.RunConfig(storage_path="s3://..."),
    ...
)
trainer.fit()
```
:::

:::{tab-item} FSDP
:sync: FSDP
For PyTorch Fully Sharded Data Parallel (FSDP), create a [`FullyShardedDataParallelPlugin`](https://huggingface.co/docs/accelerate/main/en/package_reference/fsdp) and pass it to the `Accelerator`.

```{testcode}
:skipif: True

from torch.distributed.fsdp.fully_sharded_data_parallel import FullOptimStateDictConfig, FullStateDictConfig
from accelerate import Accelerator, FullyShardedDataParallelPlugin

def train_func():
    fsdp_plugin = FullyShardedDataParallelPlugin(
        state_dict_config=FullStateDictConfig(
            offload_to_cpu=False,
            rank0_only=False
        ),
        optim_state_dict_config=FullOptimStateDictConfig(
            offload_to_cpu=False,
            rank0_only=False
        )
    )

    # Initialize accelerator
    accelerator = Accelerator(
        ...,
        fsdp_plugin=fsdp_plugin,
    )

    # Start training
    ...

from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(...),
    run_config=ray.train.RunConfig(storage_path="s3://..."),
    ...
)
trainer.fit()
```
:::
::::

Accelerate also provides the `accelerate config` CLI command to generate a configuration and the `accelerate launch` command to launch your training job. You don't need either with Ray Train, because `TorchTrainer` already sets up the Torch distributed environment and launches the training function on all workers.

For details, see the following end-to-end examples:

:::::{tab-set}
::::{tab-item} Example with Ray Data
:::{dropdown} Show code
```{literalinclude} /../../python/ray/train/examples/accelerate/accelerate_torch_trainer.py
:language: python
:start-after: __accelerate_torch_basic_example_start__
:end-before: __accelerate_torch_basic_example_end__
```
:::
::::

::::{tab-item} Example with PyTorch DataLoader
:::{dropdown} Show code
```{literalinclude} /../../python/ray/train/examples/accelerate/accelerate_torch_trainer_no_raydata.py
:language: python
:start-after: __accelerate_torch_basic_example_no_raydata_start__
:end-before: __accelerate_torch_basic_example_no_raydata_end__
```
:::
::::
:::::

:::{seealso}
For more advanced use cases, see the following Llama-2 fine-tuning example:

- [Fine-tuning Llama-2 series models with DeepSpeed, Accelerate, and Ray Train](https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed)
:::

The following user guides might also help:

- {ref}`train_scaling_config`
- {ref}`persistent-storage-guide`
- {ref}`train-checkpointing`
- {ref}`How to use Ray Data with Ray Train <data-ingest-torch>`

## `AccelerateTrainer` migration guide

Before Ray 2.7, Ray Train's `AccelerateTrainer` API was the recommended way to run Accelerate code. `AccelerateTrainer` is a subclass of {class}`TorchTrainer <ray.train.torch.TorchTrainer>` that takes a configuration file generated by `accelerate config` and applies it to all workers. Otherwise, `AccelerateTrainer` behaves identically to `TorchTrainer`.

However, this API caused confusion about whether it was the *only* way to run Accelerate code. Because you can use all Accelerate features with the combination of `Accelerator` and `TorchTrainer`, the plan is to deprecate `AccelerateTrainer` in Ray 2.8. Run your Accelerate code directly with `TorchTrainer`.
