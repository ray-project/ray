.. _train-hf-accelerate:

Get Started with Hugging Face Accelerate
========================================

The :class:`~ray.train.torch.TorchTrainer` can help you easily launch your `Accelerate <https://huggingface.co/docs/accelerate>`_  training across a distributed Ray cluster.

You only need to run your existing training code with a TorchTrainer. You can expect the final code to look like this:

.. testcode::
    :skipif: True

    from accelerate import Accelerator

    def train_func(config):
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
        ...
    )
    trainer.fit()

.. tip::

    Model and data preparation for distributed training is completely handled by the `Accelerator <https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator>`_
    object and its `Accelerator.prepare() <https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator.prepare>`_  method.

    Unlike with native PyTorch, PyTorch Lightning, or Hugging Face Transformers, **don't** call any additional Ray Train utilities
    like :meth:`~ray.train.torch.prepare_model` or :meth:`~ray.train.torch.prepare_data_loader` in your training function.

Configure Accelerate
--------------------

In Ray Train, you can set configurations through the `accelerate.Accelerator <https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator>`_
object in your training function. Below are starter examples for configuring Accelerate.

.. tab-set::

    .. tab-item:: DeepSpeed

        For example, to run DeepSpeed with Accelerate, create a `DeepSpeedPlugin <https://huggingface.co/docs/accelerate/main/en/package_reference/deepspeed>`_
        from a dictionary:

        .. testcode::
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

            def train_func(config):
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
                ...
            )
            trainer.fit()

    .. tab-item:: FSDP
        :sync: FSDP

        For PyTorch FSDP, create a `FullyShardedDataParallelPlugin <https://huggingface.co/docs/accelerate/main/en/package_reference/fsdp>`_
        and pass it to the Accelerator.

        .. testcode::
            :skipif: True

            from torch.distributed.fsdp.fully_sharded_data_parallel import FullOptimStateDictConfig, FullStateDictConfig
            from accelerate import Accelerator, FullyShardedDataParallelPlugin

            def train_func(config):
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
                ...
            )
            trainer.fit()

Note that Accelerate also provides a CLI tool, `"accelerate config"`, to generate a configuration and launch your training
job with `"accelerate launch"`. However, it's not necessary here because Ray's `TorchTrainer` already sets up the Torch
distributed environment and launches the training function on all workers.


Next, see these end-to-end examples below for more details:

.. tab-set::

    .. tab-item:: Example with Ray Data

        .. dropdown:: Show Code

            .. literalinclude:: /../../python/ray/train/examples/accelerate/accelerate_torch_trainer.py
                :language: python
                :start-after: __accelerate_torch_basic_example_start__
                :end-before: __accelerate_torch_basic_example_end__

    .. tab-item:: Example with PyTorch DataLoader

        .. dropdown:: Show Code

            .. literalinclude:: /../../python/ray/train/examples/accelerate/accelerate_torch_trainer_no_raydata.py
                :language: python
                :start-after: __accelerate_torch_basic_example_no_raydata_start__
                :end-before: __accelerate_torch_basic_example_no_raydata_end__

.. seealso::

    If you're looking for more advanced use cases, check out this Llama-2 fine-tuning example:

    - `Fine-tuning Llama-2 series models with Deepspeed, Accelerate, and Ray Train. <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_

You may also find these user guides helpful:

- :ref:`Configuring Scale and GPUs <train_scaling_config>`
- :ref:`Configuration and Persistent Storage <train-run-config>`
- :ref:`Saving and Loading Checkpoints <train-checkpointing>`
- :ref:`How to use Ray Data with Ray Train <data-ingest-torch>`


AccelerateTrainer Migration Guide
---------------------------------

Before Ray 2.7, Ray Train's `AccelerateTrainer` API was the
recommended way to run Accelerate code. As a subclass of :class:`TorchTrainer <ray.train.torch.TorchTrainer>`,
the AccelerateTrainer takes in a configuration file generated by ``accelerate config`` and applies it to all workers.
Aside from that, the functionality of ``AccelerateTrainer`` is identical to ``TorchTrainer``.

However, this caused confusion around whether this was the *only* way to run Accelerate code.
Because you can express the full Accelerate functionality with the ``Accelerator`` and ``TorchTrainer`` combination, the plan is to deprecate the ``AccelerateTrainer`` in Ray 2.8,
and it's recommend to run your  Accelerate code directly with ``TorchTrainer``.
