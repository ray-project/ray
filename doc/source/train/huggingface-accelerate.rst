.. _train-hf-accelerate:

Training with HuggingFace Accelerate
====================================

Ray :class:`~ray.train.torch.TorchTrainer` can help you easily distribute your accelerated training over a Ray Cluster.
Simply put all your existing training logics into a training function, then launch a TorchTrainer. 

You can expect the final code to look like this:

.. code-block:: python

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

    Unlike native PyTorch, Lightning or Transformers, you **don't need to** provide any additional Ray Train utilities 
    like :meth:`~ray.train.torch.prepare_model` or :meth:`~ray.train.torch.prepare_data_loader` in your training funciton. Instead, 
    you can just instantiate an `Accelerator <https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator>`_ 
    and call `Accelerator.prepare() <https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator.prepare>`_ 
    as usual to prepare everything for distributed training.

How to setup Accelerate config
------------------------------

Hugging Face Accelerate provides two approaches to configure your run:

1. Use `accelerate config` to generate a configuration YAML file, and use `accelerate launch $YOUR_PY_SCRIPT --config_file=$YOUR_CONFIG_YAML` to launch via CLI. 
2. Set configurations through the `accelerate.Accelerator <https://huggingface.co/docs/accelerate/main/en/package_reference/accelerator#accelerate.Accelerator>`_ 
object in your Python script, then run your script with custom launcher.

In Ray Train, you should always use the second approach, where Ray TorchTrainer acts as a distributed launcher and automatically sets up 
the Torch distributed environment and run thr training function on all workers.


.. tabs::

    .. group-tab:: DeepSpeed

        For example, to run DeepSpeed with Accelerate, create a `DeepSpeedPlugin <https://huggingface.co/docs/accelerate/main/en/package_reference/deepspeed>`_ 
        from a dictionary:

        .. code-block:: python

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

    .. group-tab:: FSDP

        For PyTorch FSDP, create a `FullyShardedDataParallelPlugin <https://huggingface.co/docs/accelerate/main/en/package_reference/fsdp>`_ 
        and pass it to the Accelerator.

        .. code-block:: python

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

Next, check these end-to-end examples below for more details:

.. dropdown:: End-to-end Code Example

    .. literalinclude:: /../../python/ray/train/examples/accelerate/accelerate_torch_trainer.py
        :language: python

.. seealso::

    If you're looking for more advanced use cases, check out this Llama-2 fine-tuning example: 
    
    `Fine-tuning Llama-2 series models with Deepspeed, Accelerate, and Ray Train. <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_

You may also find these user guides helpful:

- :ref:`Configuring Scale and GPUs <train_scaling_config>`
- :ref:`Configuration and Persistent Storage <train-run-config>`
- :ref:`Saving and Loading Checkpoints <train-checkpointing>`
- :ref:`How to use Ray Data with Ray Train <data-ingest-torch>`


`AccelerateTrainer` Migration Guide 
-----------------------------------

Before Ray 2.7, Ray Train's :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>` API was the 
recommended way to run Accelerate code. As a subclass of :class:`TorchTrainer <ray.train.torch.TorchTrainer>`,  
AccelerateTrainer helps users apply the configuration file generated with ``accelerate config`` to all workers. 
Aside from that, the functionality of ``AccelerateTrainer`` is identical to ``TorchTrainer``.

However, users can easily configure Accelerate within their training function, so there is no need to maintain a separate Ray Trainer API.
To provide a more simplified and flexible interface, we will deprecate ``AccelerateTrainer`` in Ray 2.8, and recommend running your 
Accelerate code with ``TorchTrainer``. 


