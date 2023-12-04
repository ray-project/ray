.. _train-deepspeed:

Get Started with DeepSpeed
==========================

The :class:`~ray.train.torch.TorchTrainer` can help you easily launch your `DeepSpeed <https://www.deepspeed.ai/>`_  training across a distributed Ray cluster.

Code example
------------

You only need to run your existing training code with a TorchTrainer. You can expect the final code to look like this:

.. testcode::
    :skipif: True

    import deepspeed
    from deepspeed.accelerator import get_accelerator

    def train_func(config):
        # Instantiate your model and dataset
        model = ...
        train_dataset = ...
        eval_dataset = ...
        deepspeed_config = {...} # Your Deepspeed config

        # Prepare everything for distributed training
        model, optimizer, train_dataloader, lr_scheduler = deepspeed.initialize(
            model=model,
            model_parameters=model.parameters(),
            training_data=tokenized_datasets["train"],
            collate_fn=collate_fn,
            config=deepspeed_config,
        )

        # Define the GPU device for the current worker
        device = get_accelerator().device_name(model.local_rank)

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


Below is a simple example of ZeRO-3 training with DeepSpeed only.

.. tab-set::

    .. tab-item:: Example with Ray Data

        .. dropdown:: Show Code

            .. literalinclude:: /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer.py
                :language: python
                :start-after: __deepspeed_torch_basic_example_start__
                :end-before: __deepspeed_torch_basic_example_end__

    .. tab-item:: Example with PyTorch DataLoader

        .. dropdown:: Show Code

            .. literalinclude:: /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer_no_raydata.py
                :language: python
                :start-after: __deepspeed_torch_basic_example_no_raydata_start__
                :end-before: __deepspeed_torch_basic_example_no_raydata_end__

.. tip::

    To run DeepSpeed with pure PyTorch, you **don't need to** provide any additional Ray Train utilities
    like :meth:`~ray.train.torch.prepare_model` or :meth:`~ray.train.torch.prepare_data_loader` in your training funciton. Instead,
    keep using `deepspeed.initialize() <https://deepspeed.readthedocs.io/en/latest/initialize.html>`_ as usual to prepare everything
    for distributed training.

Run DeepSpeed with other frameworks
-----------------------------------

Many deep learning frameworks have integrated with DeepSpeed, including Lightning, Transformers, Accelerate, and more. You can run all these combinations in Ray Train.

Check the below examples for more details:

.. list-table::
   :header-rows: 1

   * - Framework
     - Example
   * - Accelerate (:ref:`User Guide <train-hf-accelerate>`)
     - `Fine-tune Llama-2 series models with Deepspeed, Accelerate, and Ray Train. <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_
   * - Transformers (:ref:`User Guide <train-pytorch-transformers>`)
     - :ref:`Fine-tune GPT-J-6b with DeepSpeed and Hugging Face Transformers <gptj_deepspeed_finetune>`
   * - Lightning (:ref:`User Guide <train-pytorch-lightning>`)
     - :ref:`Fine-tune vicuna-13b with DeepSpeed and PyTorch Lightning <vicuna_lightning_deepspeed_finetuning>`
