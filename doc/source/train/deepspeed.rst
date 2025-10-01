.. _train-deepspeed:

Get Started with DeepSpeed
==========================

The :class:`~ray.train.torch.TorchTrainer` can help you easily launch your `DeepSpeed <https://www.deepspeed.ai/>`_ training across a distributed Ray cluster. 
DeepSpeed is an optimization library that enables efficient large-scale model training through techniques like ZeRO (Zero Redundancy Optimizer).

Benefits of Using Ray Train with DeepSpeed
------------------------------------------

- **Simplified Distributed Setup**: Ray Train handles all the distributed environment setup for you
- **Multi-Node Scaling**: Easily scale to multiple nodes with minimal code changes
- **Checkpoint Management**: Built-in checkpoint saving and loading across distributed workers
- **Seamless Integration**: Works with your existing DeepSpeed code

Code example
------------

You can use your existing DeepSpeed training code with Ray Train's TorchTrainer. The integration is minimal and preserves your familiar DeepSpeed workflow:

.. testcode::
    :skipif: True

    import deepspeed
    from deepspeed.accelerator import get_accelerator

    def train_func():
        # Instantiate your model and dataset
        model = ...
        train_dataset = ...
        eval_dataset = ...
        deepspeed_config = {...} # Your DeepSpeed config

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
        for epoch in range(num_epochs):
            # Training logic
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


Complete Examples
-----------------

Below are complete examples of ZeRO-3 training with DeepSpeed. Each example shows a full implementation of fine-tuning
 a Bidirectional Encoder Representations from Transformers (BERT) model on the Microsoft Research Paraphrase Corpus (MRPC) dataset.

Install the requirements:

.. code-block:: bash

    pip install deepspeed torch datasets transformers torchmetrics "ray[train]"

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
    like :meth:`~ray.train.torch.prepare_model` or :meth:`~ray.train.torch.prepare_data_loader` in your training function. Instead,
    keep using `deepspeed.initialize() <https://deepspeed.readthedocs.io/en/latest/initialize.html>`_ as usual to prepare everything
    for distributed training.

Run DeepSpeed with Other Frameworks
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
     - :doc:`Fine-tune GPT-J-6b with DeepSpeed and Hugging Face Transformers <examples/deepspeed/gptj_deepspeed_fine_tuning>`
   * - Lightning (:ref:`User Guide <train-pytorch-lightning>`)
     - :doc:`Fine-tune vicuna-13b with DeepSpeed and PyTorch Lightning <examples/lightning/vicuna_13b_lightning_deepspeed_finetune>`


For more information about DeepSpeed configuration options, refer to the `official DeepSpeed documentation <https://www.deepspeed.ai/docs/config-json/>`_.
