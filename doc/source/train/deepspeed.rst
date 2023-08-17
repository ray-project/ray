.. _train-deepspeed:

Training with DeepSpeed
=======================

Ray :class:`~ray.train.torch.TorchTrainer` can help you easily distribute your DeepSpeed training over a Ray Cluster.
Simply put all your existing training logics into a training function, then launch a TorchTrainer.

To run DeepSpeed with pure PyTorch, you **don't need to** provide any additional Ray Train utilities 
like :meth:`~ray.train.torch.prepare_model` or :meth:`~ray.train.torch.prepare_data_loader` in your training funciton. Instead, 
keep using `deepspeed.initialize() <https://deepspeed.readthedocs.io/en/latest/initialize.html>`_ as usual to prepare everything 
for distributed training.

Below is a simple example of ZeRO-3 training with DeepSpeed only. 

.. dropdown:: Code example

    .. literalinclude:: /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer.py
        :language: python
        :start-after: __deepspeed_torch_basic_example_start__
        :end-before: __deepspeed_torch_basic_example_end__


Run DeepSpeed with More Frameworks
----------------------------------

Many deep learning frameworks have integrated with DeepSpeed, including Lightning, Transformers, Accelerate, and more. You can run all these combinations in Ray Train.

Please check the below examples for more details:

- DeepSpeed + Accelerate: `Fine-tune Llama-2 series models with Deepspeed, Accelerate, and Ray Train. <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_
- DeepSpeed + Transformers: :ref:`Fine-tune GPT-J-6b with DeepSpeed and Hugging Face Transformers <gptj_deepspeed_finetune>`
- DeepSpeed + Lightning: :ref:`Fine-tune vicuna-13b with DeepSpeed and PyTorch Lightning <vicuna_lightning_deepspeed_finetuning>`
