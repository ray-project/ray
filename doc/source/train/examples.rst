.. _train-examples:

Ray Train Examples
==================

.. Organize example .rst files in the same manner as the
   .py files in ray/python/ray/train/examples.

Below are examples for using Ray Train with a variety of frameworks and use cases.

Beginner
--------

.. list-table::
  :widths: 1 5
  :header-rows: 1

  * - Framework
    - Example
  * - PyTorch
    - :ref:`Train a Fashion MNIST Image Classifier with PyTorch <torch_fashion_mnist_ex>`
  * - Lightning
    - :ref:`Train an MNIST Image Classifier with Lightning <lightning_mnist_example>`
  * - Transformers
    - :ref:`Fine-tune a Text Classifier on the Yelp Reviews Dataset with Hugging Face Transformers <transformers_torch_trainer_basic_example>`
  * - Accelerate
    - :ref:`Distributed Data Parallel Training with Hugging Face Accelerate <accelerate_example>`
  * - DeepSpeed
    - :ref:`Train with DeepSpeed ZeRO-3 <deepspeed_example>`
  * - TensorFlow
    - :ref:`Train an MNIST Image Classifier with TensorFlow <tensorflow_mnist_example>`
  * - Horovod
    - :ref:`Train with Horovod and PyTorch <horovod_example>`

Intermediate
------------

.. list-table::
  :widths: 1 5
  :header-rows: 1

  * - Framework
    - Example
  * - PyTorch
    - :ref:`Fine-tune of Stable Diffusion with DreamBooth and Ray Train <torch_finetune_dreambooth_ex>`
  * - Lightning
    - :ref:`Train with PyTorch Lightning and Ray Data <lightning_advanced_example>`
  * - Transformers
    - :ref:`Fine-tune a Text Classifier on GLUE Benchmark with Hugging Face Accelerate <train_transformers_glue_example>`


Advanced
--------

.. list-table::
  :widths: 1 5
  :header-rows: 1

  * - Framework
    - Example
  * - Accelerate, DeepSpeed
    - `Fine-tune Llama-2 series models with Deepspeed, Accelerate, and Ray Train TorchTrainer <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_
  * - Transformers, DeepSpeed
    - :ref:`Fine-tune GPT-J-6B with Ray Train and DeepSpeed <gptj_deepspeed_finetune>`
  * - Lightning, DeepSpeed
    - :ref:`Fine-tune vicuna-13b with PyTorch Lightning and DeepSpeed <vicuna_lightning_deepspeed_finetuning>`
  * - Lightning
    - :ref:`Fine-tune dolly-v2-7b with PyTorch Lightning and FSDP <dolly_lightning_fsdp_finetuning>`
