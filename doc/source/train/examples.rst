.. _train-examples:

Ray Train Examples
==================

.. Example .rst files should be organized in the same manner as the
   .py files in ray/python/ray/train/examples.

Below are examples for using Ray Train with a variety of models, frameworks,
and use cases. You can filter these examples by the following categories:

.. Beginner
.. --------

.. .. list-table::
..   :widths: 1 5
..   :header-rows: 1

..   * - Framework
..     - Example
..   * - PyTorch
..     - :ref:`Training an Fashion MNIST Image Classifier with PyTorch <torch_fashion_mnist_ex>`
..   * - Lightning
..     - :ref:`Training an MNIST Image Classifier with Lightning <lightning_mnist_example>`
..   * - Transformers
..     - :ref:`Fine-tuning a Text Classifier on Yelp Reviews Dataset with HF Transformers <transformers_torch_trainer_basic_example>`
..   * - Accelerate
..     - :ref:`Distributed Data Parallel Training with HF Accelerate <accelerate_example>`
..   * - DeepSpeed
..     - :ref:`Distributed Training with DeepSpeed ZeRO-3 <deepspeed_example>`
..   * - TensorFlow
..     - :ref:`TensorFlow MNIST Training Example <tensorflow_mnist_example>`
..   * - Horovod
..     - :ref:`End-to-end Horovod Training Example <horovod_example>`

.. Intermediate
.. ------------

.. .. list-table::
..   :widths: 1 5
..   :header-rows: 1

..   * - Framework
..     - Example
..   * - PyTorch
..     - `DreamBooth fine-tuning of Stable Diffusion with Ray Train <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning>`_
..   * - Lightning
..     - :ref:`Model Training with PyTorch Lightning and Ray Data <lightning_advanced_example>`
..   * - Accelerate
..     - :ref:`Fine-tuning a Text Classifier on GLUE Benchmark with HF Accelerate. <train_transformers_accelerate_example>`


.. Advanced
.. --------

.. .. list-table::
..   :widths: 1 5
..   :header-rows: 1

..   * - Framework
..     - Example
..   * - Accelerate, Deepspeed
..     - `Fine-tuning Llama-2 series models with Deepspeed, Accelerate, and Ray Train TorchTrainer <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_
..   * - Transformers, Deepspeed
..     - :ref:`Fine-tuning GPT-J-6B with Ray Train and DeepSpeed <gptj_deepspeed_finetune>`
..   * - Lightning, Deepspeed
..     - :ref:`Fine-tuning vicuna-13b with PyTorch Lightning and DeepSpeed <vicuna_lightning_deepspeed_finetuning>`
..   * - Lightning
..     - :ref:`Fine-tuning dolly-v2-7b with PyTorch Lightning and FSDP <dolly_lightning_fsdp_finetuning>`


.. tab-set::

    .. tab-item:: Beginner

        .. list-table::
          :widths: 1 5
          :header-rows: 1

          * - Framework
            - Example
          * - PyTorch
            - :ref:`Training an Fashion MNIST Image Classifier with PyTorch <torch_fashion_mnist_ex>`
          * - Lightning
            - :ref:`Training an MNIST Image Classifier with Lightning <lightning_mnist_example>`
          * - Transformers
            - :ref:`Fine-tuning a Text Classifier on Yelp Reviews Dataset with HF Transformers <transformers_torch_trainer_basic_example>`
          * - Accelerate
            - :ref:`Distributed Data Parallel Training with HF Accelerate <accelerate_example>`
          * - DeepSpeed
            - :ref:`Distributed Training with DeepSpeed ZeRO-3 <deepspeed_example>`
          * - TensorFlow
            - :ref:`TensorFlow MNIST Training Example <tensorflow_mnist_example>`
          * - Horovod
            - :ref:`End-to-end Horovod Training Example <horovod_example>`

    .. tab-item:: Intermediate

        .. list-table::
          :widths: 1 5
          :header-rows: 1

          * - Framework
            - Example
          * - Accelerate, Deepspeed
            - `Fine-tuning Llama-2 series models with Deepspeed, Accelerate, and Ray Train TorchTrainer <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_
          * - Transformers, Deepspeed
            - :ref:`Fine-tuning GPT-J-6B with Ray Train and DeepSpeed <gptj_deepspeed_finetune>`
          * - Lightning, Deepspeed
            - :ref:`Fine-tuning vicuna-13b with PyTorch Lightning and DeepSpeed <vicuna_lightning_deepspeed_finetuning>`
          * - Lightning
            - :ref:`Fine-tuning dolly-v2-7b with PyTorch Lightning and FSDP <dolly_lightning_fsdp_finetuning>`

    .. tab-item:: Advanced

        .. list-table::
          :widths: 1 5
          :header-rows: 1

          * - Framework
            - Example
          * - Accelerate, Deepspeed
            - `Fine-tuning Llama-2 series models with Deepspeed, Accelerate, and Ray Train TorchTrainer <https://github.com/ray-project/ray/tree/master/doc/source/templates/04_finetuning_llms_with_deepspeed>`_
          * - Transformers, Deepspeed
            - :ref:`Fine-tuning GPT-J-6B with Ray Train and DeepSpeed <gptj_deepspeed_finetune>`
          * - Lightning, Deepspeed
            - :ref:`Fine-tuning vicuna-13b with PyTorch Lightning and DeepSpeed <vicuna_lightning_deepspeed_finetuning>`
          * - Lightning
            - :ref:`Fine-tuning dolly-v2-7b with PyTorch Lightning and FSDP <dolly_lightning_fsdp_finetuning>`
