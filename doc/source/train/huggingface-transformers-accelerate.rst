.. _train-hf-transformers-accelerate:

Training with HuggingFace Transformers and Accelerate
=====================================================

.. TODO: Remove this guide later when the other guides are ready.

TransformersTrainer
-------------------

.. TODO: Move this into its own guide when the TorchTrainer API is ready.

:class:`TransformersTrainer <ray.train.huggingface.TransformersTrainer>` further extends :class:`TorchTrainer <ray.train.torch.TorchTrainer>`, built
for interoperability with the HuggingFace Transformers library.

Users are required to provide a ``trainer_init_per_worker`` function which returns a
``transformers.Trainer`` object. The ``trainer_init_per_worker`` function
will have access to preprocessed train and evaluation datasets.

Upon calling `TransformersTrainer.fit()`, multiple workers (ray actors) will be spawned,
and each worker will create its own copy of a ``transformers.Trainer``.

Each worker will then invoke ``transformers.Trainer.train()``, which will perform distributed
training via Pytorch DDP.


.. dropdown:: Code example

    .. literalinclude:: ../doc_code/hf_trainer.py
        :language: python
        :start-after: __hf_trainer_start__
        :end-before: __hf_trainer_end__

AccelerateTrainer
-----------------

.. TODO: Move this into its own guide.

If you prefer a more fine-grained Hugging Face API than what Transformers provides, you can use :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>`
to run training functions making use of Hugging Face Accelerate. Similarly to :class:`TransformersTrainer <ray.train.huggingface.TransformersTrainer>`, :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>`
is also an extension of :class:`TorchTrainer <ray.train.torch.TorchTrainer>`.

:class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>` allows you to pass an Accelerate configuration file generated with ``accelerate config`` to be applied on all training workers.
This ensures that the worker environments are set up correctly for Accelerate, allowing you to take advantage of Accelerate APIs and integrations such as DeepSpeed and FSDP
just as you would if you were running Accelerate without Ray.

.. note::
    ``AccelerateTrainer`` will override some settings set with ``accelerate config``, mainly related to
    the topology and networking. See the :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>`
    API reference for more details.

Aside from Accelerate support, the usage is identical to :class:`TorchTrainer <ray.train.torch.TorchTrainer>`, meaning you define your own training function
and use the :func:`~ray.train.report` API to report metrics, save checkpoints etc.


.. dropdown:: Code example

    .. literalinclude:: ../doc_code/accelerate_trainer.py
        :language: python
