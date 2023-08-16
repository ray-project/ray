.. _train-hf-accelerate:

Training with HuggingFace Accelerate
====================================

.. TODO: Remove this guide later when the other guides are ready.

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

    .. literalinclude:: ./doc_code/accelerate_trainer.py
        :language: python
