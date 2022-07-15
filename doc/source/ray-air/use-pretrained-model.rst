.. _use-pretrained-model-ref:

Use a pretrained model for batch or online inference
=====================================================

Ray AIR moves end to end machine learning workloads seamlessly through the construct of ``Checkpoint``. ``Checkpoint``
is the output of training and tuning as well as the input to downstream inference tasks.

Having said that, it is entirely possible and supported to use Ray AIR in a piecemeal fashion.

Say you already have a model trained elsewhere, you can use Ray AIR for downstream tasks such as batch and
online inference. To do that, you would need to convert the pretrained model together with any preprocessing
steps into ``Checkpoint``.

To facilitate this, we have prepared framework specific ``to_air_checkpoint`` helper function.

Examples:

.. literalinclude:: doc_code/use_pretrained_model.py
    :language: python
    :start-after: __use_pretrained_model_start__
    :end-before: __use_pretrained_model_end__
