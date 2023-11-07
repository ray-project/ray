.. _data-examples-ref:

=================
Ray Data Examples
=================

..
   Include all examples in a hidden toctree so Sphinx build does not complain.

.. toctree::
    :hidden:

    huggingface_vit_batch_prediction
    pytorch_resnet_batch_prediction
    batch_inference_object_detection
    batch_training

.. _data-recipes:

Batch inference
---------------

.. grid:: 1 2 2 3
    :gutter: 1
    :class-container: container pb-4

    .. grid-item-card::

       .. button-ref:: huggingface_vit_batch_prediction

            Image Classification Batch Inference with Huggingface Vision Transformer


    .. grid-item-card::

       .. button-ref:: pytorch_resnet_batch_prediction

            Image Classification Batch Inference with PyTorch ResNet152


    .. grid-item-card::

        .. button-ref:: batch_inference_object_detection

            Object Detection Batch Inference with PyTorch FasterRCNN_ResNet50

Many model training
-------------------

.. grid:: 1 2 2 3
    :gutter: 1
    :class-container: container pb-4

    .. grid-item-card::

        .. button-ref:: batch_training

            Many Model Training with Ray Data
