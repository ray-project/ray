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

.. _data-recipes:

Below are examples for using Ray Data for batch inference workloads with a variety of frameworks and use cases.

Beginner
--------

.. list-table::
  :widths: 1 5
  :header-rows: 1

  * - Framework
    - Example
  * - PyTorch
    - :doc:`Image Classification Batch Inference with PyTorch ResNet152 <pytorch_resnet_batch_prediction>`
  * - PyTorch
    - :doc:`Object Detection Batch Inference with PyTorch FasterRCNN_ResNet50 <batch_inference_object_detection>`
  * - Transformers
    - :doc:`Image Classification Batch Inference with Hugging Face Vision Transformer <huggingface_vit_batch_prediction>`
  * - XGBoost
    - :ref:`Tabular Data Training and Batch Inference with XGBoost <xgboost-example-ref>`
