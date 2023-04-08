Scalable Offline Batch Inference
================================
:ref:`Ray Data <datasets>` offers a highly performant and scalable solution for offline batch inference and processing on large amounts of data. 

Why should I use Ray for offline batch inference?
-------------------------------------------------
1. **Faster and Cheaper for modern Deep Learning Applications**: Ray Data is built for hybrid CPU+GPU workloads. Through streaming based execution, CPU tasks like reading and preprocessing can be executed concurrently with GPU inference.
2. **Cloud, framework, and data format agnostic**. Ray Data works on any cloud provider, any ML framework (like PyTorch and Tensorflow) and does not require a particular file format.
3. **Out of the box scaling**: The same code works on 1 machine and a large cluster with no additional changes.
4. **Python first** Express your inference job in Python instead of YAML files or job specs, allowing for interactive development.

User Guides
-----------
There are 3 primary steps in offline batch inference.

1. :ref:`Creating a distributed Dataset. <creating_datasets>`
2. :ref:`Pre-processing the Dataset. <inference_preprocessing>`
3. :ref:`Model Inference <batch_inference>`



Quick Start
-----------

Tabular
~~~~~~~

.. tabbed:: Pytorch

    .. literalinclude:: doc_code/pytorch_tabular_batch_prediction.py
        :language: python

.. tabbed:: Tensorflow

    .. literalinclude:: doc_code/tf_tabular_batch_prediction.py
        :language: python

Image
~~~~~

.. tabbed:: Pytorch

    .. literalinclude:: doc_code/torch_image_batch_pretrained.py
        :language: python