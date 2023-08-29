.. include:: /_includes/train/announcement.rst

.. _train-docs:

Ray Train: Scalable Model Training
==================================

Ray Train is a scalable model training library for distributed training and fine-tuning.

.. hidden:: 
    
    image:: ./images/train-concepts.svg
        
.. visible::

Ray Train abstracts away the complexities of distributed computing.
It allows you to use the same code to train your model on either a single node or multiple nodes.
Configure resources with a single line.
Whether you have large models or large datasets, Ray Train is the simplest solution for distributed training.

Ray Train provides support for many frameworks:

* PyTorch-based frameworks: PyTorch, PyTorch Lightning, Hugging Face Transformers, Hugging Face Accelerate, DeepSpeed
* Other deep learning frameworks: TensorFlow, Keras, Horovod
* Tree-based frameworks: XGBoost, LightGBM

Install Ray Train
-----------------

To install Ray Train, run:

.. code-block:: console

    $ pip install -U "ray[train]"

To learn more about installing Ray and its libraries, see
:ref:`Installing Ray <installation>`.

Learn more
----------

General
~~~~~~~

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-6

    .. grid-item-card::

        **Overview**
        ^^^

        Understand the key concepts for distributed training with Ray Train.

        +++
        .. button-ref:: train-overview
            :color: primary
            :outline:
            :expand:

            Learn the basics

    .. grid-item-card::

        **User Guides**
        ^^^

        Learn how to use Ray Train effectively and efficiently for your use case.

        +++
        .. button-ref:: train-user-guides
            :color: primary
            :outline:
            :expand:

            Learn how to use Ray Train

    .. grid-item-card::

        **Examples**
        ^^^

        Find examples for different frameworks.

        +++
        .. button-ref:: train-examples
            :color: primary
            :outline:
            :expand:

            See examples

    .. grid-item-card::

        **API**
        ^^^

        Get more in-depth information about the Ray Train API.

        +++
        .. button-ref:: air-trainer-ref
            :color: primary
            :outline:
            :expand:

            Read the API Reference

Get started
~~~~~~~~~~~

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-6

    .. grid-item-card::

        **PyTorch**
        ^^^

        Get started on distributing your model training with Ray Train and PyTorch.

        +++
        .. button-ref:: train-pytorch
            :color: primary
            :outline:
            :expand:

            Try Ray Train with PyTorch

    .. grid-item-card::

        **PyTorch Lightning**
        ^^^

        Get started on distributing your model training with Ray Train and Lightning.

        +++
        .. button-ref:: train-pytorch-lightning
            :color: primary
            :outline:
            :expand:

            Try Ray Train and Lightning

    .. grid-item-card::

        **Hugging Face Transformers**
        ^^^

        Get started on distributing your model training with Ray Train and Transformers.

        +++
        .. button-ref:: train-pytorch-transformers
            :color: primary
            :outline:
            :expand:

            Try Ray Train with Transformers

    .. grid-item-card::

        **More Frameworks**
        ^^^

        Don't see your framework? See these guides.

        +++
        .. button-ref:: train-more-frameworks
            :color: primary
            :outline:
            :expand:

            Try Ray Train with other frameworks

.. include:: /_includes/train/announcement_bottom.rst
