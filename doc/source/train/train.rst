.. include:: /_includes/train/announcement.rst

.. _train-docs:

Ray Train: Scalable Model Training
==================================

Ray Train is a scalable model training library for distributed training and fine-tuning.

Ray Train provides support for many frameworks:

* PyTorch trainers: PyTorch, PyTorch Lightning, HuggingFace Transformers, HuggingFace Accelerate
* Other deep learning libraries: TensorFlow and Keras
* Tree-based trainers: XGBoost, LightGBM

Install Ray Train
-----------------

To install Ray Train, run:

.. code-block:: console

    $ pip install -U "ray[train]"

To learn more about installing Ray and its libraries, see
:ref:`Installing Ray <installation>`.

Learn more
----------

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

        **Distributed PyTorch**
        ^^^

        Get started on distributing your model training with Ray Train and PyTorch.

        +++
        .. button-ref:: train-pytorch
            :color: primary
            :outline:
            :expand:

            Try Ray Train with PyTorch

    .. grid-item-card::

        **Distributed Lightning**
        ^^^

        Get started on distributing your model training with Ray Train and Lightning.

        +++
        .. button-ref:: train-pytorch-lightning
            :color: primary
            :outline:
            :expand:

            Try Ray Train with Lightning

    .. grid-item-card::

        **Distributed Transformers**
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

    .. grid-item-card::

        **Examples**
        ^^^

        Find both single-worker and scaling-out examples.

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

.. include:: /_includes/train/announcement_bottom.rst
