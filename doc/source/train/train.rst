.. _train-docs:

Ray Train: Scalable Model Training
==================================

.. toctree::
    :hidden:

    Overview <overview>
    PyTorch Guide <getting-started-pytorch>
    PyTorch Lightning Guide <getting-started-pytorch-lightning>
    Hugging Face Transformers Guide <getting-started-transformers>
    more-frameworks
    User Guides <user-guides>
    Examples <examples>
    Benchmarks <benchmarks>
    api/api


.. div:: sd-d-flex-row sd-align-major-center sd-align-minor-center

    .. div:: sd-w-50

        .. raw:: html
           :file: images/logo.svg


Ray Train is a scalable machine learning library for distributed training and fine-tuning.

Ray Train allows you to scale model training code from a single machine to a cluster of machines in the cloud, and abstracts away the complexities of distributed computing.
Whether you have large models or large datasets, Ray Train is the simplest solution for distributed training.

Ray Train provides support for many frameworks:

.. list-table::
   :widths: 1 1
   :header-rows: 1

   * - PyTorch Ecosystem
     - More Frameworks
   * - PyTorch
     - TensorFlow
   * - PyTorch Lightning
     - Keras
   * - Hugging Face Transformers
     - Horovod
   * - Hugging Face Accelerate
     - XGBoost
   * - DeepSpeed
     - LightGBM

Install Ray Train
-----------------

To install Ray Train, run:

.. code-block:: console

    $ pip install -U "ray[train]"

To learn more about installing Ray and its libraries, see
:ref:`Installing Ray <installation>`.

Get started
-----------

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

        **PyTorch**
        ^^^

        Get started on distributed model training with Ray Train and PyTorch.

        +++
        .. button-ref:: train-pytorch
            :color: primary
            :outline:
            :expand:

            Try Ray Train with PyTorch

    .. grid-item-card::

        **PyTorch Lightning**
        ^^^

        Get started on distributed model training with Ray Train and Lightning.

        +++
        .. button-ref:: train-pytorch-lightning
            :color: primary
            :outline:
            :expand:

            Try Ray Train with Lightning

    .. grid-item-card::

        **Hugging Face Transformers**
        ^^^

        Get started on distributed model training with Ray Train and Transformers.

        +++
        .. button-ref:: train-pytorch-transformers
            :color: primary
            :outline:
            :expand:

            Try Ray Train with Transformers

Learn more
----------

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-6

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

        **User Guides**
        ^^^

        Get how-to instructions for common training tasks with Ray Train.

        +++
        .. button-ref:: train-user-guides
            :color: primary
            :outline:
            :expand:

            Read how-to guides

    .. grid-item-card::

        **Examples**
        ^^^

        Browse end-to-end code examples for different use cases.

        +++
        .. button-ref:: train-examples
            :color: primary
            :outline:
            :expand:

            Learn through examples

    .. grid-item-card::

        **API**
        ^^^

        Consult the API Reference for full descriptions of the Ray Train API.

        +++
        .. button-ref:: train-api
            :color: primary
            :outline:
            :expand:

            Read the API Reference
