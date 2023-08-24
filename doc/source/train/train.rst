.. include:: /_includes/train/announcement.rst

.. _train-docs:

Ray Train: Scalable Model Training
==================================

Ray Train is a scalable model training library for distributed training and fine-tuning on a cluster.
Ray Train simplifies scaling by abstracting away the details of distributed computing.
Model training on a single node or multiple nodes, uses the same training code, making Ray Train the easiest way to train large models with large datasets.
Ray Train is one of the Ray AI Libraries, a suite of libraries built on Ray, for scaling machine learning.

Ray Train provides Pythonic wrapper classes for many frameworks:

* PyTorch trainers: PyTorch, PyTorch Lightning, HuggingFace Transformers, HuggingFace Accelerate
* Other deep learning libraries: Tensorflow and Keras
* Tree-based trainers: XGboost, LightGBM

Ray Train supports the common methods and tooling for deep learning pipelines:

* Callbacks
* Checkpointing
* Integration with TensorBoard, Weights and Biases, and MLflow
* Data loading from sources like S3

Ray Train works seamlessly with other Ray AI Libraries, allowing you to compose complex Machine Learning Operations (MLOps) pipelines from data loading and preprocessing to model serving and deploying:

* Use :ref:`Ray Data <data>` with Train to load and process datasets both small and large.
* Use :ref:`Ray Tune <tune-main>` with Train to sweep parameter grids and leverage cutting edge hyperparameter search algorithms.
* Leverage the :ref:`Ray cluster launcher <cluster-index>` to launch autoscaling or spot instance clusters on any cloud.

Install Ray Train
-----------------

To install Ray Train, run:

.. code-block:: console

    $ pip install -U 'ray[train]'

To learn more about installing Ray and its libraries, see
:ref:`Installing Ray <installation>`.

Learn more
----------

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-6

    .. grid-item-card::

        **Ray Train Overview**
        ^^^

        Understand the key concepts for distributed training.

        +++
        .. button-ref:: train-overview
            :color: primary
            :outline:
            :expand:

            Learn the basics

    .. grid-item-card::

        **Get Started**
        ^^^

        Start with the guide for the framework you are using.

        +++
        .. button-ref:: train-pytorch
            :color: primary
            :outline:
            :expand:

            Distribute training with PyTorch
    .. grid-item-card::

        **User Guides**
        ^^^

        Learn how to use Ray Train, from basic usage to end-to-end guides.

        +++
        .. button-ref:: train-user-guides
            :color: primary
            :outline:
            :expand:

            Learn how to use Ray Train

    .. grid-item-card::

        **Examples**
        ^^^

        Find both simple and scaling-out examples of using Ray Train.

        +++
        .. button-ref:: train-examples
            :color: primary
            :outline:
            :expand:

            Ray Train Examples

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

Next steps
----------

* :ref:`Overview for Ray Train <train-key-concepts>`
* :doc:`PyTorch Guide </train/getting-started-pytorch>`
* :doc:`TensorFlow Guide </train/distributed-tensorflow-keras>`
* :doc:`Tree-Based Trainer Guide </train/distributed-xgboost-lightgbm>`

.. include:: /_includes/train/announcement_bottom.rst
