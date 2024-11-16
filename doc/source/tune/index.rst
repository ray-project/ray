.. _tune-main:

Ray Tune: Hyperparameter Tuning
===============================

.. toctree::
    :hidden:

    Getting Started <getting-started>
    Key Concepts <key-concepts>
    tutorials/overview
    examples/index
    faq
    api/api

.. image:: images/tune_overview.png
    :scale: 50%
    :align: center

Tune is a Python library for experiment execution and hyperparameter tuning at any scale.
You can tune your favorite machine learning framework (:ref:`PyTorch <tune-pytorch-cifar-ref>`, :ref:`XGBoost <tune-xgboost-ref>`, :doc:`TensorFlow and Keras <examples/tune_mnist_keras>`, and :doc:`more <examples/index>`) by running state of the art algorithms such as :ref:`Population Based Training (PBT) <tune-scheduler-pbt>` and :ref:`HyperBand/ASHA <tune-scheduler-hyperband>`.
Tune further integrates with a wide range of additional hyperparameter optimization tools, including :doc:`Ax <examples/ax_example>`, :doc:`BayesOpt <examples/bayesopt_example>`, :doc:`BOHB <examples/bohb_example>`, :doc:`Nevergrad <examples/nevergrad_example>`, and :doc:`Optuna <examples/optuna_example>`.

**Click on the following tabs to see code examples for various machine learning frameworks**:

.. tab-set::

    .. tab-item:: Quickstart

        To run this example, install the following: ``pip install "ray[tune]"``.

        In this quick-start example you `minimize` a simple function of the form ``f(x) = a**2 + b``, our `objective` function.
        The closer ``a`` is to zero and the smaller ``b`` is, the smaller the total value of ``f(x)``.
        We will define a so-called `search space` for  ``a`` and ``b`` and let Ray Tune explore the space for good values.

        .. callout::

            .. literalinclude:: ../../../python/ray/tune/tests/example.py
               :language: python
               :start-after: __quick_start_begin__
               :end-before: __quick_start_end__

            .. annotations::
                <1> Define an objective function.

                <2> Define a search space.

                <3> Start a Tune run and print the best result.


    .. tab-item:: Keras+Hyperopt

        To tune your Keras models with Hyperopt, you wrap your model in an objective function whose ``config`` you
        can access for selecting hyperparameters.
        In the example below we only tune the ``activation`` parameter of the first layer of the model, but you can
        tune any parameter of the model you want.
        After defining the search space, you can simply initialize the ``HyperOptSearch`` object and pass it to ``run``.
        It's important to tell Ray Tune which metric you want to optimize and whether you want to maximize or minimize it.

        .. callout::

            .. literalinclude:: doc_code/keras_hyperopt.py
                :language: python
                :start-after: __keras_hyperopt_start__
                :end-before: __keras_hyperopt_end__

            .. annotations::
                <1> Wrap a Keras model in an objective function.

                <2> Define a search space and initialize the search algorithm.

                <3> Start a Tune run that maximizes accuracy.

    .. tab-item:: PyTorch+Optuna

        To tune your PyTorch models with Optuna, you wrap your model in an objective function whose ``config`` you
        can access for selecting hyperparameters.
        In the example below we only tune the ``momentum`` and learning rate (``lr``) parameters of the model's optimizer,
        but you can tune any other model parameter you want.
        After defining the search space, you can simply initialize the ``OptunaSearch`` object and pass it to ``run``.
        It's important to tell Ray Tune which metric you want to optimize and whether you want to maximize or minimize it.
        We stop tuning this training run after ``5`` iterations, but you can easily define other stopping rules as well.


        .. callout::

            .. literalinclude:: doc_code/pytorch_optuna.py
                :language: python
                :start-after: __pytorch_optuna_start__
                :end-before: __pytorch_optuna_end__

            .. annotations::
                <1> Wrap a PyTorch model in an objective function.

                <2> Define a search space and initialize the search algorithm.

                <3> Start a Tune run that maximizes mean accuracy and stops after 5 iterations.

With Tune you can also launch a multi-node :ref:`distributed hyperparameter sweep <tune-distributed-ref>`
in less than 10 lines of code.
And you can move your models from training to serving on the same infrastructure with `Ray Serve`_.

.. _`Ray Serve`: ../serve/index.html


.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::

        **Getting Started**
        ^^^

        In our getting started tutorial you will learn how to tune a PyTorch model
        effectively with Tune.

        +++
        .. button-ref:: tune-tutorial
            :color: primary
            :outline:
            :expand:

            Get Started with Tune

    .. grid-item-card::

        **Key Concepts**
        ^^^

        Understand the key concepts behind Ray Tune.
        Learn about tune runs, search algorithms, schedulers and other features.

        +++
        .. button-ref:: tune-60-seconds
            :color: primary
            :outline:
            :expand:

            Tune's Key Concepts

    .. grid-item-card::

        **User Guides**
        ^^^

        Our guides teach you about key features of Tune,
        such as distributed training or early stopping.


        +++
        .. button-ref:: tune-guides
            :color: primary
            :outline:
            :expand:

            Learn How To Use Tune

    .. grid-item-card::

        **Examples**
        ^^^

        In our examples you can find practical tutorials for using frameworks such as
        scikit-learn, Keras, TensorFlow, PyTorch, and mlflow, and state of the art search algorithm integrations.

        +++
        .. button-ref::  tune-examples-ref
            :color: primary
            :outline:
            :expand:

            Ray Tune Examples

    .. grid-item-card::

        **Ray Tune FAQ**
        ^^^

        Find answers to commonly asked questions in our detailed FAQ.

        +++
        .. button-ref:: tune-faq
            :color: primary
            :outline:
            :expand:

            Ray Tune FAQ

    .. grid-item-card::

        **Ray Tune API**
        ^^^

        Get more in-depth information about the Ray Tune API, including all about search spaces,
        algorithms and training configurations.

        +++
        .. button-ref:: tune-api-ref
            :color: primary
            :outline:
            :expand:

            Read the API Reference

Citing Tune
-----------

If Tune helps you in your academic research, you are encouraged to cite `our paper <https://arxiv.org/abs/1807.05118>`__.
Here is an example bibtex:

.. code-block:: tex

    @article{liaw2018tune,
        title={Tune: A Research Platform for Distributed Model Selection and Training},
        author={Liaw, Richard and Liang, Eric and Nishihara, Robert
                and Moritz, Philipp and Gonzalez, Joseph E and Stoica, Ion},
        journal={arXiv preprint arXiv:1807.05118},
        year={2018}
    }
