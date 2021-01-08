.. _tune-main:

Tune: Scalable Hyperparameter Tuning
====================================

.. tip:: We'd love to hear your feedback on using Tune - `get in touch <https://forms.gle/PTRvGLbKRdUfuzQo9>`_!

.. image:: /images/tune.png
    :scale: 30%
    :align: center

Tune is a Python library for experiment execution and hyperparameter tuning at any scale. Core features:

* Launch a multi-node :ref:`distributed hyperparameter sweep <tune-distributed>` in less than 10 lines of code.
* Supports any machine learning framework, :ref:`including PyTorch, XGBoost, MXNet, and Keras <tune-guides>`.
* Automatically manages :ref:`checkpoints <tune-checkpoint>` and logging to :ref:`TensorBoard <tune-logging>`.
* Choose among state of the art algorithms such as :ref:`Population Based Training (PBT) <tune-scheduler-pbt>`, :ref:`BayesOptSearch <bayesopt>`, :ref:`HyperBand/ASHA <tune-scheduler-hyperband>`.
* Move your models from training to serving on the same infrastructure with `Ray Serve`_.

.. _`Ray Serve`: serve/index.html

**Want to get started?** Head over to the :doc:`Key Concepts page </tune/key-concepts>`.

.. tip:: Join the `Ray community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_ to discuss Ray Tune (and other Ray libraries)!


Quick Start
-----------

To run this example, install the following: ``pip install 'ray[tune]'``.

This example runs a parallel grid search to optimize an example objective function.

.. literalinclude:: ../../../python/ray/tune/tests/example.py
   :language: python
   :start-after: __quick_start_begin__
   :end-before: __quick_start_end__

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results


.. image:: /images/tune-start-tb.png
    :scale: 30%
    :align: center

If using TF2 and TensorBoard, Tune will also automatically generate TensorBoard HParams output:

.. image:: /images/tune-hparams-coord.png
    :scale: 20%
    :align: center


Why choose Tune?
----------------

There are many other hyperparameter optimization libraries out there. If you're new to Tune, you're probably wondering, "what makes Tune different?"

Cutting-edge optimization algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As a user, you're probably looking into hyperparameter optimization because you want to quickly increase your model performance.

Tune enables you to leverage a variety of these cutting edge optimization algorithms, reducing the cost of tuning by `aggressively terminating bad hyperparameter evaluations <tune-scheduler-hyperband>`_, intelligently :ref:`choosing better parameters to evaluate <tune-search-alg>`, or even :ref:`changing the hyperparameters during training <tune-scheduler-pbt>` to optimize hyperparameter schedules.

First-class Developer Productivity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A key problem with machine learning frameworks is the need to restructure all of your code to fit the framework.

With Tune, you can optimize your model just by :ref:`adding a few code snippets <tune-tutorial>`.

Further, Tune actually removes boilerplate from your code training workflow, automatically :ref:`managing checkpoints <tune-checkpoint>` and :ref:`logging results to tools <tune-logging>` such as MLFlow and TensorBoard.


Multi-GPU & distributed training out of the box
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Hyperparameter tuning is known to be highly time-consuming, so it is often necessary to parallelize this process. Most other tuning frameworks require you to implement your own multi-process framework or build your own distributed system to speed up hyperparameter tuning.

However, Tune allows you to transparently :ref:`parallelize across multiple GPUs and multiple nodes <tune-parallelism>`. Tune even has seamless :ref:`fault tolerance and cloud support <tune-distributed>`, allowing you to scale up your hyperparameter search by 100x while reducing costs by up to 10x by using cheap preemptible instances.

What if I'm already doing hyperparameter tuning?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You might be already using an existing hyperparameter tuning tool such as HyperOpt or Bayesian Optimization.

In this situation, Tune actually allows you to power up your existing workflow. Tune's :ref:`Search Algorithms <tune-search-alg>` integrate with a variety of popular hyperparameter tuning libraries (such as Nevergrad or HyperOpt) and allow you to seamlessly scale up your optimization process -- without sacrificing performance.


Reference Materials
-------------------

Here are some reference materials for Tune:

* :doc:`/tune/user-guide`
* :ref:`Frequently asked questions <tune-faq>`
* `Code <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__: GitHub repository for Tune

Below are some blog posts and talks about Tune:

- [blog] `Tune: a Python library for fast hyperparameter tuning at any scale <https://towardsdatascience.com/fast-hyperparameter-tuning-at-scale-d428223b081c>`_
- [blog] `Cutting edge hyperparameter tuning with Ray Tune <https://medium.com/riselab/cutting-edge-hyperparameter-tuning-with-ray-tune-be6c0447afdf>`_
- [blog] `Simple hyperparameter and architecture search in tensorflow with Ray Tune <http://louiskirsch.com/ai/ray-tune>`_
- [slides] `Talk given at RISECamp 2019 <https://docs.google.com/presentation/d/1v3IldXWrFNMK-vuONlSdEuM82fuGTrNUDuwtfx4axsQ/edit?usp=sharing>`_
- [video] `Talk given at RISECamp 2018 <https://www.youtube.com/watch?v=38Yd_dXW51Q>`_
- [video] `A Guide to Modern Hyperparameter Optimization (PyData LA 2019) <https://www.youtube.com/watch?v=10uz5U3Gy6E>`_ (`slides <https://speakerdeck.com/richardliaw/a-modern-guide-to-hyperparameter-optimization>`_)

Citing Tune
-----------

If Tune helps you in your academic research, you are encouraged to cite `our paper <https://arxiv.org/abs/1807.05118>`__. Here is an example bibtex:

.. code-block:: tex

    @article{liaw2018tune,
        title={Tune: A Research Platform for Distributed Model Selection and Training},
        author={Liaw, Richard and Liang, Eric and Nishihara, Robert
                and Moritz, Philipp and Gonzalez, Joseph E and Stoica, Ion},
        journal={arXiv preprint arXiv:1807.05118},
        year={2018}
    }
