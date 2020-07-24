.. _tune-index:

Tune: Scalable Hyperparameter Tuning
====================================

.. image:: images/tune.png
    :scale: 30%
    :align: center

Tune is a Python library for experiment execution and hyperparameter tuning at any scale. Core features:

  * Launch a multi-node :ref:`distributed hyperparameter sweep <tune-distributed>` in less than 10 lines of code.
  * Supports any machine learning framework, :ref:`including PyTorch, XGBoost, MXNet, and Keras<tune-guides-overview>`.
  * Automatically manages :ref:`checkpoints <tune-checkpoint>` and logging to :ref:`TensorBoard <tune-logging>`.
  * Choose among state of the art algorithms such as :ref:`Population Based Training (PBT) <tune-scheduler-pbt>`, :ref:`BayesOptSearch <bayesopt>`, :ref:`HyperBand/ASHA <tune-scheduler-hyperband>`.
  * Move your models from training to serving on the same infrastructure with `Ray Serve`_.

.. _`Ray Serve`: serve/index.html

**Want to get started?** Head over to the :ref:`60 second Tune tutorial <tune-60-seconds>`.

.. tip:: We'd love to hear your feedback on using Tune - fill out a `short survey <https://forms.gle/PTRvGLbKRdUfuzQo9>`_!

.. tip:: Join the `Ray community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_ to discuss Ray Tune (and other Ray libraries)!


Quick Start
-----------

To run this example, install the following: ``pip install 'ray[tune]' torch torchvision``.

This example runs a small grid search to train a convolutional neural network using PyTorch and Tune.

.. literalinclude:: ../../python/ray/tune/tests/example.py
   :language: python
   :start-after: __quick_start_begin__
   :end-before: __quick_start_end__

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results


.. image:: images/tune-start-tb.png
    :scale: 30%
    :align: center

If using TF2 and TensorBoard, Tune will also automatically generate TensorBoard HParams output:

.. image:: images/tune-hparams-coord.png
    :scale: 20%
    :align: center


Why choose Tune?
----------------

There are many other hyperparameter optimization libraries out there. If you're new to Tune, you're probably wondering, "what makes Tune different?"

.. include:: tune/why_tune.rst

Reference Materials
-------------------

Here are some reference materials for Tune:

  * :ref:`Tune Tutorials, Guides, and Examples <tune-guides-overview>`
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
