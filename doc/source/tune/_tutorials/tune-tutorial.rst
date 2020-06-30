.. _tune-tutorial:

A Basic Tune Tutorial
=====================

.. image:: /images/tune-api.svg

This tutorial will walk you through the following process to setup a Tune experiment using Pytorch. Specifically, we'll leverage early stopping and Bayesian Optimization (via HyperOpt) to optimize your PyTorch model.


.. tip:: If you have suggestions as to how to improve this tutorial, please `let us know <https://github.com/ray-project/ray/issues/new/choose>`_!

Pytorch Model Setup
~~~~~~~~~~~~~~~~~~~

To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install ray torch torchvision

We first run some imports:

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __tutorial_imports_begin__
   :end-before: __tutorial_imports_end__


Below, we have some boiler plate code for a PyTorch training function. :ref:`Skip ahead to the Tune usage <tutorial-tune-setup>`.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __train_def_begin__
   :end-before: __train_def_end__

.. _tutorial-tune-setup:

Setting up Tune
~~~~~~~~~~~~~~~

Below, we define a function that trains the Pytorch model for multiple epochs. This function will be executed on a separate :ref:`Ray process <actor-guide>` underneath the hood, so we need to communicate the performance of the model back to Tune (which is on the main Python process).

To do this, we call ``tune.report`` in our training function, which sends the performance value back to Tune.

.. tip:: Since the function is executed on the separate process, it is important to make sure that the function is :ref:`serializable by Ray <serialization-guide>`.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __train_func_begin__
   :end-before: __train_func_end__

Let's run 1 trial, randomly sampling from a uniform distribution for learning rate and momentum.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __eval_func_begin__
   :end-before: __eval_func_end__

We can then plot the performance of this trial.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __plot_begin__
   :end-before: __plot_end__

.. note:: Tune will automatically run parallel trials across all available cores/GPUs on your machine or cluster. To limit the number of cores that Tune uses, you can call ``ray.init(num_cpus=<int>, num_gpus=<int>)`` before ``tune.run``.


Early Stopping with ASHA
~~~~~~~~~~~~~~~~~~~~~~~~

Let's integrate a Trial Scheduler to our search - ASHA, a scalable algorithm for principled early stopping.

How does it work? On a high level, it terminates trials that are less promising and
allocates more time and resources to more promising trials. See `this blog post <https://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/>`__ for more details.

We can afford to **increase the search space by 5x**, by adjusting the parameter ``num_samples``. See :ref:`tune-schedulers` for more details of available schedulers and library integrations.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __run_scheduler_begin__
   :end-before: __run_scheduler_end__

You can run the below in a Jupyter notebook to visualize trial progress.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __plot_scheduler_begin__
   :end-before: __plot_scheduler_end__

.. image:: /images/tune-df-plot.png
    :scale: 50%
    :align: center

You can also use Tensorboard for visualizing results.

.. code:: bash

    $ tensorboard --logdir {logdir}


Search Algorithms in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~

With Tune you can combine powerful hyperparameter search libraries such as `HyperOpt <https://github.com/hyperopt/hyperopt>`_ and `Ax <https://ax.dev>`_ with state-of-the-art algorithms such as HyperBand without modifying any model training code. Tune allows you to use different search algorithms in combination with different trial schedulers. See :ref:`tune-search-alg` for more details of available algorithms and library integrations.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __run_searchalg_begin__
   :end-before: __run_searchalg_end__


Evaluate your model
~~~~~~~~~~~~~~~~~~~

You can evaluate best trained model using the Analysis object to retrieve the best model:

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __run_analysis_begin__
   :end-before: __run_analysis_end__


Next Steps
----------

* Take a look at the :ref:`tune-user-guide` for a more comprehensive overview of Tune's features.
* Browse our :ref:`gallery of examples <tune-general-examples>` to see how to use Tune with PyTorch, XGBoost, Tensorflow, etc.
