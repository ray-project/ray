.. _tune-tutorial:

A Basic Tune Tutorial
=====================

This tutorial will walk you through the process of setting up Tune. Specifically, we'll leverage early stopping and Bayesian Optimization (via HyperOpt) to optimize your PyTorch model.


.. tip:: If you have suggestions as to how to improve this tutorial, please `let us know <https://github.com/ray-project/ray/issues/new/choose>`_!

To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install ray torch torchvision

Pytorch Model Setup
~~~~~~~~~~~~~~~~~~~

To start off, let's first import some dependencies:

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __tutorial_imports_begin__
   :end-before: __tutorial_imports_end__

Then, let's define the PyTorch model that we'll be training.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after:  __model_def_begin__
   :end-before:  __model_def_end__


Below, we have some boiler plate code for training and evaluating your model in Pytorch. :ref:`Skip ahead to the Tune usage <tutorial-tune-setup>`.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __train_def_begin__
   :end-before: __train_def_end__

.. _tutorial-tune-setup:

Setting up Tune
~~~~~~~~~~~~~~~

Below, we define a function that trains the Pytorch model for multiple epochs. This function will be executed on a separate :ref:`Ray Actor (process) <actor-guide>` underneath the hood, so we need to communicate the performance of the model back to Tune (which is on the main Python process).

To do this, we call :ref:`tune.report <tune-function-docstring>` in our training function, which sends the performance value back to Tune.

.. tip:: Since the function is executed on the separate process, make sure that the function is :ref:`serializable by Ray <serialization-guide>`.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __train_func_begin__
   :end-before: __train_func_end__

Let's run 1 trial by calling :ref:`tune.run <tune-run-ref>` and :ref:`randomly sample <tune-sample-docs>` from a uniform distribution for learning rate and momentum.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __eval_func_begin__
   :end-before: __eval_func_end__

``tune.run`` returns an :ref:`ExperimentAnalysis object <tune-analysis-docs>`. You can use this to plot the performance of this trial.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __plot_begin__
   :end-before: __plot_end__

.. note:: Tune will automatically run parallel trials across all available cores/GPUs on your machine or cluster. To limit the number of cores that Tune uses, you can call ``ray.init(num_cpus=<int>, num_gpus=<int>)`` before ``tune.run``. If you're using a Search Algorithm like Bayesian Optimization, you'll want to use the :ref:`ConcurrencyLimiter <limiter>`.


Early Stopping with ASHA
~~~~~~~~~~~~~~~~~~~~~~~~

Let's integrate early stopping into our optimization process. Let's use :ref:`ASHA <tune-scheduler-hyperband>`, a scalable algorithm for `principled early stopping`_.

.. _`principled early stopping`: https://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/

On a high level, ASHA terminates trials that are less promising and allocates more time and resources to more promising trials. As our optimization process becomes more efficient, we can afford to **increase the search space by 5x**, by adjusting the parameter ``num_samples``.

ASHA is implemented in Tune as a "Trial Scheduler". These Trial Schedulers can early terminate bad trials, pause trials, clone trials, and alter hyperparameters of a running trial. See :ref:`the TrialScheduler documentation <tune-schedulers>` for more details of available schedulers and library integrations.

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

You can also use :ref:`Tensorboard <tensorboard>` for visualizing results.

.. code:: bash

    $ tensorboard --logdir {logdir}


Search Algorithms in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to :ref:`TrialSchedulers <tune-schedulers>`, you can further optimize your hyperparameters by using an intelligent search technique like Bayesian Optimization. To do this, you can use a Tune :ref:`Search Algorithm <tune-search-alg>`. Search Algorithms leverage optimization algorithms to intelligently navigate the given hyperparameter space.

Note that each library has a specific way of defining the search space.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __run_searchalg_begin__
   :end-before: __run_searchalg_end__

.. note:: Tune allows you to use some search algorithms in combination with different trial schedulers. See :ref:`this page for more details <tune-schedulers>`.

Evaluate your model
~~~~~~~~~~~~~~~~~~~

You can evaluate best trained model using the :ref:`ExperimentAnalysis object <tune-analysis-docs>` to retrieve the best model:

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __run_analysis_begin__
   :end-before: __run_analysis_end__


Next Steps
----------

* Take a look at the :doc:`/tune/user-guide` for a more comprehensive overview of Tune's features.
* Check out the :ref:`Tune tutorials <tune-guides>` for guides on using Tune with your preferred machine learning library.
* Browse our :ref:`gallery of examples <tune-general-examples>` to see how to use Tune with PyTorch, XGBoost, Tensorflow, etc.
* `Let us know <https://github.com/ray-project/ray/issues>`__ if you ran into issues or have any questions by opening an issue on our Github.
