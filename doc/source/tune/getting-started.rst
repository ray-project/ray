.. _tune-tutorial:

.. TODO: make this an executable notebook later on.

Getting Started
===============

This tutorial will walk you through the process of setting up a Tune experiment.
We'll start with a PyTorch model and show you how to leverage Ray Tune to optimize the hyperparameters of this model.
Specifically, we'll leverage early stopping and Bayesian Optimization via HyperOpt to do so.

.. tip:: If you have suggestions on how to improve this tutorial,
    please `let us know <https://github.com/ray-project/ray/issues/new/choose>`_!

To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install "ray[tune]" torch torchvision

Setting Up a Pytorch Model to Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start off, let's first import some dependencies.
We import some PyTorch and TorchVision modules to help us create a model and train it.
Also, we'll import Ray Tune to help us optimize the model.
As you can see we use a so-called scheduler, in this case the ``ASHAScheduler`` that we will use for tuning the model
later in this tutorial.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __tutorial_imports_begin__
   :end-before: __tutorial_imports_end__

Then, let's define a simple PyTorch model that we'll be training.
If you're not familiar with PyTorch, the simplest way to define a model is to implement a ``nn.Module``.
This requires you to set up your model with ``__init__`` and then implement a ``forward`` pass.
In this example we're using a small convolutional neural network consisting of one 2D convolutional layer, a fully
connected layer, and a softmax function.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after:  __model_def_begin__
   :end-before:  __model_def_end__

Below, we have implemented functions for training and evaluating your Pytorch model.
We define a ``train`` and a ``test`` function for that purpose.
If you know how to do this, skip ahead to the next section.

.. dropdown:: Training and evaluating the model

    .. literalinclude:: /../../python/ray/tune/tests/tutorial.py
       :language: python
       :start-after: __train_def_begin__
       :end-before: __train_def_end__

.. _tutorial-tune-setup:

Setting up Tune
~~~~~~~~~~~~~~~

Below, we define a function that trains the Pytorch model for multiple epochs.
This function will be executed on a separate :ref:`Ray Actor (process) <actor-guide>` underneath the hood,
so we need to communicate the performance of the model back to Tune (which is on the main Python process).

To do this, we call :ref:`session.report <tune-function-docstring>` in our training function,
which sends the performance value back to Tune. Since the function is executed on the separate process,
make sure that the function is :ref:`serializable by Ray <serialization-guide>`.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __train_func_begin__
   :end-before: __train_func_end__

Let's run one trial by calling :ref:`Tuner.fit <tune-run-ref>` and :ref:`randomly sample <tune-sample-docs>`
from a uniform distribution for learning rate and momentum.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __eval_func_begin__
   :end-before: __eval_func_end__

``Tuner.fit`` returns an :ref:`ResultGrid object <tune-analysis-docs>`.
You can use this to plot the performance of this trial.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
   :language: python
   :start-after: __plot_begin__
   :end-before: __plot_end__

.. note:: Tune will automatically run parallel trials across all available cores/GPUs on your machine or cluster.
    To limit the number of concurrent trials, use the :ref:`ConcurrencyLimiter <limiter>`.


Early Stopping with ASHA
~~~~~~~~~~~~~~~~~~~~~~~~

Let's integrate early stopping into our optimization process. Let's use :ref:`ASHA <tune-scheduler-hyperband>`, a scalable algorithm for `principled early stopping`_.

.. _`principled early stopping`: https://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/

On a high level, ASHA terminates trials that are less promising and allocates more time and resources to more promising trials.
As our optimization process becomes more efficient, we can afford to **increase the search space by 5x**, by adjusting the parameter ``num_samples``.

ASHA is implemented in Tune as a "Trial Scheduler".
These Trial Schedulers can early terminate bad trials, pause trials, clone trials, and alter hyperparameters of a running trial.
See :ref:`the TrialScheduler documentation <tune-schedulers>` for more details of available schedulers and library integrations.

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

You can also use :ref:`TensorBoard <tensorboard>` for visualizing results.

.. code:: bash

    $ tensorboard --logdir {logdir}


Search Algorithms in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to :ref:`TrialSchedulers <tune-schedulers>`, you can further optimize your hyperparameters
by using an intelligent search technique like Bayesian Optimization.
To do this, you can use a Tune :ref:`Search Algorithm <tune-search-alg>`.
Search Algorithms leverage optimization algorithms to intelligently navigate the given hyperparameter space.

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

* Check out the :ref:`Tune tutorials <tune-guides>` for guides on using Tune with your preferred machine learning library.
* Browse our :ref:`gallery of examples <tune-general-examples>` to see how to use Tune with PyTorch, XGBoost, Tensorflow, etc.
* `Let us know <https://github.com/ray-project/ray/issues>`__ if you ran into issues or have any questions by opening an issue on our Github.
