.. _tune-60-seconds:

============
Key Concepts
============

Let's quickly walk through the key concepts you need to know to use Tune.
In this guide, we'll be covering the following:

.. contents::
    :local:
    :depth: 1

.. image:: /images/tune-workflow.png

Trainables
----------

To start, let's try to maximize this objective function:

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __basic_objective_start__
    :end-before: __basic_objective_end__


To use Tune, you will need to wrap this function in a lightweight :ref:`trainable API <trainable-docs>`.
You can either use a :ref:`function-based version <tune-function-api>` or a :ref:`class-based version <tune-class-api>`.

.. tabbed:: Function API

    Here's an example of specifying the objective function using :ref:`the function-based API <tune-function-api>`:

    .. literalinclude:: doc_code/key_concepts.py
        :language: python
        :start-after: __function_api_start__
        :end-before: __function_api_end__

.. tabbed:: Class API

    Here's an example of specifying the objective function using the :ref:`class-based API <tune-class-api>`:

    .. literalinclude:: doc_code/key_concepts.py
        :language: python
        :start-after: __class_api_start__
        :end-before: __class_api_end__

    .. tip:: Do not use ``tune.report`` within a ``Trainable`` class.
    .. TODO: why not? explain.

See the documentation: :ref:`trainable-docs` and :ref:`examples <tune-general-examples>`.

Hyperparameters
---------------

What are *hyperparameters?* And how are they different from *model parameters*?

In supervised learning, we train a model with labeled data so the model can properly identify new data values.
Everything about the model is defined by a set of parameters, such as the weights in a linear regression. These
are *model parameters*; they are learned during training.

.. image:: /images/hyper-model-parameters.png

In contrast, the *hyperparameters* define structural details about the kind of model itself, like whether or not
we are using a linear regression or classification, what architecture is best for a neural network,
how many layers, what kind of filters, etc. They are defined before training, not learned.

.. image:: /images/hyper-network-params.png

Other quantities considered *hyperparameters* include learning rates, discount rates, etc. If we want our training
process and resulting model to work well, we first need to determine the optimal or near-optimal set of *hyperparameters*.

How do we determine the optimal *hyperparameters*? The most direct approach is to perform a loop where we pick
a candidate set of values from some reasonably inclusive list of possible values, train a model, compare the results
achieved with previous loop iterations, and pick the set that performed best. This process is called
*Hyperparameter Tuning* or *Optimization* (HPO). And *hyperparameters* are specified over a configured and confined
search space, collectively defined for each *hyperparameter* in a ``config`` dictionary.

tune.run and Trials
-------------------

Use :ref:`tune.run <tune-run-ref>` to execute hyperparameter tuning.
This function manages your experiment and provides many features such as :ref:`logging <tune-logging>`,
:ref:`checkpointing <tune-checkpoint-syncing>`, and :ref:`early stopping <tune-stopping-ref>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __run_tunable_start__
    :end-before: __run_tunable_end__

``tune.run`` will generate a couple of hyperparameter configurations from its arguments,
wrapping them into :ref:`Trial objects <trial-docstring>`.

Each trial has

- a hyperparameter configuration (``trial.config``), id (``trial.trial_id``)
- a resource specification (``resources_per_trial`` or ``trial.placement_group_factory``)
- And other configuration values.

Each trial is also associated with one instance of a :ref:`Trainable <trainable-docs>`.
You can access trial objects through the :ref:`ExperimentAnalysis object <tune-concepts-analysis>`
provided after ``tune.run`` finishes.

``tune.run`` will execute until all trials stop or error:

.. TODO: how to make sure this doesn't get outdated?
.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 1/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 1 (1 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
    | Trial name           | status   | loc                 |         a |      b |  score | total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+----------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.1289 |        7.54952 |    15 |
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+


You can also easily run 10 trials. Tune automatically :ref:`determines how many trials will run in parallel <tune-parallelism>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __run_tunable_samples_start__
    :end-before: __run_tunable_samples_end__


Finally, you can randomly sample or grid search hyperparameters via Tune's :ref:`search space API <tune-default-search-space>`:

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __search_space_start__
    :end-before: __search_space_end__

See more documentation: :ref:`tune-run-ref`.

Search spaces
-------------

To optimize your *hyperparameters*, you have to define a *search space*.
A search space defines valid values for your hyperparameters and can specify
how these values are sampled (e.g. from a uniform distribution or a normal
distribution).

Tune offers various functions to define search spaces and sampling methods.
:ref:`You can find the documentation of these search space definitions here <tune-sample-docs>`.

Usually you pass your search space definition in the `config` parameter of
``tune.run()``.

Here's an example covering all search space functions. Again,
:ref:`here is the full explanation of all these functions <tune-sample-docs>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __config_start__
    :end-before: __config_end__


Search Algorithms
-----------------

To optimize the hyperparameters of your training process, you will want to use
a :ref:`Search Algorithm <tune-search-alg>` which will help suggest better hyperparameters.
To run the example below, make sure to first run ``pip install bayesian-optimization``.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __bayes_start__
    :end-before: __bayes_end__

Tune has SearchAlgorithms that integrate with many popular **optimization** libraries,
such as :ref:`Nevergrad <nevergrad>` and :ref:`HyperOpt <tune-hyperopt>`.
Tune automatically converts the provided search space into the search
spaces the search algorithms/underlying library expect.

See the documentation: :ref:`tune-search-alg`.

Trial Schedulers
----------------

In addition, you can make your training process more efficient by using a :ref:`Trial Scheduler <tune-schedulers>`.

Trial Schedulers can stop/pause/tweak the hyperparameters of running trials,
making your hyperparameter tuning process much faster.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __hyperband_start__
    :end-before: __hyperband_end__

:ref:`Population-based Training <tune-scheduler-pbt>` and :ref:`HyperBand <tune-scheduler-hyperband>`
are examples of popular optimization algorithms implemented as Trial Schedulers.

Unlike **Search Algorithms**, :ref:`Trial Scheduler <tune-schedulers>` do not select which hyperparameter
configurations to evaluate. However, you can use them together.

See the documentation: :ref:`schedulers-ref`.

.. _tune-concepts-analysis:

Analysis
--------

``tune.run`` returns an :ref:`ExperimentAnalysis <tune-analysis-docs>` object which has methods you can use for
analyzing your training.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __analysis_start__
    :end-before: __analysis_end__

This object can also retrieve all training runs as dataframes,
allowing you to do ad-hoc data analysis over your results.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __results_start__
    :end-before: __results_end__

What's Next?
-------------

Now that you have a working understanding of Tune, check out:

* :ref:`tune-guides`: Tutorials for using Tune with your preferred machine learning library.
* :doc:`/tune/examples/index`: End-to-end examples and templates for using Tune with your preferred machine learning library.
* :ref:`tune-tutorial`: A simple tutorial that walks you through the process of setting up a Tune experiment.


Further Questions or Issues?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /_includes/_help.rst
