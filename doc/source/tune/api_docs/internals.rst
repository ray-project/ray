Tune Internals
==============

This page overviews the design and architectures of Tune and provides docstrings for internal components.

.. image:: ../../images/tune-arch.png

The blue boxes refer to internal components, and green boxes are public-facing.

Main Components
---------------

Tune's main components consist of TrialRunner, Trial objects, TrialExecutor, SearchAlg, TrialScheduler, and Trainable.

TrialRunner
~~~~~~~~~~~
[`source code <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial_runner.py>`__]
This is the main driver of the training loop. This component
uses the TrialScheduler to prioritize and execute trials,
queries the SearchAlgorithm for new
configurations to evaluate, and handles the fault tolerance logic.

**Fault Tolerance**: The TrialRunner executes checkpointing if ``checkpoint_freq``
is set, along with automatic trial restarting in case of trial failures (if ``max_failures`` is set).
For example, if a node is lost while a trial (specifically, the corresponding
Trainable of the trial) is still executing on that node and checkpointing
is enabled, the trial will then be reverted to a ``"PENDING"`` state and resumed
from the last available checkpoint when it is run.
The TrialRunner is also in charge of checkpointing the entire experiment execution state
upon each loop iteration. This allows users to restart their experiment
in case of machine failure.

See the docstring at :ref:`trialrunner-docstring`.

Trial objects
~~~~~~~~~~~~~
[`source code <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial.py>`__]
This is an internal data structure that contains metadata about each training run. Each Trial
object is mapped one-to-one with a Trainable object but are not themselves
distributed/remote. Trial objects transition among
the following states: ``"PENDING"``, ``"RUNNING"``, ``"PAUSED"``, ``"ERRORED"``, and
``"TERMINATED"``.

See the docstring at :ref:`trial-docstring`.

TrialExecutor
~~~~~~~~~~~~~
[`source code <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial_executor.py>`__]
The TrialExecutor is a component that interacts with the underlying execution framework.
It also manages resources to ensure the cluster isn't overloaded. By default, the TrialExecutor uses Ray to execute trials.

See the docstring at :ref:`raytrialexecutor-docstring`.


SearchAlg
~~~~~~~~~
[`source code <https://github.com/ray-project/ray/tree/master/python/ray/tune/suggest>`__] The SearchAlgorithm is a user-provided object
that is used for querying new hyperparameter configurations to evaluate.

SearchAlgorithms will be notified every time a trial finishes
executing one training step (of ``train()``), every time a trial
errors, and every time a trial completes.

TrialScheduler
~~~~~~~~~~~~~~
[`source code <https://github.com/ray-project/ray/blob/master/python/ray/tune/schedulers>`__] TrialSchedulers operate over a set of possible trials to run,
prioritizing trial execution given available cluster resources.

TrialSchedulers are given the ability to kill or pause trials,
and also are given the ability to reorder/prioritize incoming trials.

Trainables
~~~~~~~~~~
[`source code <https://github.com/ray-project/ray/blob/master/python/ray/tune/trainable.py>`__]
These are user-provided objects that are used for
the training process. If a class is provided, it is expected to conform to the
Trainable interface. If a function is provided. it is wrapped into a
Trainable class, and the function itself is executed on a separate thread.

Trainables will execute one step of ``train()`` before notifying the TrialRunner.


.. _raytrialexecutor-docstring:

RayTrialExecutor
----------------

.. autoclass:: ray.tune.ray_trial_executor.RayTrialExecutor
    :show-inheritance:
    :members:

.. _trialexecutor-docstring:

TrialExecutor
-------------

.. autoclass:: ray.tune.trial_executor.TrialExecutor
    :members:

.. _trialrunner-docstring:

TrialRunner
-----------

.. autoclass:: ray.tune.trial_runner.TrialRunner

.. _trial-docstring:

Trial
-----

.. autoclass:: ray.tune.trial.Trial

.. _resources-docstring:

Resources
---------

.. autoclass:: ray.tune.resources.Resources



Registry
--------

.. autofunction:: ray.tune.register_trainable

.. autofunction:: ray.tune.register_env
