.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-callback-docs:

RLlib's callback APIs
=====================



.. note::

    Currently, RLlib only invokes callbacks in :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
    and :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors.
    The Ray team is considering expanding callbacks onto :py:class:`~ray.rllib.core.learner.learner.Learner`
    actors and possibly :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instances as well.



Callbacks and Custom Metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can provide callbacks to be called at points during policy evaluation.
These callbacks have access to state for the current
`episode <https://github.com/ray-project/ray/blob/master/rllib/evaluation/episode.py>`__.
Certain callbacks such as ``on_postprocess_trajectory``, ``on_sample_end``,
and ``on_train_result`` are also places where custom postprocessing can be applied to
intermediate data or results.

User-defined state can be stored for the
`episode <https://github.com/ray-project/ray/blob/master/rllib/evaluation/episode.py>`__
in the ``episode.user_data`` dict, and custom scalar metrics reported by saving values
to the ``episode.custom_metrics`` dict. These custom metrics are aggregated and
reported as part of training results. For a full example, take a look at
`this example script here <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_metrics_and_callbacks.py>`__
and
`these unit test cases here <https://github.com/ray-project/ray/blob/master/rllib/algorithms/tests/test_callbacks_old_stack.py>`__.

.. tip::
    You can create custom logic that can run on each evaluation episode by checking
    if the :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` is in
    evaluation mode, through accessing ``worker.policy_config["in_evaluation"]``.
    You can then implement this check in ``on_episode_start()`` or ``on_episode_end()``
    in your subclass of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`.
    For running callbacks before and after the evaluation
    runs in whole we provide ``on_evaluate_start()`` and ``on_evaluate_end``.

.. dropdown:: Click here to see the full API of the ``RLlibCallback`` class

    .. autoclass:: ray.rllib.callbacks.callbacks.RLlibCallback
        :members:


Chaining callbacks
------------------

You can define more than one :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback` class and send them in a list to the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method.
You can also send lists of callables, instead of a single callable, to the different
arguments of that method.

For example, assume you already have a subclass of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`
written and would like to reuse it in different experiments. However, one of your experiments
requires some debug callback code you would like to inject only temporarily for a couple of runs.

Resolution order of chained callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib resolves all available callback methods and callables for a given event
as follows:

Subclasses of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback` take precedence
over individual or lists of callables provided through the various arguments of
the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method.

For example, assume the callback event is ``on_train_result``, which fires at the end of
a training iteration and inside the algorithm's process.

- RLlib loops through the list of all given :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`
  subclasses and calls their ``on_train_result`` method. Thereby, it keeps the exact order the user
  provided in the list.
- RLlib then loops through the list of all defined ``on_train_result`` callables. The user configured these
  by calling the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method
  and defining the ``on_train_result`` argument in this call.

.. code-block:: python

    class MyCallbacks(RLlibCallback):
        def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs):
            print("subclass 1")

    class MyDebugCallbacks(RLlibCallback):
        def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs):
            print("subclass 2")

    # Define the callbacks order through the config.
    # Subclasses first, then individual `on_train_result` (or other events) callables:
    config.callbacks(
        callbacks_class=[MyDebugCallbacks, MyCallbacks],
        on_train_result=[
            lambda algorithm, **kw: print('in lambda 1'),
            lambda algorithm, **kw: print('in lambda 2'),
        ],
    )

    # When training the algorithm, after each training iteration, you should see
    # something like:
    # > subclass 2
    # > subclass 1
    # > in lambda 1
    # > in lambda 2
