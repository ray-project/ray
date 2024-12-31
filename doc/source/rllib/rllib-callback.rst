.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-callback-docs:


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


Chaining Callbacks
~~~~~~~~~~~~~~~~~~

Use the :py:func:`~ray.rllib.algorithms.callbacks.make_multi_callbacks` utility to chain
multiple callbacks together.

.. autofunction:: ray.rllib.algorithms.callbacks.make_multi_callbacks
