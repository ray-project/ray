.. |newstack| image:: ../../doc/source/rllib/images/sigils/rllib-sigil-new-api-stack.svg
    :class: inline-figure
    :width: 32

- |newstack| `Custom Algorith.training_step() method combining on- and off-policy learning <https://github.com/ray-project/ray/blob/master/rllib/examples/algorithms/custom_training_step_on_and_off_policy_combined.py>`__:
   Example of how to override the :py:meth:`~ray.rllib.algorithms.algorithm.training_step` method of the
   :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class to train two different policies in parallel
   (also using multi-agent API).
