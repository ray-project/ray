.. _custom-policies-reference-docs:

Building Custom Policy Classes
==============================

.. warning::
    As of Ray >= 1.9, it is no longer recommended to use the ``build_policy_class()`` or
    ``build_tf_policy()`` utility functions for creating custom Policy sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    either one of the built-in types:
    :py:class:`~ray.rllib.policy.dynamic_tf_policy.DynamicTFPolicy`
    or
    :py:class:`~ray.rllib.policy.torch_policy.TorchPolicy`

In order to create a custom Policy, sub-class :py:class:`~ray.rllib.policy.policy.Policy` (for a generic,
framework-agnostic policy),
:py:class:`~ray.rllib.policy.torch_policy.TorchPolicy`
(for a PyTorch specific policy), or
:py:class:`~ray.rllib.policy.dynamic_tf_policy.DynamicTFPolicy`
(for a TensorFlow specific policy) and override one or more of their methods. Those are in particular:

* :py:meth:`~ray.rllib.policy.policy.Policy.compute_actions_from_input_dict`
* :py:meth:`~ray.rllib.policy.policy.Policy.postprocess_trajectory`
* :py:meth:`~ray.rllib.policy.policy.Policy.loss`

`See here for an example on how to override TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo_torch_policy.py>`_.
