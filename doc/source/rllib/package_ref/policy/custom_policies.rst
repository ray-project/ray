.. _custom-policies-reference-docs:

Building Custom Policy Classes
==============================

.. warning::
    As of Ray >= 1.9, it is no longer recommended to use the ``build_policy_class()`` or
    ``build_tf_policy()`` utility functions for creating custom Policy sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    either one of the built-in types:
    :py:class:`~ray.rllib.policy.dynamic_tf_policy_v2.DynamicTFPolicyV2`,
    :py:class:`~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2`
    or
    :py:class:`~ray.rllib.policy.torch_policy_v2.TorchPolicyV2`

In order to create a custom Policy, sub-class :py:class:`~ray.rllib.policy.policy.Policy` (for a generic,
framework-agnostic policy),
:py:class:`~ray.rllib.policy.torch_policy.TorchPolicy`
(for a PyTorch specific policy), or
:py:class:`~ray.rllib.policy.dynamic_tf_policy_v2.DynamicTFPolicyV2` / :py:class:`~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2`
(for a TensorFlow specific policy).
Then override one or more of these class' methods. The methods of interest are in particular:

* :py:meth:`~ray.rllib.policy.policy.Policy.make_model`
* :py:meth:`~ray.rllib.policy.policy.Policy.compute_actions`
* :py:meth:`~ray.rllib.policy.policy.Policy.loss`
* :py:meth:`~ray.rllib.policy.policy.Policy.process_gradients`
* :py:meth:`~ray.rllib.policy.policy.Policy.stats_fn`
* :py:meth:`~ray.rllib.policy.policy.Policy.postprocess_trajectory`


`See here for an example on how to override TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo_torch_policy.py>`_.
