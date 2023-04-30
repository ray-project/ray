
Building Custom Policy Classes
------------------------------

.. currentmodule:: ray.rllib

.. warning::
    As of Ray >= 1.9, it is no longer recommended to use the ``build_policy_class()`` or
    ``build_tf_policy()`` utility functions for creating custom Policy sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    either one of the built-in types:
    :py:class:`~policy.eager_tf_policy_v2.EagerTFPolicyV2`
    or
    :py:class:`~policy.torch_policy_v2.TorchPolicyV2`

In order to create a custom Policy, sub-class :py:class:`~policy.policy.Policy` (for a generic,
framework-agnostic policy),
:py:class:`~policy.torch_policy_v2.TorchPolicyV2`
(for a PyTorch specific policy), or
:py:class:`~policy.eager_tf_policy_v2.EagerTFPolicyV2`
(for a TensorFlow specific policy) and override one or more of their methods. Those are in particular:

* :py:meth:`~policy.policy.Policy.compute_actions_from_input_dict`
* :py:meth:`~policy.policy.Policy.postprocess_trajectory`
* :py:meth:`~policy.policy.Policy.loss`

`See here for an example on how to override TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo_torch_policy.py>`_.
