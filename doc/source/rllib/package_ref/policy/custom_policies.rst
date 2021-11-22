.. _custom-policies-reference-docs:

Building Custom Policy Classes
==============================

.. warning::
    As of Ray >= 1.9, it is no longer recommended to use the ``build_policy_class()`` or
    ``build_tf_policy()`` utility functions for creating custom Policy sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    either one of the built-in types:
    `DynamicTFPolicy <https://github.com/ray-project/ray/blob/master/rllib/policy/dynamic_tf_policy.py>`_
    or
    `TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/policy/torch_policy.py>`_

In order to create a custom Policy, simply sub-class ``Policy`` (for a generic,
framework-agnostic policy),
`TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/policy/torch_policy.py>`_
(for a PyTorch specific policy), or
`DynamicTFPolicy <https://github.com/ray-project/ray/blob/master/rllib/policy/dynamic_tf_policy.py>`_
(for a TensorFlow specific policy) and override one or more of their methods. Those are in particular:

* compute_actions_from_input_dict
* postprocess_trajectory
* loss

`See here for a simple example on how to override TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo_torch_policy.py>`_.
