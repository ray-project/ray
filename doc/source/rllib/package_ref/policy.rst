.. _policy-docs:


Policy APIs
===========

Base Policy class (ray.rllib.policy.policy.Policy)
++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.policy.policy.Policy
    :special-members: __init__
    :members:


DL Framework Specific Sub-Classes
+++++++++++++++++++++++++++++++++

TFPolicy
--------

.. autoclass:: ray.rllib.policy.tf_policy.TFPolicy
    :special-members: __init__
    :members:

DynamicTFPolicy
---------------

.. autoclass:: ray.rllib.policy.dynamic_tf_policy.DynamicTFPolicy
    :special-members: __init__
    :members:

TorchPolicy
-----------

.. autoclass:: ray.rllib.policy.torch_policy.TorchPolicy
    :special-members: __init__
    :members:


Building Custom Policy Classes
++++++++++++++++++++++++++++++

.. automodule:: ray.rllib.policy.policy_template
    :members:

.. automodule:: ray.rllib.policy.tf_policy_template
    :members:
