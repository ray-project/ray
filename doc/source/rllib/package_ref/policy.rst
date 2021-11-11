.. _policy-reference-docs:

Policy APIs
===========

.. toctree::
   :maxdepth: 1

   policy/policy.rst
   policy/tf_policies.rst
   policy/torch_policy.rst

The ``Policy`` class contains functionality to compute actions for decision making
in an environment, as well as computing loss(es) and gradients, updating a neural
network model as well as postprocessing a collected environment trajectory.
One or more ``Policy`` objects sit inside a `RolloutWorker's`` "policy_map" and
are - if more than one - are selected based on a multi-agent ``policy_mapping_fn``,
which maps agent IDs to a policy ID.

.. https://docs.google.com/drawings/d/1eFAVV1aU47xliR5XtGqzQcdvuYs2zlVj1Gb8Gg0gvnc/edit
.. figure:: ../../images/rllib/policy_classes_overview.svg
    :align: left

    **RLlib's Policy class hierarchy:** Policies are deep-learning framework
    specific as they hold functionality to handle a computation graph (e.g. a
    TensorFlow 1.x graph in a session). You can define custom policy behavior
    by sub-classing either of the available, built-in classes, depending on your
    needs.


Building Custom Policy Classes
++++++++++++++++++++++++++++++

.. warning::
    It is not recommended anymore to use the ``build_policy_class()`` or ``build_tf_policy()``
    utility functions for creating custom Policy sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    either one of the built-in types.

In order to create a custom Policy, simply sub-class ``Policy`` (for a generic,
framework-agnostic policy), ``TorchPolicy`` (for a PyTorch specific policy), or
``TFPolicy`` (for a TensorFlow specific policy) and override one or more of their
methods. Those are in particular:

* compute_actions_from_input_dict
* postprocess_trajectory
* loss

`See here for a simple example on how to override TorchPolicy <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo_torch_policy.py>`_.
