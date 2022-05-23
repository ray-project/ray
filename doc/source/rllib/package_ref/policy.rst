.. _policy-reference-docs:

Policies
========

The :py:class:`~ray.rllib.policy.policy.Policy` class contains functionality to compute
actions for decision making in an environment, as well as computing loss(es) and gradients,
updating a neural network model as well as postprocessing a collected environment trajectory.
One or more :py:class:`~ray.rllib.policy.policy.Policy` objects sit inside a
:py:class:`~ray.rllib.evaluation.RolloutWorker`'s :py:class:`~ray.rllib.policy.policy_map.PolicyMap` and
are - if more than one - are selected based on a multi-agent ``policy_mapping_fn``,
which maps agent IDs to a policy ID.

.. https://docs.google.com/drawings/d/1eFAVV1aU47xliR5XtGqzQcdvuYs2zlVj1Gb8Gg0gvnc/edit
.. figure:: ../images/policy_classes_overview.svg
    :align: left

    **RLlib's Policy class hierarchy:** Policies are deep-learning framework
    specific as they hold functionality to handle a computation graph (e.g. a
    TensorFlow 1.x graph in a session). You can define custom policy behavior
    by sub-classing either of the available, built-in classes, depending on your
    needs.


Policy API Reference
--------------------

.. toctree::
   :maxdepth: 1

   policy/policy.rst
   policy/tf_policies.rst
   policy/torch_policy.rst
   policy/custom_policies.rst

