.. _policy-map-docs:

PolicyMap (ray.rllib.policy.policy_map.PolicyMap)
=================================================

``PolicyMaps`` are used inside ``RolloutWorkers`` and map ``PolicyIDs`` (defined in
the `config.multiagent.policies` dictionary) to ``Policy`` instances.
The Policies are used to calculate actions for the next environment steps, losses for
model updates, and other functionalities covered by RLlib's `Policy API <https://docs.ray.io/en/master/rllib/package_ref/policy.html>`_.
A mapping function is used by episode objects to map AgentIDs produced by the environment
to one of the PolicyIDs.

It is possible to add and remove policies to/from the Trainer's workers at any given time
(even within an ongoing episode) as well as to change the policy mapping function.
See the `Trainer's add_policy(), remove_policy(), and change_policy_mapping_fn() <https://docs.ray.io/en/master/rllib/package_ref/trainer.html>`_
methods for more details.

.. autoclass:: ray.rllib.policy.policy_map.PolicyMap
    :members:
