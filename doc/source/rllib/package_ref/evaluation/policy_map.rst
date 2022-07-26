.. _policy-map-docs:

PolicyMap (ray.rllib.policy.policy_map.PolicyMap)
=================================================

``PolicyMaps`` are used inside :py:class:`~ray.rllib.evaluation.rollout_workers.RolloutWorker`s and
map ``PolicyIDs`` (defined in the `config.multiagent.policies` dictionary) to :py:class:`~ray.rllib.policy.policy.Policy` instances.
The Policies are used to calculate actions for the next environment steps, losses for
model updates, and other functionalities covered by RLlib's :py:class:`~ray.rllib.policy.policy.Policy` API.
A mapping function is used by episode objects to map AgentIDs produced by the environment to one of the PolicyIDs.

It is possible to add and remove policies to/from the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`'s workers at any given time
(even within an ongoing episode) as well as to change the policy mapping function.
See the Algorithm's methods: :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.add_policy`,
:py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.remove_policy`, and
:py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.change_policy_mapping_fn` for more details.

.. autoclass:: ray.rllib.policy.policy_map.PolicyMap
    :members:
