.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-hierarchical-environments-doc:

Hierarchical Environments
=========================

.. include:: /_includes/rllib/new_api_stack.rst

You can implement hierarchical training as a special case of multi-agent RL. For example, consider a two-level hierarchy of policies,
where a top-level policy issues high level tasks that are executed at a finer timescale by one or more low-level policies.
The following timeline shows one step of the top-level policy, which corresponds to four low-level actions:

.. code-block:: text

   top-level: action_0 -------------------------------------> action_1 ->
   low-level: action_0 -> action_1 -> action_2 -> action_3 -> action_4 ->

Alternatively, you could implement an environment, in which the two agent types don't act at the same time (overlappingly),
but the low-level agents wait for the high-level agent to issue an action, then act n times before handing
back control to the high-level agent:

.. code-block:: text

   top-level: action_0 -----------------------------------> action_1 ->
   low-level: ---------> action_0 -> action_1 -> action_2 ------------>


You can implement any of these hierarchical action patterns as a multi-agent environment with various
types of agents, for example a high-level agent and a low-level agent. When set up using the correct
agent to module mapping functions, from RLlib's perspective, the problem becomes a simple independent
multi-agent problem with different types of policies.

Your configuration might look something like the following:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        .multi_agent(
            policies={"top_level", "low_level"},
            policy_mapping_fn=(
                lambda aid, eps, **kw: "low_level" if aid.startswith("low_level") else "top_level"
            ),
            policies_to_train=["top_level"],
        )
    )


In this setup, the appropriate rewards at any hierarchy level should be provided by the multi-agent env implementation.
The environment class is also responsible for routing between agents, for example conveying `goals <https://arxiv.org/pdf/1703.01161.pdf>`__ from higher-level
agents to lower-level agents as part of the lower-level agent observation.

See `this runnable example of a hierarchical env <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical/hierarchical_training.py>`__.
