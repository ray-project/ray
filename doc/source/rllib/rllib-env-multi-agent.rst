.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-multi-agent-environments-doc:

Multi-Agent and Hierarchical
----------------------------

In a multi-agent environment, multiple "agents" act either simultaneously,
in a turn-based sequence, or through a combination of both approaches.

For instance, in a traffic simulation, there might be multiple "car" and
"traffic light" agents interacting simultaneously. In a board game,
two or more agents may act in turn-based sequences.

The multi-agent model in RLlib follows this structure:
1. Your environment (a subclass of
   :py:class:`~ray.rllib.env.multi_agent_env.MultiAgentEnv`) returns
   dictionaries that map AgentIDs (strings chosen by the environment) to
   each agent's observations, rewards, termination/truncation flags, and
   info dictionaries.
1. You define the RLModules (policies) available for training. New RLModules
   can be added on-the-fly during training.
1. You define a mapping function that associates each AgentID with one of
   the available RLModule IDs, which will be used to compute actions for that agent.

This structure is illustrated in the following figure:

.. image:: images/multi-agent.svg

When implementing a :py:class:`~ray.rllib.env.multi_agent_env.MultiAgentEnv`,
only include agent IDs in the observation dictionary for agents that should
send actions into the next `step()` call.

This API allows you to design any type of multi-agent environment, from
`turn-based games <https://github.com/ray-project/ray/blob/master/rllib/examples/self_play_with_open_spiel.py>`__
to environments where `all agents act simultaneously <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/classes/multi_agent.py>`__,
or any combination of these.

Example: Environment with Simultaneous Agent Steps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this example, all agents in the environment step simultaneously:

.. code-block:: python

    # Multi-agent environment with two car agents and one traffic light agent.
    env = MultiAgentTrafficEnv(num_cars=2, num_traffic_lights=1)

    # Observations are a dict mapping agent IDs to their respective observations.
    # Only include agent IDs in the observation dict for agents that require actions
    # in the next `step()` call (here: all agents act simultaneously).
    print(env.reset())
    # Output:
    # {
    #   "car_1": [[...]],
    #   "car_2": [[...]],
    #   "traffic_light_1": [[...]]
    # }

    # In the `step` call, provide actions for each agent that had an observation.
    observations, rewards, terminateds, truncateds, infos = env.step(
        actions={"car_1": ..., "car_2": ..., "traffic_light_1": ...})

    # Observations, rewards, and termination flags are returned as dictionaries.
    print(rewards)
    # Output: {"car_1": 3, "car_2": -1, "traffic_light_1": 0}

    # Each agent can terminate individually; the episode ends when
    # terminateds["__all__"] is set to True.
    print(terminateds)
    # Output: {"car_2": True, "__all__": False}

Example: Turn-Based Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a turn-based environment, agents step in a sequence. For example, consider a
two-player TicTacToe game:

.. code-block:: python

    # Turn-based environment with two agents: "player1" and "player2".
    env = TicTacToe()

    # Observations map agent IDs to their respective observations.
    # Only the agent that should act next has an observation.
    print(env.reset())
    # Output:
    # {
    #   "player1": [[...]]
    # }

    # During `step`, only provide actions for agents listed in the observation dict.
    new_obs, rewards, terminateds, truncateds, infos = env.step(actions={"player1": ...})

    # Observations, rewards, and termination flags are returned as dictionaries.
    print(rewards)
    # Output: {"player1": 0, "player2": 0}

    # Agents can terminate individually; the episode ends when
    # terminateds["__all__"] is set to True.
    print(terminateds)
    # Output: {"player1": False, "__all__": False}

    # Next, it’s player2's turn, so `new_obs` contains only player2's observation.
    print(new_obs)
    # Output:
    # {
    #   "player2": [[...]]
    # }

Configuring Multi-Agent Training with Shared Algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If all agents use the same algorithm class to train their policies, configure
multi-agent training as follows:

.. code-block:: python

    from ray.rllib.algorithm.ppo import PPOConfig
    from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
    from ray.rllib.core.rl_module.rl_module import RLModuleSpec

    config = (
        PPOConfig()
        .environment(env="my_multiagent_env")
        .multi_agent(
            policy_mapping_fn=lambda agent_id, episode, **kwargs: (
                "traffic_light" if agent_id.startswith("traffic_light_")
                else random.choice(["car1", "car2"])
            ),
            algorithm_config_overrides_per_module={
                "car1": PPOConfig.overrides(gamma=0.85),
                "car2": PPOConfig.overrides(lr=0.00001),
            },
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(rl_module_specs={
                "car1": RLModuleSpec(),
                "car2": RLModuleSpec(),
                "traffic_light": RLModuleSpec(),
            }),
        )
    )

    algo = config.build()
    print(algo.train())

To exclude certain policies from being updated, use the ``config.multi_agent(policies_to_train=[..])`` config setting.
This allows running in multi-agent environments with a mix of non-learning and learning policies:

.. code-block:: python

    def policy_mapping_fn(agent_id, episode, **kwargs):
        agent_idx = int(agent_id[-1])  # 0 (player1) or 1 (player2)
        return "learning_policy" if episode.id_ % 2 == agent_idx else "random_policy"

    config = (
        PPOConfig()
        .environment(env="two_player_game")
        .multi_agent(
            policy_mapping_fn=policy_mapping_fn,
            policies_to_train=["learning_policy"],
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(rl_module_specs={
                "learning_policy": RLModuleSpec(),
                "random_policy": RLModuleSpec(rl_module_class=RandomRLModule),
            }),
        )
    )

    algo = config.build()
    print(algo.train())

RLlib will create and route decisions to each policy based on the provided
``policy_mapping_fn``. Training statistics for each policy are reported
separately in the result-dict returned by ``train()``.

The example scripts `rock_paper_scissors_heuristic_vs_learned.py <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py>`__
and `rock_paper_scissors_learned_vs_learned.py <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_learned_vs_learned.py>`__
demonstrate competing policies with heuristic and learned strategies.


Scaling to Many Agents
~~~~~~~~~~~~~~~~~~~~~~

To scale to hundreds of agents using the same policy, RLlib batches policy
evaluations across agents within a `MultiAgentEnv`. You can also auto-vectorize
these environments by setting ``num_envs_per_env_runner > 1``.


PettingZoo Multi-Agent Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`PettingZoo <https://github.com/Farama-Foundation/PettingZoo>`__ offers a
repository of over 50 diverse multi-agent environments. Though not directly
compatible with RLlib, they can be adapted using the RLlib `PettingZooEnv` wrapper:

.. code-block:: python

    from ray.tune.registry import register_env
    from pettingzoo.butterfly import prison_v3
    from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv

    env_creator = lambda config: prison_v3.env(num_floors=config.get("num_floors", 4))
    register_env("prison", lambda config: PettingZooEnv(env_creator(config)))

    config = PPOConfig.environment("prison", env_config={"num_floors": 5})

See `rllib_pistonball.py <https://github.com/Farama-Foundation/PettingZoo/blob/master/tutorials/Ray/rllib_pistonball.py>`__ for a full example.


Variable-Sharing Between Policies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib supports variable-sharing across policies.

See the `PettingZoo parameter sharing example <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_parameter_sharing.py>`__ for details.


Implementing a Centralized Critic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO!!!

Grouping Agents
~~~~~~~~~~~~~~~

It is common to have groups of agents in multi-agent RL. RLlib treats agent groups like a single agent with a Tuple action and observation space.
The group agent can then be assigned to a single policy for centralized execution, or to specialized multi-agent policies that
implement centralized training but decentralized execution. You can use the ``MultiAgentEnv.with_agent_groups()`` method to define these groups:

.. literalinclude:: ../../../rllib/env/multi_agent_env.py
   :language: python
   :start-after: __grouping_doc_begin__
   :end-before: __grouping_doc_end__

For environments with multiple groups, or mixtures of agent groups and individual agents, you can use grouping in conjunction with the policy mapping API described in prior sections.


Hierarchical Environments
~~~~~~~~~~~~~~~~~~~~~~~~~

Hierarchical training can sometimes be implemented as a special case of multi-agent RL. For example, consider a three-level hierarchy of policies, where a top-level policy issues high level actions that are executed at finer timescales by a mid-level and low-level policy. The following timeline shows one step of the top-level policy, which corresponds to two mid-level actions and five low-level actions:

.. code-block:: text

   top_level ---------------------------------------------------------------> top_level --->
   mid_level_0 -------------------------------> mid_level_0 ----------------> mid_level_1 ->
   low_level_0 -> low_level_0 -> low_level_0 -> low_level_1 -> low_level_1 -> low_level_2 ->

This can be implemented as a multi-agent environment with three types of agents. Each higher-level action creates a new lower-level agent instance with a new id (e.g., ``low_level_0``, ``low_level_1``, ``low_level_2`` in the above example). These lower-level agents pop in existence at the start of higher-level steps, and terminate when their higher-level action ends. Their experiences are aggregated by policy, so from RLlib's perspective it's just optimizing three different types of policies. The configuration might look something like this:

.. code-block:: python

    "multiagent": {
        "policies": {
            "top_level": (custom_policy or None, ...),
            "mid_level": (custom_policy or None, ...),
            "low_level": (custom_policy or None, ...),
        },
        "policy_mapping_fn":
            lambda agent_id:
                "low_level" if agent_id.startswith("low_level_") else
                "mid_level" if agent_id.startswith("mid_level_") else "top_level"
        "policies_to_train": ["top_level"],
    },


In this setup, the appropriate rewards for training lower-level agents must be provided by the multi-agent env implementation.
The environment class is also responsible for routing between the agents, e.g., conveying `goals <https://arxiv.org/pdf/1703.01161.pdf>`__ from higher-level
agents to lower-level agents as part of the lower-level agent observation.

See this file for a runnable example: `hierarchical_training.py <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical/hierarchical_training.py>`__.
