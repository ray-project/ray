.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-multi-agent-environments-doc:

Multi-Agent Environments
========================

In a multi-agent environment, multiple "agents" act simultaneously, in a turn-based
sequence, or through an arbitrary combination of both.

For instance, in a traffic simulation, there might be multiple "car" and
"traffic light" agents interacting simultaneously, whereas in a board game,
two or more agents may act in a turn-based sequence.

Several different policy networks may be used to control the various agents.
Thereby, each of the agents in the environment maps to exactly one particular policy. This mapping is
determined by a user-provided function, called the "mapping function". Note that if there
are ``N`` agents mapping to ``M`` policies, ``N`` is always larger or equal to ``M``,
allowing for any policy to control more than one agent.

.. figure:: images/envs/multi_agent_setup.svg
    :width: 600

    **Multi-agent setup:** ``N`` agents live in the environment and take actions computed by ``M`` policy networks.
    The mapping from agent to policy is flexible and determined by a user-provided mapping function. Here, `agent_1`
    and `agent_3` both map to `policy_1`, whereas `agent_2` maps to `policy_2`.


RLlib's MultiAgentEnv API
-------------------------

The :py:class`~ray.rllib.env.multi_agent_env.MultiAgentEnv` API of RLlib closely follows the
conventions and APIs of `Farama's gymnasium (single-agent) <gymnasium.farama.org>`__ envs and even subclasses
from `gymnasium.Env`, however, instead of publishing individual observations, rewards, and termination/truncation flags
from `reset()` and `step()`, a custom :py:class`~ray.rllib.env.multi_agent_env.MultiAgentEnv` implementation
outputs dictionaries, one for observations, one for rewards, etc..in which agent IDs map
In each such multi-agent dictionary, agent IDs map to the respective individual agent's observation/reward/etc..

Here is a first draft of an example :py:class`~ray.rllib.env.multi_agent_env.MultiAgentEnv` implementation:

.. code-block::

    from ray.rllib.env.multi_agent_env import MultiAgentEnv

    class MyMultiAgentEnv(MultiAgentEnv):

        def __init__(self, config=None):
            super().__init__()
            ...

        def reset(self, *, seed=None, options=None):
            ...
            # return observation dict and infos dict.
            return {"agent_1": [obs of agent_1], "agent_2": [obs of agent_2]}, {}

        def step(self, action_dict):
            # return observation dict, rewards dict, termination/truncation dicts, and infos dict
            return {"agent_1": [obs of agent_1]}, {...}, ...


Agent Definitions
~~~~~~~~~~~~~~~~~

The number of agents in your environment and their IDs are entirely controlled by your :py:class`~ray.rllib.env.multi_agent_env.MultiAgentEnv`
code. Your env decides, which agents start after an episode reset, which agents enter the episode at a later point, which agents
terminate the episode early, and which agents stay in the episode until the entire episode ends.

To define, which agent IDs might even show up in your episodes, set the `self.possible_agents` attribute to a list of
all possible agent ID.

.. code-block::

    def __init__(self, config=None):
        super().__init__()
        ...
        # Define all agent IDs that might even show up in your episodes.
        self.possible_agents = ["agent_1", "agent_2"]
        ...

In case your environment only starts with a subset of agent IDs and/or terminates some agent IDs before the end of the episode,
you also need to permanently adjust the `self.agents` attribute throughout the course of your episode.
If - on the other hand - all agent IDs are static throughout your episodes, you can set `self.agents` to be the same
as `self.possible_agents` and don't change its value throughout the rest of your code:

.. code-block::

    def __init__(self, config=None):
        super().__init__()
        ...
        # If your agents never change throughout the episode, set
        # `self.agents` to the same list as `self.possible_agents`.
        self.agents = self.possible_agents = ["agent_1", "agent_2"]
        # Otherwise, you will have to adjust `self.agents` in `reset()` and `step()` to whatever the
        # currently "alive" agents are.
        ...

Observation- and Action Spaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Next, you should set the observation- and action-spaces of each (possible) agent ID in your constructor.
Use the `self.observation_spaces` and `self.action_spaces` attributes to define dictionaries mapping
agent IDs to the individual agents' spaces. For example:

.. code-block::

    import gymnasium as gym
    import numpy as np

    ...

        def __init__(self, config=None):
            super().__init__()
            ...
            self.observation_spaces = {
                "agent_1": gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
                "agent_2": gym.spaces.Box(-1.0, 1.0, (3,), np.float32),
            }
            self.action_spaces = {
                "agent_1": gym.spaces.Discrete(2),
                "agent_2": gym.spaces.Box(0.0, 1.0, (1,), np.float32),
            }
            ...

In case your episodes hosts a lot of agents, some sharing the same observation- or action
spaces, and you don't want to create very large spaces dicts, you can also override the
:py:meth:`~ray.rllib.env.multi_agent_env.MultiAgentEnv.get_observation_space` and
:py:meth:`~ray.rllib.env.multi_agent_env.MultiAgentEnv.get_action_space` methods and implement the mapping logic
from agent ID to space yourself. For example:

.. code-block::

    def get_observation_space(self, agent_id):
        if agent_id.startswith("robot_"):
            return gym.spaces.Box(0, 255, (84, 84, 3), np.uint8)
        elif agent_id.startswith("decision_maker"):
            return gym.spaces.Discrete(2)
        else:
            raise ValueError(f"bad agent id: {agent_id}!")


Observation-, Reward-, and Termination Dictionaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The last two things you need to implement in your custom :py:class:`~ray.rllib.env.multi_agent_env.MultiAgentEnv`
are the `reset()` and `step()` methods. Equivalently to a single-agent `gymnasium.Env <https://gymnasium.farama.org/_modules/gymnasium/core/#Env>`__,
you have to return observations and infos from `reset()`, and return observations, rewards, termination/truncation flags, and infos
from `step()`, however, instead of individual values, these all have to be dictionaries mapping agent IDs to the respective
individual agents' values.

Let's take a look at an example `reset()` implementation first:

.. code-block::

    def reset(self, *, seed=None, options=None):
        ...
        return {
            "agent_1": np.array([0.0, 1.0, 0.0, 0.0]),
            "agent_2": np.array([0.0, 0.0, 1.0]),
        }, {}  # <- empty info dict

Here, your episode starts with both agents in it, and both expected to compute and send actions
for the following `step()` call.

In general, the returned observations dict must contain those agents (and only those agents)
that should act next. Agent IDs that should NOT act in the next `step()` call should NOT have
their observations in the returned observations dict.

Note also that this rule does not apply to reward dicts, termination dicts, or truncation dicts, which
may contain any agent ID at any time step regardless of whether the agent ID is expected to act or not
in the next `step()` call. This is so that an action of agent A can trigger some reward for agent B, even
though B is currently not acting itself. The same is true for termination flags: Agent A may act in a way
that terminates agent B from the episode without agent B having acted itself recently.

In other words, you determine the exact order and synchronization of agent actions in your multi-agent episode
through the agent IDs contained in (and missing from) your observations dicts.
Only those agent ID that must compute and send actions into the next `step()` call must be part of the
returned observation dict.

This API allows you to design any type of multi-agent environment, from turn-based games to
environments where all agents always act simultaneously, to any combination of these two patterns.

Let's take a look at two specific, complete :py:class:`~ray.rllib.env.multi_agent_env.MultiAgentEnv` example implementations,
one where agents always act simulatenously and one where agents act in a turn-based sequence.


Example: Environments with Simultaneous Agent Steps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



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

Example: Turn-Based Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a turn-based environment, agents step in a sequence. For example, consider a two-player TicTacToe game:

.. literalinclude::


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

Other Supported Multi-Agent Env APIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Besides RLlib's own :py:class`~ray.rllib.env.multi_agent_env.MultiAgentEnv` API, you can also use
`Farama's PettingZoo <pettingzoo.farama.org>`__ API for writing your custom multi-agent env.


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

.. note::

    Multi-agent setups are not vectorizable yet. The Ray team is working on a solution for
    this restriction by utilizing `gymnasium >= 1.x` custom vectorization feature.


PettingZoo Multi-Agent Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`PettingZoo <https://github.com/Farama-Foundation/PettingZoo>`__ offers a
repository of over 50 diverse multi-agent environments, directly compatible with RLlib
through its built-in `PettingZooEnv` wrapper:

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

