import gymnasium as gym
import numpy as np

from ray.rllib.core import Columns
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class TwoAgentCartPoleWithGlobalObservations(MultiAgentEnv):
    """2-agent version of CartPole, where each agent contributes to the overall action.

    The only agentID active in this `MultiAgentEnv` is "global", however, under the
    hood, this Env is a true multi-agent one (with hidden "agent0" and "agent1"
    players). In particular:

    - The observation space is the same as for CartPole-v1, `Box(shape=(4,), float)` and
    the individual values in an observation have the same meaning.
    - The action space is a `MultiDiscrete([2, 2])`, where each agent controls one slot
    in that `MultiDiscrete` (`agent0` the first 2 discrete actions, 0 or 1, and
    `agent1` the second 2 discrete actions, also 0 or 1).
    - The actual actions applied to the underlying CartPole mechanism are as follows:
    If `agent0` picks 1 and `player1` picks 0, then cartpole action=0 (move left).
    If `agent0` picks 0 and `agent1` picks 1, then cartpole action=1 (move right).
    In all other cases (both agents pick the same value), a random underlying cartpole
    action is applied.
    - The reward for the "global" agent is a dummy value that should NOT be used for
    training. Instead, the indivudal rewards for each of the two agents are published
    in the `infos` dicts, for example: infos={"global": {"agent0": 0.2, "agent1": 0.6}}.
    Note that the `infos` dict returned from `reset()` doesn't contain any reward
    information. Both agents receive 0.5 reward per step and an additional 0.1 bonus
    if they chose 1 and the other agent choses 0.

    The env should be used with a single-policy network, where the input is the global
    observation and the output are parameters for the `MultiDiscrete` action
    distribution, from which all agents' actions can be sampled at once.
    Additionally, if value-function based training is used, the model should have 2
    value heads for the different reward streams (one for each agent).

    See here for an example RLModule:
    `rllib/examples/rl_modules/classes/shared_policy_separate_vf_heads_rlm.py`

    See here for an example custom Learner with custom loss function that can use the
    above module and learn in this env:
    `rllib/examples/learners/classes/shared_policy_separate_vf_heads_ppo_torch_learner.py`  # noqa
    """

    def __init__(self, config=None):
        super().__init__()

        # Create the underlying (single-agent) CartPole env.
        self._env = gym.make("CartPole-v1")

        self.agents = self.possible_agents = ["global"]

        # Define spaces.
        self.observation_spaces = {"global": self._env.observation_space}
        self.action_spaces = {
            "global": gym.spaces.MultiDiscrete(
                [self._env.action_space.n, self._env.action_space.n]
            ),
        }

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed, options=options)
        obs, _ = self._env.reset()
        return {"global": obs}, {}

    def step(self, actions):
        move_left, move_right = actions["global"]

        # Reward both agents equally for stabilizing the pole.
        reward_agent_left = reward_agent_right = 0.5
        # Slightly reward the agent that "takes initiative" (performs the action).
        if move_left and not move_right:
            action = 0
            reward_agent_left += 0.1
        elif move_right and not move_left:
            action = 1
            reward_agent_right += 0.1
        # Penalize inconsistent behavior (both agents wanting to move or none of them).
        else:
            # Act randomly in these cases.
            action = np.random.randint(0, 2)
            reward_agent_left -= 0.1
            reward_agent_right -= 0.1

        obs, _, terminated, truncated, _ = self._env.step(action)

        return (
            # The global observation.
            {"global": obs},
            # Global reward (not used for training).
            {"global": reward_agent_left + reward_agent_right},
            # The global termination/truncation flags.
            {"__all__": terminated},
            {"__all__": truncated},
            # Individual rewards, per-agent.
            {
                "global": {
                    Columns.REWARDS + "_agent0": reward_agent_left,
                    Columns.REWARDS + "_agent1": reward_agent_right,
                },
            },
        )


class RewardsFromInfosConnector(ConnectorV2):
    """Connector copying individual rewards from `infos` to specific batch columns."""

    def __call__(
        self,
        *,
        rl_module,
        batch,
        episodes,
        explore=None,
        shared_data=None,
        metrics=None,
        **kwargs,
    ):
        for sa_episode in self.single_agent_episode_iterator(episodes):
            # Skip 1st `info` dict, b/c it's irrelevant for the reward. For example,
            # the episode starts with a `reset()` call and a first observation, but
            # the corresponding `infos` dict doesn't have reward information in it.
            infos = sa_episode.get_infos()[1:]
            for agent in [0, 1]:
                col = Columns.REWARDS + f"_agent{agent}"
                self.add_n_batch_items(
                    batch=batch,
                    column=col,
                    items_to_add=[info[col] for info in infos],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        return batch
