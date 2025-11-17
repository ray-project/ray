"""
Example showing how to create a multi-agent env, in which the different agents
have different observation and action spaces.

These spaces do NOT necessarily have to be specified manually by the user. Instead,
RLlib tries to automatically infer them from the env provided spaces dicts
(agentID -> obs/act space) and the policy mapping fn (mapping agent IDs to policy IDs).

How to run this script
----------------------
`python [script file name].py --num-agents=2`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`
"""

import gymnasium as gym

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


class BasicMultiAgentMultiSpaces(MultiAgentEnv):
    """A simple multi-agent example environment where agents have different spaces.

    agent0: obs=Box(10,), act=Discrete(2)
    agent1: obs=Box(20,), act=Discrete(3)

    The logic of the env doesn't really matter for this example. The point of this env
    is to show how to use multi-agent envs, in which the different agents utilize
    different obs- and action spaces.
    """

    def __init__(self, config=None):
        self.agents = ["agent0", "agent1"]

        self.terminateds = set()
        self.truncateds = set()

        # Provide full (preferred format) observation- and action-spaces as Dicts
        # mapping agent IDs to the individual agents' spaces.
        self.observation_spaces = {
            "agent0": gym.spaces.Box(low=-1.0, high=1.0, shape=(10,)),
            "agent1": gym.spaces.Box(low=-1.0, high=1.0, shape=(20,)),
        }
        self.action_spaces = {
            "agent0": gym.spaces.Discrete(2),
            "agent1": gym.spaces.Discrete(3),
        }

        super().__init__()

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()
        return {i: self.get_observation_space(i).sample() for i in self.agents}, {}

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i] = self.get_observation_space(i).sample()
            rew[i] = 0.0
            terminated[i] = False
            truncated[i] = False
            info[i] = {}
        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, terminated, truncated, info


parser = add_rllib_example_script_args(
    default_iters=10, default_reward=80.0, default_timesteps=10000
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(env=BasicMultiAgentMultiSpaces)
        .training(train_batch_size=1024)
        .multi_agent(
            # Use a simple set of policy IDs. Spaces for the individual policies
            # are inferred automatically using reverse lookup via the
            # `policy_mapping_fn` and the env provided spaces for the different
            # agents. Alternatively, you could use:
            # policies: {main0: PolicySpec(...), main1: PolicySpec}
            policies={"main0", "main1"},
            # Simple mapping fn, mapping agent0 to main0 and agent1 to main1.
            policy_mapping_fn=(lambda aid, episode, **kw: f"main{aid[-1]}"),
            # Only train main0.
            policies_to_train=["main0"],
        )
    )

    run_rllib_example_script_experiment(base_config, args)
