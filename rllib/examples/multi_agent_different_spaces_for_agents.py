"""
Example showing how one can create a multi-agent env, in which the different agents
have different observation and action spaces.
These spaces do NOT necessarily have to be specified manually by the user. Instead,
RLlib will try to automatically infer them from the env provided spaces dicts
(agentID -> obs/act space) and the policy mapping fn (mapping agent IDs to policy IDs).

---
Run this example with defaults (using Tune):

  $ python multi_agent_different_spaces_for_agents.py
"""

import argparse
import gym
import os

import ray
from ray import air, tune
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class BasicMultiAgentMultiSpaces(MultiAgentEnv):
    """A simple multi-agent example environment where agents have different spaces.

    agent0: obs=(10,), act=Discrete(2)
    agent1: obs=(20,), act=Discrete(3)

    The logic of the env doesn't really matter for this example. The point of this env
    is to show how one can use multi-agent envs, in which the different agents utilize
    different obs- and action spaces.
    """

    def __init__(self, config=None):
        self.agents = {"agent0", "agent1"}
        self._agent_ids = set(self.agents)

        self.dones = set()

        # Provide full (preferred format) observation- and action-spaces as Dicts
        # mapping agent IDs to the individual agents' spaces.
        self._spaces_in_preferred_format = True
        self.observation_space = gym.spaces.Dict(
            {
                "agent0": gym.spaces.Box(low=-1.0, high=1.0, shape=(10,)),
                "agent1": gym.spaces.Box(low=-1.0, high=1.0, shape=(20,)),
            }
        )
        self.action_space = gym.spaces.Dict(
            {"agent0": gym.spaces.Discrete(2), "agent1": gym.spaces.Discrete(3)}
        )

        super().__init__()

    def reset(self):
        self.dones = set()
        return {i: self.observation_space[i].sample() for i in self.agents}

    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i] = self.observation_space[i].sample()
            rew[i] = 0.0
            done[i] = False
            info[i] = {}
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # general args
    parser.add_argument(
        "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
    )
    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="tf",
        help="The DL framework specifier.",
    )
    parser.add_argument("--eager-tracing", action="store_true")
    parser.add_argument(
        "--stop-iters", type=int, default=10, help="Number of iterations to train."
    )
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=10000,
        help="Number of timesteps to train.",
    )
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=80.0,
        help="Reward at which we stop training.",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    config = {
        "env": BasicMultiAgentMultiSpaces,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 1,
        "multiagent": {
            # Use a simple set of policy IDs. Spaces for the individual policies
            # will be inferred automatically using reverse lookup via the
            # `policy_mapping_fn` and the env provided spaces for the different
            # agents. Alternatively, you could use:
            # policies: {main0: PolicySpec(...), main1: PolicySpec}
            "policies": {"main0", "main1"},
            # Simple mapping fn, mapping agent0 to main0 and agent1 to main1.
            "policy_mapping_fn": (lambda aid, episode, worker, **kw: f"main{aid[-1]}"),
            # Only train main0.
            "policies_to_train": ["main0"],
        },
        "framework": args.framework,
        "eager_tracing": args.eager_tracing,
    }

    tune.Tuner(
        args.run,
        run_config=air.RunConfig(
            stop=stop,
        ),
        param_space=config,
    ).fit()
