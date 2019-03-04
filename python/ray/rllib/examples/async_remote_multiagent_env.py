from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of using BaseEnv to implement a remote, async multi-agent env.

This demonstrates using the BaseEnv API to implement an environment that
both has vectorized inference and parallel running of many different child
environment actors per process. For example, this may be useful if both model
inference and your env are expensive to run.

By extending BaseEnv directly, you have a lot of control over the
implementation details of the env (i.e., for performance tuning).

When you run this example, it prints output showing the dynamic batching
over remote envs in action:

(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 6, 8}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 5, 7, 9}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 6, 8}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 5, 7, 9}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 6, 8}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 5, 9, 7}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 6, 7, 8}
(pid=30350) Worker 1 returning obs batch for actors {0, 1, 2, 3, 4, 5, 6, 9}
"""

import argparse
import gym
import random
import time

import ray
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.tests.test_multi_agent_env import MultiCartpole
from ray import tune
from ray.tune import run_experiments
from ray.tune.registry import register_env

# Inject this delay to simulate an expensive to run env
FAKE_ENV_DELAY = 0.1

# How long to accumulate observations for batch inference
BATCH_SECONDS = 0.1

parser = argparse.ArgumentParser()
parser.add_argument("--iters", type=int, default=100)
parser.add_argument("--num-workers", type=int, default=1)
parser.add_argument("--num-remote-envs-per-worker", type=int, default=10)


@ray.remote(num_cpus=0)
class MyEnvActor(object):
    """Each RLlib worker will create a number of these remote env actors."""

    def __init__(self):
        self.env = MultiCartpole(4)  # multiagent
        print("Created new env actor")

    def reset(self):
        obs = self.env.reset()
        # each keyed by agent_id in the env
        rew = {agent_id: 0 for agent_id in obs.keys()}
        info = {agent_id: {} for agent_id in obs.keys()}
        done = {"__all__": False}
        return obs, rew, done, info

    def step(self, action_dict):
        obs, rew, done, info = self.env.step(action_dict)
        time.sleep(FAKE_ENV_DELAY)  # pretend this env is expensive to run
        return obs, rew, done, info


class MyAsyncMultiAgentEnv(BaseEnv):
    """Custom env class directly extending BaseEnv."""

    def __init__(self, env_config):
        self.actors = [
            MyEnvActor.remote() for _ in range(env_config["num_remote_envs"])
        ]
        self.worker_index = env_config.worker_index
        self.pending = None  # lazy init

    def poll(self):
        if self.pending is None:
            self.pending = {a.reset.remote(): a for a in self.actors}

        # each keyed by env_id in [0, num_remote_envs)
        obs, rewards, dones, infos = {}, {}, {}, {}
        ready = []

        # Wait for at least 1 env, but not more than 100ms total
        while not ready:
            ready, _ = ray.wait(
                list(self.pending),
                num_returns=len(self.pending),
                timeout=BATCH_SECONDS)

        # Get and return observations for each of the ready envs
        env_ids = set()
        for obj_id in ready:
            actor = self.pending.pop(obj_id)
            env_id = self.actors.index(actor)
            env_ids.add(env_id)
            ob, rew, done, info = ray.get(obj_id)
            obs[env_id] = ob
            rewards[env_id] = rew
            dones[env_id] = done
            infos[env_id] = info

        print("Worker {} returning obs batch for actors {}".format(
            self.worker_index, env_ids))
        return obs, rewards, dones, infos, {}

    def send_actions(self, action_dict):
        for env_id, actions in action_dict.items():
            actor = self.actors[env_id]
            obj_id = actor.step.remote(actions)
            self.pending[obj_id] = actor

    def try_reset(self, env_id):
        obs, _, _, _ = ray.get(self.actors[env_id].reset.remote())
        return obs


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    register_env("multi_cartpole", lambda conf: MyAsyncMultiAgentEnv(conf))
    dummy = gym.make("CartPole-v0")
    obs_space = dummy.observation_space
    act_space = dummy.action_space

    run_experiments({
        "test": {
            "run": "PPO",
            "env": "multi_cartpole",
            "stop": {
                "training_iteration": args.iters,
            },
            "config": {
                "log_level": "INFO",
                "num_sgd_iter": 10,
                "num_workers": args.num_workers,
                "env_config": {
                    "num_remote_envs": args.num_remote_envs_per_worker,
                },
                "train_batch_size": 200,
                "sample_batch_size": 100,
                "multiagent": {
                    "policy_graphs": {
                        "p1": (PPOPolicyGraph, obs_space, act_space, {}),
                        "p2": (PPOPolicyGraph, obs_space, act_space, {}),
                    },
                    "policy_mapping_fn": tune.function(
                        lambda agent_id: random.choice(["p1", "p2"])),
                },
            },
        }
    })
