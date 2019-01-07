from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

"""Simple example of two agents working together to stabilize a cartpole.

Used as a quick test of whether a centralized value function is working/helping

Classic cart-pole system implemented by Rich Sutton et al.
Copied from http://incompleteideas.net/sutton/book/code/pole.c
permalink: https://perma.cc/C9ZM-652R
"""

import math
from gym import logger
from gym.envs.classic_control.cartpole import CartPoleEnv
import numpy as np

import ray
from ray import tune
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class SharedCartPoleEnv(CartPoleEnv, MultiAgentEnv):
    metadata = {
        'render.modes': ['human', 'rgb_array'],
        'video.frames_per_second': 50
    }

    def step(self, actions):
        force = 0
        for agent, action in actions.items():
            if action == 1:
                force += self.force_mag / 2.0
            else:
                force -= self.force_mag / 2.0
        state = self.state
        x, x_dot, theta, theta_dot = state
        costheta = math.cos(theta)
        sintheta = math.sin(theta)
        temp = (
                       force + self.polemass_length * theta_dot
                       * theta_dot * sintheta) / self.total_mass
        thetaacc = (self.gravity * sintheta - costheta * temp) / (
                self.length * (
                               4.0 / 3.0 - self.masspole * costheta *
                               costheta / self.total_mass))
        xacc = temp - self.polemass_length * thetaacc * \
            costheta / self.total_mass
        x = x + self.tau * x_dot
        x_dot = x_dot + self.tau * xacc
        theta = theta + self.tau * theta_dot
        theta_dot = theta_dot + self.tau * thetaacc
        self.state = (x, x_dot, theta, theta_dot)
        done = x < -self.x_threshold \
            or x > self.x_threshold \
            or theta < -self.theta_threshold_radians \
            or theta > self.theta_threshold_radians
        done = bool(done)

        if not done:
            reward = 1.0
        elif self.steps_beyond_done is None:
            # Pole just fell!
            self.steps_beyond_done = 0
            reward = 1.0
        else:
            if self.steps_beyond_done == 0:
                logger.warn(
                    "You are calling 'step()' even though this "
                    "environment has already returned done = True. "
                    "You should always call 'reset()' once "
                    "you receive 'done = True' -- any further "
                    "steps are undefined behavior.")
            self.steps_beyond_done += 1
            reward = 0.0

        states = {}
        rewards = {}
        dones = {}
        infos = {}
        dones["__all__"] = done
        for agent_id in actions.keys():
            states[agent_id] = np.array(self.state)
            rewards[agent_id] = reward
            dones[agent_id] = done
            infos[agent_id] = {}
        return states, rewards, dones, infos

    def reset(self):
        self.state = self.np_random.uniform(low=-0.05, high=0.05, size=(4,))
        self.steps_beyond_done = None
        return {'agent-0': np.array(self.state),
                'agent-1': np.array(self.state)}


if __name__ == "__main__":
    ray.init(num_cpus=1)

    # Simple environment with `num_agents` independent cartpole entities
    register_env("multi_cartpole", lambda _: SharedCartPoleEnv())
    single_env = SharedCartPoleEnv()
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    # Each policy can have a different configuration (including custom model)
    def gen_policy(i):
        return (PPOPolicyGraph, obs_space, act_space, {})

    # Setup PPO with an ensemble of `num_policies` different policy graphs
    policy_graphs = {'shared': (PPOPolicyGraph, obs_space, act_space, {})}

    def policy_mapping_fn(_):
        return 'shared'

    run_experiments({
        "test": {
            "run": "PPO",
            "env": "multi_cartpole",
            "stop": {
                "training_iteration": 100
            },
            "config": {
                "use_centralized_vf": True,
                "max_vf_agents": 2,
                "log_level": "DEBUG",
                "num_sgd_iter": 10,
                "num_workers": 0,
                "multiagent": {
                    "policy_graphs": policy_graphs,
                    "policy_mapping_fn": tune.function(policy_mapping_fn),
                },
            },
        }
    })
