from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import gym

from ray.rllib.models import ModelCatalog
from ray.rllib.impala import DEFAULT_CONFIG
from ray.rllib.impala.policy import Policy


def test_shadow(env_id):
    env_model = {
        #"grayscale": True,
        #"zero_mean": False,
        #"dim": 80,
        #"channel_major": False
    }
    env_creator = lambda env_config: gym.make(env_id)
    env = ModelCatalog.get_preprocessor_as_wrapper(
            None, env_creator(""), env_model)
    ob_space = env.observation_space.shape
    ac_space = env.action_space


    config = DEFAULT_CONFIG
    config["optimizer"]["train_batch_size"] = 40
    num_trajs = config["optimizer"]["train_batch_size"] // config["num_local_steps"]
    config["optimizer"]["num_trajs"] = num_trajs
    traj_length = config["num_local_steps"]
    agent = Policy(None, ob_space, ac_space, config)
    print("Agent created successfully")

    
    obs = list()
    features = list()
    actions = list()
    rewards = list()
    extras = list()

    traj_obs = list()
    traj_features = list()
    traj_actions = list()
    traj_rewards = list()
    traj_extras = list()

    num_step = 0
    all_rwd = list()

    for t in range(1000):
        last_state = env.reset()
        last_features = agent.get_initial_features()
        done = False
        episode_ts = 0
        episode_rwd = .0
        while not done:
            action, pi_info = agent.compute(
                last_state,
                last_features[0],
                last_features[1])
            next_state, rwd, done, info = env.step(action)
            
            traj_obs.append(last_state)
            traj_features.append(last_features)
            traj_actions.append(action)
            episode_rwd += rwd
            traj_rewards.append(rwd)
            traj_extras.append(pi_info["logprobs"])
            if len(traj_obs) == traj_length:
                obs += traj_obs#obs.append(traj_obs)
                features.append(traj_features)
                actions+=traj_actions#actions.append(traj_actions)
                rewards+=traj_rewards#rewards.append(traj_rewards)
                extras+=traj_extras#extras.append(traj_extras)
                if len(obs) == num_trajs * traj_length:
                    samples = {
                        "obs": obs,
                        "features": features,
                        "actions": actions,
                        "rewards": rewards,
                        "logprobs": extras}
                    #agent.compute_gradients(samples)
                    agent.compute_and_apply_gradients(samples)
                    raw_input()
                    obs = list()
                    features = list()
                    actions = list()
                    rewards = list()
                    extras = list()
                traj_obs = list()
                traj_features = list()
                traj_actions = list()
                traj_rewards = list()
                traj_extras = list()

            episode_ts += 1
            last_state = next_state
            last_features = pi_info["features"]

        all_rwd.append(episode_rwd)
        print(np.mean(all_rwd[-100:]))


if __name__=="__main__":
    test_shadow("CartPole-v0")
