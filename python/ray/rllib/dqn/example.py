#!/usr/bin/env python

import gym

from ray.rllib.dqn import models, learn
from ray.rllib.dqn.common.atari_wrappers_deprecated \
    import wrap_dqn, ScaledFloatFrame


def main():
    env = gym.make("PongNoFrameskip-v4")
    env = ScaledFloatFrame(wrap_dqn(env))
    model = models.cnn_to_mlp(
        convs=[(32, 8, 4), (64, 4, 2), (64, 3, 1)],
        hiddens=[256],
        dueling=True
    )
    act = learn(
        env,
        q_func=model,
        lr=1e-4,
        max_timesteps=2000000,
        buffer_size=10000,
        exploration_fraction=0.1,
        exploration_final_eps=0.01,
        train_freq=4,
        learning_starts=10000,
        target_network_update_freq=1000,
        gamma=0.99,
        prioritized_replay=True
    )
    act.save("pong_model.pkl")
    env.close()


if __name__ == '__main__':
    main()
