"""A more stable successor to TD3.

By default, this uses a near-identical configuration to that reported in the
TD3 paper.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, \
    DEFAULT_CONFIG as DDPG_CONFIG
from ray.rllib.utils import merge_dicts

TD3_DEFAULT_CONFIG = merge_dicts(
    DDPG_CONFIG,
    {
        # largest changes: twin Q functions, delayed policy updates, and target
        # smoothing
        "twin_q": True,
        "policy_delay": 2,
        "smooth_target_policy": True,
        "target_noise": 0.2,
        "target_noise_clip": 0.5,

        # other changes & things we want to keep fixed: IID Gaussian
        # exploration noise, larger actor learning rate, no l2 regularisation,
        # no Huber loss, etc.
        "exploration_should_anneal": False,
        "exploration_noise_type": "gaussian",
        "exploration_gaussian_sigma": 0.1,
        "learning_starts": 10000,
        "pure_exploration_steps": 10000,
        "actor_hiddens": [400, 300],
        "critic_hiddens": [400, 300],
        "n_step": 1,
        "gamma": 0.99,
        "actor_lr": 1e-3,
        "critic_lr": 1e-3,
        "l2_reg": 0.0,
        "tau": 5e-3,
        "train_batch_size": 100,
        "use_huber": False,
        "target_network_update_freq": 0,
        "num_workers": 0,
        "num_gpus_per_worker": 0,
        "per_worker_exploration": False,
        "worker_side_prioritization": False,
        "buffer_size": 1000000,
        "prioritized_replay": False,
        "clip_rewards": False,
        "use_state_preprocessor": False,
    },
)

TD3Trainer = DDPGTrainer.with_updates(
    name="TD3", default_config=TD3_DEFAULT_CONFIG)
