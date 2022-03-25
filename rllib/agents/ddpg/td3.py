"""A more stable successor to TD3.

By default, this uses a near-identical configuration to that reported in the
TD3 paper.
"""
from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, DEFAULT_CONFIG as DDPG_CONFIG
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict

TD3_DEFAULT_CONFIG = DDPGTrainer.merge_trainer_configs(
    DDPG_CONFIG,
    {
        # largest changes: twin Q functions, delayed policy updates, and target
        # smoothing
        "twin_q": True,
        "policy_delay": 2,
        "smooth_target_policy": True,
        "target_noise": 0.2,
        "target_noise_clip": 0.5,
        "exploration_config": {
            # TD3 uses simple Gaussian noise on top of deterministic NN-output
            # actions (after a possible pure random phase of n timesteps).
            "type": "GaussianNoise",
            # For how many timesteps should we return completely random
            # actions, before we start adding (scaled) noise?
            "random_timesteps": 10000,
            # Gaussian stddev of action noise for exploration.
            "stddev": 0.1,
            # Scaling settings by which the Gaussian noise is scaled before
            # being added to the actions. NOTE: The scale timesteps start only
            # after(!) any random steps have been finished.
            # By default, do not anneal over time (fixed 1.0).
            "initial_scale": 1.0,
            "final_scale": 1.0,
            "scale_timesteps": 1,
        },
        # other changes & things we want to keep fixed:
        # larger actor learning rate, no l2 regularisation, no Huber loss, etc.
        "learning_starts": 10000,
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
        "worker_side_prioritization": False,
        "buffer_size": 1000000,
        "prioritized_replay": False,
        "clip_rewards": False,
        "use_state_preprocessor": False,
    },
)


class TD3Trainer(DDPGTrainer):
    @classmethod
    @override(DDPGTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return TD3_DEFAULT_CONFIG
