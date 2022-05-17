"""A more stable successor to TD3.

By default, this uses a near-identical configuration to that reported in the
TD3 paper.
"""
from ray.rllib.agents.ddpg.ddpg import DDPGConfig, DDPGTrainer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

TD3_DEFAULT_CONFIG = DDPGTrainer.merge_trainer_configs(
    DDPGConfig().to_dict(),
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
        # Update the target network every `target_network_update_freq` sample timesteps.
        "target_network_update_freq": 0,
        "num_workers": 0,
        "num_gpus_per_worker": 0,
        "clip_rewards": False,
        "use_state_preprocessor": False,
        "replay_buffer_config": {
            "type": "MultiAgentReplayBuffer",
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": 1000000,
            "learning_starts": 10000,
            "worker_side_prioritization": False,
        },
    },
)


class TD3Trainer(DDPGTrainer):
    @classmethod
    @override(DDPGTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return TD3_DEFAULT_CONFIG
