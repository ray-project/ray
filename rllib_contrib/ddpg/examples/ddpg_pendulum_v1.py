from rllib_ddpg.ddpg import DDPG, DDPGConfig

import ray
from ray import air, tune

if __name__ == "__main__":
    ray.init()

    config = (
        DDPGConfig()
        .rollouts(num_rollout_workers=0, rollout_fragment_length=1)
        .framework("torch")
        .environment("Pendulum-v1", clip_rewards=False)
        .training(
            actor_hiddens=[64, 64],
            critic_hiddens=[64, 64],
            n_step=1,
            model={},
            gamma=0.99,
            replay_buffer_config={
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 10000,
                "worker_side_prioritization": False,
            },
            num_steps_sampled_before_learning_starts=500,
            actor_lr=1e-3,
            critic_lr=1e-3,
            use_huber=True,
            huber_threshold=1.0,
            l2_reg=1e-6,
            train_batch_size=64,
            target_network_update_freq=0,
        )
        .reporting(min_sample_timesteps_per_iteration=600)
        .exploration(
            exploration_config={
                "type": "OrnsteinUhlenbeckNoise",
                "scale_timesteps": 10000,
                "initial_scale": 1.0,
                "final_scale": 0.02,
                "ou_base_scale": 0.1,
                "ou_theta": 0.15,
                "ou_sigma": 0.2,
            }
        )
    )

    num_iterations = 100

    tuner = tune.Tuner(
        DDPG,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": -320,
                "timesteps_total": 30000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
