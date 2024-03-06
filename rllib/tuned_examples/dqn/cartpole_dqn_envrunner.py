import ray
from ray import train, tune
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

# from ray.rllib.utils.framework import try_import_torch

# torch, _ = try_import_torch()
# torch.autograd.set_detect_anomaly(True)

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .framework(framework="torch")
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=0,
    )
    .resources(
        num_learner_workers=0,
    )
    .training(
        model={
            "fcnet_hiddens": [64],
            "fcnet_activation": "linear",
            "epsilon": [(0, 1.0), (5000, 0.02)],
            # "fcnet_weights_initializer": "xavier_uniform_",
            # "post_fcnet_weights_initializer": "xavier_uniform_",
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
        },
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        double_q=False,
        num_atoms=10,
        dueling=False,
        noisy=False,
        sigma0=0.5,
        lr=1.0,
    )
)

stop = {
    "sampler_results/episode_reward_mean": 100,
    "timesteps_total": 10000,
}

ray.init(local_mode=True)
tuner = tune.Tuner(
    "DQN",
    param_space=config,
    run_config=train.RunConfig(
        stop=stop,
        name="test_rainbow",
    ),
    # tune_config=tune.TuneConfig(
    #     num_samples=10,
    # )
)
tuner.fit()

# algo = config.build()

# for i in range(30):
#     results = algo.train()

# results
