# import ray
from itertools import product
from ray import train, tune
from ray.air.integrations.wandb import WandbLoggerCallback
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
            "fcnet_hiddens": [256],
            "fcnet_activation": "linear",
            "epsilon": [(0, 1.0), (10000, 0.05)],
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
        double_q=True,
        num_atoms=10,
        noisy=True,
        dueling=True,
        # double_q=tune.grid_search([False, True]),
        # num_atoms=tune.grid_search([1, 10]),
        # dueling=tune.grid_search([False, True]),
        # noisy=tune.grid_search([False, True]),
        sigma0=0.5,
        lr=0.0005,
    )
)

stop = {
    "sampler_results/episode_reward_mean": 450.0,
    "timesteps_total": 50000,
}

# ray.init(local_mode=True)


for double_q, num_atoms, dueling, noisy in product(
    *[[False, True], [1, 10], [False, True], [False, True]]
):
    config = config.training(
        double_q=double_q, num_atoms=num_atoms, dueling=dueling, noisy=noisy
    )
    tuner = tune.Tuner(
        "DQN",
        param_space=config,
        run_config=train.RunConfig(
            stop=stop,
            name="dqn_new_stack",
            callbacks=[
                WandbLoggerCallback(
                    api_key_file="data/wandb/wandb_api_key.txt",
                    project="DQN",
                    name="new_stack",
                    log_config=True,
                )
            ],
        ),
        # tune_config=tune.TuneConfig(
        #     num_samples=10,
        # )
    )
    tuner.fit()

# algo = config.build()
# for _ in range(10000):
#     results = algo.train()
#     print(f"R={results['sampler_results']['episode_reward_mean']}")
