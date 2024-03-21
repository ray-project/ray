import ray
from itertools import product
from ray import train, tune
from ray.air.integrations.wandb import WandbLoggerCallback
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .framework(framework="torch")
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        # SB3
        #rollout_fragment_length=256,
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=0,
    )
    .resources(
        num_learner_workers=0,
    )
    .training(
        model={
            "fcnet_hiddens": [256],
            "fcnet_activation": "relu",
            "epsilon": [(0, 1.0), (10000, 0.05)],
            # "fcnet_weights_initializer": "xavier_uniform_",
            # "post_fcnet_weights_initializer": "xavier_uniform_",
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        # SB3
        # train_batch_size=64,
        # training_intensity=32,
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
        # double_q=tune.grid_search([False, True]),
        # num_atoms=tune.grid_search([1, 10]),
        # dueling=tune.grid_search([False, True]),
        # noisy=tune.grid_search([False, True]),
        sigma0=0.2,
        lr=0.0005,
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
        evaluation_num_workers=1,
        evaluation_duration="auto",
        evaluation_config={
            "explore": False,
        },
    )
)

stop = {
    "evaluation/sampler_results/episode_reward_mean": 500.0,
    "timesteps_total": 100000,
}

#ray.init(local_mode=True)
tuner = tune.Tuner(
    "DQN",
    param_space=config,
    run_config=train.RunConfig(
        stop=stop,
        name="dqn_new_stack_bm",
        callbacks=[
            WandbLoggerCallback(
                api_key_file="data/wandb/wandb_api_key.txt",
                project="DQN_Benchmarks",
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

# for double_q, num_atoms, dueling, noisy in product(
#     *[[False, True], [1, 10], [False, True], [False, True]]
# ):
#     print("Running trial with:")
#     print(f"double_q={double_q}")
#     print(f"num_atoms={num_atoms}")
#     print(f"dueling={dueling}")
#     print(f"noisy={noisy}")
#     config = (
#         DQNConfig()
#         .environment(env="CartPole-v1")
#         .framework(framework="torch")
#         .experimental(_enable_new_api_stack=True)
#         .rollouts(
#             env_runner_cls=SingleAgentEnvRunner,
#             num_rollout_workers=0,
#         )
#         .resources(
#             num_learner_workers=0,
#         )
#         .training(
#             model={
#                 "fcnet_hiddens": [256],
#                 "fcnet_activation": "relu",
#                 "epsilon": [(0, 1.0), (10000, 0.05)],
#                 # "fcnet_weights_initializer": "xavier_uniform_",
#                 # "post_fcnet_weights_initializer": "xavier_uniform_",
#                 "fcnet_bias_initializer": "zeros_",
#                 "post_fcnet_bias_initializer": "zeros_",
#                 "post_fcnet_hiddens": [256],
#             },
#             replay_buffer_config={
#                 "type": "PrioritizedEpisodeReplayBuffer",
#                 "capacity": 50000,
#                 "alpha": 0.6,
#                 "beta": 0.4,
#             },
#             double_q=double_q,
#             num_atoms=num_atoms,
#             noisy=noisy,
#             dueling=dueling,
#             # double_q=tune.grid_search([False, True]),
#             # num_atoms=tune.grid_search([1, 10]),
#             # dueling=tune.grid_search([False, True]),
#             # noisy=tune.grid_search([False, True]),
#             sigma0=0.2,
#             lr=0.0005,
#         )
#     )
#     tuner = tune.Tuner(
#         "DQN",
#         param_space=config,
#         run_config=train.RunConfig(
#             stop=stop,
#             name="dqn_new_stack",
#             callbacks=[
#                 WandbLoggerCallback(
#                     api_key_file="data/wandb/wandb_api_key.txt",
#                     project="DQN",
#                     name="new_stack",
#                     log_config=True,
#                 )
#             ],
#         ),
#         # tune_config=tune.TuneConfig(
#         #     num_samples=10,
#         # )
#     )
#     tuner.fit()

# algo = config.build()
# for _ in range(stop["timesteps_total"]):
#     results = algo.train()
#     R = results["sampler_results"]["episode_reward_mean"]
#     print(f"R={results['sampler_results']['episode_reward_mean']}")
#     if R >= stop["sampler_results/episode_reward_mean"]:
#         break
