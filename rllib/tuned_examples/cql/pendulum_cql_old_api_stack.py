from ray.rllib.algorithms.cql import CQLConfig


config = (
    CQLConfig()
    .environment("Pendulum-v1")
    .framework("torch")
    .offline_data(
        input_="dataset",
        input_config={
            "paths": ["tests/data/pendulum/enormous.zip"],
            "format": "json",
        },
        actions_in_input_normalized=True,
    )
    .env_runners(
        num_env_runners=2,
    )
    .training(
        bc_iters=100,
        train_batch_size=2000,
        twin_q=False,
    )
    .reporting(
        min_time_s_per_iteration=10,
        metrics_num_episodes_for_smoothing=5,
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_env_runners=2,
        evaluation_duration=10,
        evaluation_config={
            "input": "sampler",
            "explore": False,
        },
    )
)

algo = config.build()


for i in range(100):
    print(f"Iteration: {i}")
    results = algo.train()
    print(results)
