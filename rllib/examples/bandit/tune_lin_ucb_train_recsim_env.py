"""Example of using LinUCB on a RecSim environment. """

import argparse
from matplotlib import pyplot as plt
import pandas as pd
import time

from ray import air, tune
import ray.rllib.examples.env.recommender_system_envs_with_recsim  # noqa


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--framework",
        choices=["tf2", "torch"],
        default="torch",
        help="The DL framework specifier.",
    )
    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")

    ray.init()

    config = {
        # "RecSim-v1" is a pre-registered RecSim env.
        # Alternatively, you can do:
        # `from ray.rllib.examples.env.recommender_system_envs_with_recsim import ...`
        # - LongTermSatisfactionRecSimEnv
        # - InterestExplorationRecSimEnv
        # - InterestEvolutionRecSimEnv
        # Then: "env": [the imported RecSim class]
        "env": "RecSim-v1",
        "env_config": {
            "num_candidates": 10,
            "slate_size": 1,
            "convert_to_discrete_action_space": True,
            "wrap_for_bandits": True,
        },
        "framework": args.framework,
        "eager_tracing": (args.framework == "tf2"),
    }

    # Actual env timesteps per `train()` call will be
    # 10 * min_sample_timesteps_per_iteration (100 by default) = 1,000
    training_iterations = 5000

    print("Running training for %s time steps" % training_iterations)

    start_time = time.time()
    tuner = tune.Tuner(
        "BanditLinUCB",
        param_space=config,
        run_config=air.RunConfig(
            stop={"training_iteration": training_iterations},
            checkpoint_confnig=air.CheckpointConfig(
                checkpoint_at_end=False,
            ),
        ),
        tune_config=tune.TuneConfig(
            num_samples=1,
        ),
    )
    results = tuner.fit()

    print("The trials took", time.time() - start_time, "seconds\n")

    # Analyze cumulative regrets of the trials
    frame = pd.DataFrame()
    for result in results:
        frame = frame.append(result.metrics_dataframe, ignore_index=True)
    x = frame.groupby("agent_timesteps_total")["episode_reward_mean"].aggregate(
        ["mean", "max", "min", "std"]
    )

    plt.plot(x["mean"])
    plt.fill_between(
        x.index, x["mean"] - x["std"], x["mean"] + x["std"], color="b", alpha=0.2
    )
    plt.title("Episode reward mean")
    plt.xlabel("Training steps")
    plt.show()
