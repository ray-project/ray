""" Example of using LinUCB on a recommendation environment with parametric
    actions. """

import argparse
from matplotlib import pyplot as plt
import os
import pandas as pd
import time

import ray
from ray import tune
from ray.tune import register_env
from ray.rllib.env.wrappers.recsim import (
    MultiDiscreteToDiscreteActionWrapper,
    RecSimObservationBanditWrapper,
)
from ray.rllib.examples.env.bandit_envs_recommender_system import (
    ParametricRecSys,
)

# Because ParametricRecSys follows RecSim's API, we have to wrap it before
# it can work with our Bandits agent.
register_env(
    "ParametricRecSysEnv",
    lambda cfg: MultiDiscreteToDiscreteActionWrapper(
        RecSimObservationBanditWrapper(ParametricRecSys(**cfg))
    ),
)

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

    # Temp fix to avoid OMP conflict.
    os.environ["KMP_DUPLICATE_LIB_OK"] = "True"

    ray.init()

    config = {
        "framework": args.framework,
        "eager_tracing": (args.framework == "tf2"),
        "env": "ParametricRecSysEnv",
        "env_config": {
            "embedding_size": 20,
            "num_docs_to_select_from": 10,
            "slate_size": 1,
            "num_docs_in_db": 100,
            "num_users_in_db": 1,
            "user_time_budget": 1.0,
        },
        "num_envs_per_worker": 2,  # Test with batched inference.
        "evaluation_interval": 20,
        "evaluation_duration": 100,
        "evaluation_duration_unit": "episodes",
        "simple_optimizer": True,
    }

    # Actual env timesteps per `train()` call will be
    # 10 * min_sample_timesteps_per_reporting (100 by default) = 1,000.
    training_iterations = 10

    print("Running training for %s time steps" % training_iterations)

    start_time = time.time()
    analysis = tune.run(
        "BanditLinUCB",
        config=config,
        stop={"training_iteration": training_iterations},
        num_samples=2,
        checkpoint_at_end=False,
    )

    print("The trials took", time.time() - start_time, "seconds\n")

    # Analyze cumulative regrets of the trials
    frame = pd.DataFrame()
    for key, df in analysis.trial_dataframes.items():
        frame = frame.append(df, ignore_index=True)
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
