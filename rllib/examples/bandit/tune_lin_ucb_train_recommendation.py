""" Example of using LinUCB on a recommendation environment with parametric
    actions. """

from matplotlib import pyplot as plt
import os
import pandas as pd
import time

from ray import tune
from ray.rllib.examples.env.bandit_envs_recommender_system import ParametricItemRecoEnv

if __name__ == "__main__":
    # Temp fix to avoid OMP conflict
    os.environ["KMP_DUPLICATE_LIB_OK"] = "True"

    config = {
        "env": ParametricItemRecoEnv,
    }

    # Actual training_iterations will be 10 * timesteps_per_iteration
    # (100 by default) = 2,000
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
