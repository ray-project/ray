""" Example of using LinUCB on a recommendation environment with parametric
    actions. """

import os
import time

from matplotlib import pyplot as plt
import pandas as pd

from ray import tune
from ray.rllib.contrib.bandits.agents.lin_ucb import UCB_CONFIG
from ray.rllib.contrib.bandits.envs import ParametricItemRecoEnv

if __name__ == "__main__":

    # Temp fix to avoid OMP conflict
    os.environ["KMP_DUPLICATE_LIB_OK"] = "True"

    UCB_CONFIG["env"] = ParametricItemRecoEnv

    # Actual training_iterations will be 20 * timesteps_per_iteration
    # (100 by default) = 2,000
    training_iterations = 20

    print("Running training for %s time steps" % training_iterations)

    start_time = time.time()
    analysis = tune.run(
        "contrib/LinUCB",
        config=UCB_CONFIG,
        stop={"training_iteration": training_iterations},
        num_samples=5,
        checkpoint_at_end=False)

    print("The trials took", time.time() - start_time, "seconds\n")

    # Analyze cumulative regrets of the trials
    frame = pd.DataFrame()
    for key, df in analysis.trial_dataframes.items():
        frame = frame.append(df, ignore_index=True)
    x = frame.groupby("num_steps_trained")[
        "learner/cumulative_regret"].aggregate(["mean", "max", "min", "std"])

    plt.plot(x["mean"])
    plt.fill_between(
        x.index,
        x["mean"] - x["std"],
        x["mean"] + x["std"],
        color="b",
        alpha=0.2)
    plt.title("Cumulative Regret")
    plt.xlabel("Training steps")
    plt.show()
