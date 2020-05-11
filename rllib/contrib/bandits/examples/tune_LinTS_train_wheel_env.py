""" Example of using Linear Thompson Sampling on WheelBandit environment.
    For more information on WheelBandit, see https://arxiv.org/abs/1802.09127 .
"""

import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from ray import tune
from ray.rllib.contrib.bandits.agents import LinTSTrainer
from ray.rllib.contrib.bandits.agents.lin_ts import TS_CONFIG
from ray.rllib.contrib.bandits.envs import WheelBanditEnv


def plot_model_weights(means, covs, ax):
    fmts = ["bo", "ro", "yx", "k+", "gx"]
    labels = ["arm{}".format(i) for i in range(5)]

    ax.set_title("Weights distributions of arms")

    for i in range(0, 5):
        x, y = np.random.multivariate_normal(means[i] / 30, covs[i], 5000).T
        ax.plot(x, y, fmts[i], label=labels[i])

    ax.set_aspect("equal")
    ax.grid(True, which="both")
    ax.axhline(y=0, color="k")
    ax.axvline(x=0, color="k")
    ax.legend(loc="best")


if __name__ == "__main__":
    TS_CONFIG["env"] = WheelBanditEnv

    # Actual training_iterations will be 20 * timesteps_per_iteration
    # (100 by default) = 2,000
    training_iterations = 20

    print("Running training for %s time steps" % training_iterations)

    start_time = time.time()
    analysis = tune.run(
        LinTSTrainer,
        config=TS_CONFIG,
        stop={"training_iteration": training_iterations},
        num_samples=2,
        checkpoint_at_end=True)

    print("The trials took", time.time() - start_time, "seconds\n")

    # Analyze cumulative regrets of the trials
    frame = pd.DataFrame()
    for key, df in analysis.trial_dataframes.items():
        frame = frame.append(df, ignore_index=True)

    x = frame.groupby("num_steps_trained")[
        "learner/cumulative_regret"].aggregate(["mean", "max", "min", "std"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

    ax1.plot(x["mean"])

    ax1.set_title("Cumulative Regret")
    ax1.set_xlabel("Training steps")

    # Restore trainer from checkpoint
    trial = analysis.trials[0]
    trainer = LinTSTrainer(config=TS_CONFIG)
    trainer.restore(trial.checkpoint.value)

    # Get model to plot arm weights distribution
    model = trainer.get_policy().model
    means = [model.arms[i].theta.numpy() for i in range(5)]
    covs = [model.arms[i].covariance.numpy() for i in range(5)]

    # Plot weight distributions for different arms
    plot_model_weights(means, covs, ax2)
    fig.tight_layout()
    plt.show()
