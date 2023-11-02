""" Example of using Linear Thompson Sampling on WheelBandit environment.
    For more information on WheelBandit, see https://arxiv.org/abs/1802.09127 .
"""

import argparse
import time

import numpy as np
from matplotlib import pyplot as plt
from rllib_bandit.bandit import BanditLinTSConfig

import ray
from ray import air, tune
from ray.rllib.examples.env.bandit_envs_discrete import WheelBanditEnv


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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--framework",
        choices=["tf2", "torch"],
        default="torch",
        help="The DL framework specifier.",
    )
    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")

    ray.init(num_cpus=2)

    config = BanditLinTSConfig().environment(WheelBanditEnv).framework(args.framework)

    # Actual env steps per `train()` call will be
    # 10 * `min_sample_timesteps_per_iteration` (100 by default) = 1,000
    training_iterations = 10

    print("Running training for %s time steps" % training_iterations)

    start_time = time.time()
    tuner = tune.Tuner(
        "BanditLinTS",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"training_iteration": training_iterations},
            checkpoint_config=air.CheckpointConfig(
                checkpoint_at_end=True,
            ),
        ),
        tune_config=tune.TuneConfig(
            num_samples=1,
        ),
    )
    results = tuner.fit()

    print("The trials took", time.time() - start_time, "seconds\n")

    # Analyze cumulative regrets of the trials.
    # There is only one trial
    result = results.get_best_result()
    x = result.metrics_dataframe.groupby("agent_timesteps_total")[
        "episode_reward_mean"
    ].aggregate(["mean", "max", "min", "std"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

    ax1.plot(x["mean"])

    ax1.set_title("Episode reward mean")
    ax1.set_xlabel("Training steps")

    # Restore Algorithm from checkpoint
    checkpoint = results.get_best_result().checkpoint
    algo = config.build()
    with checkpoint.as_directory() as checkpoint_dir:
        algo.restore(checkpoint_dir)

    # Get model to plot arm weights distribution
    model = algo.get_policy().model
    means = [model.arms[i].theta.numpy() for i in range(5)]
    covs = [model.arms[i].covariance.numpy() for i in range(5)]

    # Plot weight distributions for different arms
    plot_model_weights(means, covs, ax2)
    fig.tight_layout()
    plt.show()
