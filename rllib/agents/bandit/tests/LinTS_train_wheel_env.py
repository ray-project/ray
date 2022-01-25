""" Example of using Linear Thompson Sampling on WheelBandit environment.
    For more information on WheelBandit, see https://arxiv.org/abs/1802.09127 .
"""

import numpy as np
from matplotlib import pyplot as plt

from ray.rllib.agents.bandit.bandit import BanditLinTSTrainer
from ray.rllib.examples.env.bandit_envs_discrete import WheelBanditEnv
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO


def plot_model_weights(means, covs):
    fmts = ["bo", "ro", "yx", "k+", "gx"]
    labels = ["arm{}".format(i) for i in range(5)]

    fig, ax = plt.subplots(figsize=(6, 4))

    ax.set_title("Weights distributions of arms")

    for i in range(0, 5):
        x, y = np.random.multivariate_normal(means[i] / 30, covs[i], 5000).T
        ax.plot(x, y, fmts[i], label=labels[i])

    ax.grid(True, which="both")
    ax.axhline(y=0, color="k")
    ax.axvline(x=0, color="k")
    ax.legend(loc="best")
    plt.show()


if __name__ == "__main__":
    num_iter = 10
    print("Running training for %s time steps" % num_iter)
    trainer = BanditLinTSTrainer(env=WheelBanditEnv)

    policy = trainer.get_policy()
    model = policy.model

    print("Using exploration strategy:", policy.exploration)
    print("Using model:", model)

    for i in range(num_iter):
        trainer.train()

    info = trainer.train()
    print(info["info"][LEARNER_INFO])

    # Get model parameters
    means = [model.arms[i].theta.numpy() for i in range(5)]
    covs = [model.arms[i].covariance.numpy() for i in range(5)]

    # Plot weight distributions for different arms
    plot_model_weights(means, covs)
