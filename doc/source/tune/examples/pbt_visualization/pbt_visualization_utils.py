import matplotlib.pyplot as plt
import numpy as np


def get_init_theta():
    return np.array([0.9, 0.9])


def Q_batch(theta):
    return 1.2 - (3/4 * theta[:, 0] ** 2 + theta[:, 1] ** 2)


def get_arrows(theta_history, perturbation_interval):
    theta_history = theta_history[1:, :]
    arrow_start = theta_history[np.arange(perturbation_interval - 1, len(theta_history), perturbation_interval)]
    arrow_end = theta_history[np.arange(perturbation_interval, len(theta_history), perturbation_interval)]
    if len(arrow_end) > len(arrow_start):
        arrow_end = arrow_end[:len(arrow_start)]
    else:
        arrow_start = arrow_start[:len(arrow_end)]
    deltas = arrow_end - arrow_start
    return arrow_start, deltas


def plot_parameter_history(results, colors, labels, perturbation_interval=None, ax=None):
    if ax is None:
        fig, ax = plt.subplots()

    theta_0 = get_init_theta()

    x = np.linspace(-0.2, 1.0, 50)
    y = np.linspace(-0.2, 1.0, 50)
    xx, yy = np.meshgrid(x, y)
    xys = np.transpose(np.stack((xx, yy)).reshape(2, -1))
    ax.contourf(xx, yy, Q_batch(xys).reshape(xx.shape), 25)
    ax.set_xlabel("theta0")
    ax.set_ylabel("theta1")
    ax.set_title("Q(theta0, theta1)")

    for i in range(len(results)):
        df = results[i].metrics_dataframe

        theta0_history = np.concatenate([
            [theta_0[0]],
            df["theta0"].to_numpy()
        ])
        theta1_history = np.concatenate([
            [theta_0[1]],
            df["theta1"].to_numpy()
        ])
        training_iters = np.concatenate([[0], df["training_iteration"].to_numpy()])

        ax.scatter(
            theta0_history,
            theta1_history,
            s=200 / ((training_iters + 1) ** 1/3),
            c=colors[i],
            alpha=0.5,
            label=labels[i],
        )
        for i, theta0, theta1 in zip(training_iters, theta0_history, theta1_history):
            if i < 20:
                ax.annotate(i, (theta0, theta1))

        if perturbation_interval is not None:
            theta_history = np.hstack((
                theta0_history.reshape(-1, 1),
                theta1_history.reshape(-1, 1))
            )
            arrow_starts, deltas = get_arrows(theta_history, perturbation_interval)
            for arrow_start, delta in zip(arrow_starts, deltas):
                ax.arrow(
                    arrow_start[0], arrow_start[1], delta[0], delta[1],
                    head_width=0.01,
                    length_includes_head=True,
                    alpha=0.25,
                )

    ax.legend()


def plot_Q_history(results, colors, labels, ax=None):
    if ax is None:
        fig, ax = plt.subplots()
    ax.set_title("True function (Q) value over training iterations")
    ax.set_xlabel("training_iteration")
    ax.set_ylabel("Q(theta)")
    for i in range(len(results)):
        df = results[i].metrics_dataframe
        ax.plot(df["Q"], label=labels[i], color=colors[i])
    ax.legend()