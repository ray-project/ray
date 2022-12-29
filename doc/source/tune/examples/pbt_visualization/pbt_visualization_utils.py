import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
import numpy as np


def get_init_theta():
    return np.array([0.9, 0.9])


def Q_batch(theta):
    """Returns the true function value for a batch of parameters with size (B, 2)"""
    return 1.2 - (3 / 4 * theta[:, 0] ** 2 + theta[:, 1] ** 2)


def get_arrows(theta_history, perturbation_interval):
    theta_history = theta_history[1:, :]
    arrow_start = theta_history[
        np.arange(perturbation_interval - 1, len(theta_history), perturbation_interval)
    ]
    arrow_end = theta_history[
        np.arange(perturbation_interval, len(theta_history), perturbation_interval)
    ]
    if len(arrow_end) > len(arrow_start):
        arrow_end = arrow_end[: len(arrow_start)]
    else:
        arrow_start = arrow_start[: len(arrow_end)]
    deltas = arrow_end - arrow_start
    return arrow_start, deltas


def plot_parameter_history(
    results,
    colors,
    labels,
    perturbation_interval=None,
    fig=None,
    ax=None,
    plot_until_iter=None,
    include_colorbar=True,
):
    if fig is None or ax is None:
        fig, ax = plt.subplots()

    theta_0 = get_init_theta()

    x = np.linspace(-0.2, 1.0, 50)
    y = np.linspace(-0.2, 1.0, 50)
    xx, yy = np.meshgrid(x, y)
    xys = np.transpose(np.stack((xx, yy)).reshape(2, -1))
    contour = ax.contourf(xx, yy, Q_batch(xys).reshape(xx.shape), 20)
    ax.set_xlabel("theta0")
    ax.set_ylabel("theta1")
    ax.set_title("Q(theta)")

    scatters = []
    for i in range(len(results)):
        df = results[i].metrics_dataframe

        # Append the initial theta values to the history
        theta0_history = np.concatenate([[theta_0[0]], df["theta0"].to_numpy()])
        theta1_history = np.concatenate([[theta_0[1]], df["theta1"].to_numpy()])
        training_iters = np.concatenate([[0], df["training_iteration"].to_numpy()])

        if plot_until_iter is None:
            plot_until_iter = len(training_iters)

        scatter = ax.scatter(
            theta0_history[:plot_until_iter],
            theta1_history[:plot_until_iter],
            # Size of scatter point decreases as training iteration increases
            s=100 / ((training_iters[:plot_until_iter] + 1) ** 1 / 3),
            alpha=0.5,
            c=colors[i],
            label=labels[i],
        )
        scatters.append(scatter)
        for i, theta0, theta1 in zip(training_iters, theta0_history, theta1_history):
            if i % (perturbation_interval or 1) == 0 and i < plot_until_iter:
                ax.annotate(i, (theta0, theta1))

        if perturbation_interval is not None:
            theta_history = np.hstack(
                (theta0_history.reshape(-1, 1), theta1_history.reshape(-1, 1))
            )[:plot_until_iter, :]
            arrow_starts, deltas = get_arrows(theta_history, perturbation_interval)
            for arrow_start, delta in zip(arrow_starts, deltas):
                ax.arrow(
                    arrow_start[0],
                    arrow_start[1],
                    delta[0],
                    delta[1],
                    head_width=0.01,
                    length_includes_head=True,
                    alpha=0.25,
                )
    ax.legend(loc="upper left")
    if include_colorbar:
        fig.colorbar(contour, ax=ax, orientation="vertical")
    return scatters


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


def make_animation(
    results, colors, labels, perturbation_interval=None, filename="pbt.gif"
):
    fig, ax = plt.subplots(figsize=(8, 8))

    def animate(i):
        ax.clear()
        return plot_parameter_history(
            results,
            colors,
            labels,
            perturbation_interval=perturbation_interval,
            fig=fig,
            ax=ax,
            plot_until_iter=i,
            include_colorbar=False,
        )

    ani = FuncAnimation(
        fig, animate, interval=200, blit=True, repeat=True, frames=range(1, 101)
    )
    ani.save(filename, writer=PillowWriter())
    plt.close()
