import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
import numpy as np


def get_init_theta():
    return np.array([0.9, 0.9])


def Q_batch(theta):
    """Returns the true function value for a batch of parameters with size (B, 2)"""
    return 1.2 - (3 / 4 * theta[:, 0] ** 2 + theta[:, 1] ** 2)


def get_arrows(theta_history, perturbation_interval):
    """
    Computes the start points and deltas for arrows showing parameter perturbations.

    Args:
        theta_history: History of parameter values of shape (iterations, 2)
        perturbation_interval: Number of iterations between perturbations

    Returns:
        Tuple[np.ndarray, np.ndarray]: Arrow start points and deltas
    """
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
    """
    Plot parameter history overlaid on the true reward contour.

    Args:
        results: List of result objects containing metrics dataframes
        colors: List of colors for each result
        labels: List of labels for each result
        perturbation_interval: Interval at which parameter perturbations occur
        fig: Existing figure to plot on (creates new if None)
        ax: Existing axes to plot on (creates new if None)
        plot_until_iter: Maximum iteration to plot (plots all if None)
        include_colorbar: Whether to include a colorbar for the contour plot

    Returns:
        List of scatter plot objects
    """
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
    ax.set_title("Parameter History and True Reward Q(theta) Contour")

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
    ax.legend(loc="upper left", title="Trial Initial Parameters")
    if include_colorbar:
        cbar = fig.colorbar(contour, ax=ax, orientation="vertical")
        cbar.set_label("Reward Q(theta)")
    return scatters


def plot_Q_history(results, colors, labels, ax=None):
    """
    Plot the history of true reward values over training iterations.

    Args:
        results: List of result objects containing metrics dataframes
        colors: List of colors for each result
        labels: List of labels for each result
        ax: Existing axes to plot on (creates new if None)
    """
    if ax is None:
        fig, ax = plt.subplots()
    ax.set_title("True Reward (Q) Value Over Training Iterations")
    ax.set_xlabel("Training Iteration")
    ax.set_ylabel("Reward Q(theta)")
    for i in range(len(results)):
        df = results[i].metrics_dataframe
        ax.plot(df["Q"], label=labels[i], color=colors[i])
    ax.legend(title="Trial Initial Parameters")


def make_animation(
    results, colors, labels, perturbation_interval=None, filename="pbt.gif", fps=5
):
    fig, ax = plt.subplots(figsize=(8, 8))

    def animate(i):
        ax.clear()
        ax.set_title("Parameter Evolution Over Iterations")
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
        fig, animate, interval=1000 // fps, blit=True, repeat=True, frames=range(1, 101)
    )
    ani.save(filename, writer=PillowWriter(fps=fps))
    plt.close()
