import numpy as np
from scipy.optimize import minimize
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Hyperparameter, Kernel
from sklearn.metrics import pairwise_distances
from sklearn.metrics.pairwise import euclidean_distances


class TV_SquaredExp(Kernel):
    """Time varying squared exponential kernel.
    For more info see the TV-GP-UCB paper:
    http://proceedings.mlr.press/v51/bogunovic16.pdf
    """

    def __init__(
        self,
        variance=1.0,
        lengthscale=1.0,
        epsilon=0.1,
        variance_bounds=(1e-5, 1e5),
        lengthscale_bounds=(1e-5, 1e5),
        epsilon_bounds=(1e-5, 0.5),
    ):
        self.variance = variance
        self.lengthscale = lengthscale
        self.epsilon = epsilon
        self.variance_bounds = variance_bounds
        self.lengthscale_bounds = lengthscale_bounds
        self.epsilon_bounds = epsilon_bounds

    @property
    def hyperparameter_variance(self):
        return Hyperparameter("variance", "numeric", self.variance_bounds)

    @property
    def hyperparameter_lengthscale(self):
        return Hyperparameter("lengthscale", "numeric", self.lengthscale_bounds)

    @property
    def hyperparameter_epsilon(self):
        return Hyperparameter("epsilon", "numeric", self.epsilon_bounds)

    def __call__(self, X, Y=None, eval_gradient=False):
        X = np.atleast_2d(X)
        if Y is None:
            Y = X

        epsilon = np.clip(self.epsilon, 1e-5, 0.5)

        # Time must be in the first column
        T1 = X[:, 0].reshape(-1, 1)
        T2 = Y[:, 0].reshape(-1, 1)
        dists = pairwise_distances(T1, T2, "cityblock")
        timekernel = (1 - epsilon) ** (0.5 * dists)

        # RBF kernel on remaining features
        X_spatial = X[:, 1:]
        Y_spatial = Y[:, 1:]
        rbf = self.variance * np.exp(
            -np.square(euclidean_distances(X_spatial, Y_spatial)) / self.lengthscale
        )

        K = rbf * timekernel

        if eval_gradient:
            K_gradient_variance = K
            dist2 = np.square(euclidean_distances(X_spatial, Y_spatial))
            K_gradient_lengthscale = K * dist2 / self.lengthscale
            n = dists / 2
            K_gradient_epsilon = -K * n * epsilon / (1 - epsilon)
            return K, np.dstack(
                [K_gradient_variance, K_gradient_lengthscale, K_gradient_epsilon]
            )

        return K

    def diag(self, X):
        return np.full(X.shape[0], self.variance, dtype=np.float64)

    def is_stationary(self):
        return False

    @property
    def theta(self):
        return np.log([self.variance, self.lengthscale, self.epsilon])

    @theta.setter
    def theta(self, theta):
        self.variance = np.exp(theta[0])
        self.lengthscale = np.exp(theta[1])
        self.epsilon = np.exp(theta[2])

    @property
    def bounds(self):
        return np.log(
            [
                list(self.variance_bounds),
                list(self.lengthscale_bounds),
                list(self.epsilon_bounds),
            ]
        )


def normalize(data, wrt):
    """Normalize data to be in range (0,1), with respect to (wrt) boundaries,
    which can be specified.
    """
    return (data - np.min(wrt, axis=0)) / (
        np.max(wrt, axis=0) - np.min(wrt, axis=0) + 1e-8
    )


def standardize(data):
    """Standardize to be Gaussian N(0,1). Clip final values."""
    data = (data - np.mean(data, axis=0)) / (np.std(data, axis=0) + 1e-8)
    return np.clip(data, -2, 2)


def UCB(m, m1, x, fixed, kappa=None):
    """UCB acquisition function. Interesting points to note:
    1) We concat with the fixed points, because we are not optimizing wrt
       these. This is the Reward and Time, which we can't change. We want
       to find the best hyperparameters *given* the reward and time.
    2) We use m to get the mean and m1 to get the variance. If we already
       have trials running, then m1 contains this information. This reduces
       the variance at points currently running, even if we don't have
       their label.
       Ref: https://jmlr.org/papers/volume15/desautels14a/desautels14a.pdf

    """
    c1 = 0.2
    c2 = 0.4
    beta_t = c1 + max(0, np.log(c2 * m.X_train_.shape[0]))
    kappa = np.sqrt(beta_t) if kappa is None else kappa

    xtest = np.concatenate((fixed.reshape(-1, 1), np.array(x).reshape(-1, 1))).T

    try:
        mean = m.predict(xtest)[0]
    except ValueError:
        mean = -9999

    try:
        _, std = m1.predict(xtest, return_std=True)
        var = std[0] ** 2
    except ValueError:
        var = 0
    return mean + kappa * var


def optimize_acq(func, m, m1, fixed, num_f):
    """Optimize acquisition function."""

    opts = {"maxiter": 200, "maxfun": 200, "disp": False}

    T = 10
    best_value = -999
    best_theta = m1.X_train_[0, :]

    bounds = [(0, 1) for _ in range(m.X_train_.shape[1] - num_f)]

    for ii in range(T):
        x0 = np.random.uniform(0, 1, m.X_train_.shape[1] - num_f)

        res = minimize(
            lambda x: -func(m, m1, x, fixed),
            x0,
            bounds=bounds,
            method="L-BFGS-B",
            options=opts,
        )

        val = func(m, m1, res.x, fixed)
        if val > best_value:
            best_value = val
            best_theta = res.x

    return np.clip(best_theta, 0, 1)


def select_length(Xraw, yraw, bounds, num_f):
    """Select the number of datapoints to keep, using cross validation"""
    min_len = 200

    if Xraw.shape[0] < min_len:
        return Xraw.shape[0]
    else:
        length = min_len - 10
        scores = []
        while length + 10 <= Xraw.shape[0]:
            length += 10

            base_vals = np.array(list(bounds.values())).T
            X_len = Xraw[-length:, :]
            y_len = yraw[-length:]
            oldpoints = X_len[:, :num_f]
            old_lims = np.concatenate(
                (np.max(oldpoints, axis=0), np.min(oldpoints, axis=0))
            ).reshape(2, oldpoints.shape[1])
            limits = np.concatenate((old_lims, base_vals), axis=1)

            X = normalize(X_len, limits)
            y = standardize(y_len).reshape(y_len.size, 1)

            kernel = TV_SquaredExp(variance=1.0, lengthscale=1.0, epsilon=0.1)
            m = GaussianProcessRegressor(kernel=kernel, optimizer="fmin_l_bfgs_b")
            m.fit(X, y)

            scores.append(m.log_marginal_likelihood_value_)
        idx = np.argmax(scores)
        length = (idx + int((min_len / 10))) * 10
        return length
