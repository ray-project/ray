import numpy as np
from scipy.optimize import minimize

import GPy
from GPy.kern import Kern
from GPy.core import Param
from sklearn.metrics import pairwise_distances
from sklearn.metrics.pairwise import euclidean_distances


## PB2 time varying kernel
class TV_SquaredExp(Kern):
    def __init__(self,
                 input_dim,
                 variance=1.,
                 lengthscale=1.,
                 epsilon=0.,
                 active_dims=None):
        super().__init__(input_dim, active_dims, 'time_se')
        self.variance = Param('variance', variance)
        self.lengthscale = Param('lengthscale', lengthscale)
        self.epsilon = Param('epsilon', epsilon)
        self.link_parameters(self.variance, self.lengthscale, self.epsilon)

    def K(self, X, X2):
        # time must be in the far left column
        if self.epsilon > 0.5:  # 0.5
            self.epsilon = 0.5
        if X2 is None:
            X2 = np.copy(X)
        T1 = X[:, 0].reshape(-1, 1)
        T2 = X2[:, 0].reshape(-1, 1)
        dists = pairwise_distances(T1, T2, 'cityblock')
        timekernel = (1 - self.epsilon)**(0.5 * dists)

        X = X[:, 1:]
        X2 = X2[:, 1:]

        RBF = self.variance * np.exp(
            -np.square(euclidean_distances(X, X2)) / self.lengthscale)

        return RBF * timekernel

    def Kdiag(self, X):
        return self.variance * np.ones(X.shape[0])

    def update_gradients_full(self, dL_dK, X, X2):
        if X2 is None:
            X2 = np.copy(X)
        T1 = X[:, 0].reshape(-1, 1)
        T2 = X2[:, 0].reshape(-1, 1)

        X = X[:, 1:]
        X2 = X2[:, 1:]
        dist2 = np.square(euclidean_distances(X, X2)) / self.lengthscale

        dvar = np.exp(-np.square(
            (euclidean_distances(X, X2)) / self.lengthscale))
        dl = -(2 * euclidean_distances(X, X2)**2 * self.variance *
               np.exp(-dist2)) * self.lengthscale**(-2)
        n = pairwise_distances(T1, T2, 'cityblock') / 2
        deps = -n * (1 - self.epsilon)**(n - 1)

        self.variance.gradient = np.sum(dvar * dL_dK)
        self.lengthscale.gradient = np.sum(dl * dL_dK)
        self.epsilon.gradient = np.sum(deps * dL_dK)


## PB2 data normalizing functions
def normalize(data, wrt):
    # data = data to normalize
    # wrt = data will be normalized with respect to this
    return (data - np.min(wrt, axis=0)) / (
        np.max(wrt, axis=0) - np.min(wrt, axis=0))


def standardize(data):
    data = (data - np.mean(data, axis=0)) / (np.std(data, axis=0) + 1e-8)
    return np.clip(data, -2, 2)


## UCB acquisition function
def UCB(m, m1, x, fixed, kappa=0.5):

    c1 = 0.2
    c2 = 0.4
    beta_t = c1 * np.log(c2 * m.X.shape[0])
    kappa = np.sqrt(beta_t)

    xtest = np.concatenate((fixed.reshape(-1, 1), np.array(x).reshape(-1,
                                                                      1))).T

    preds = m.predict(xtest)
    mean = preds[0][0][0]

    preds = m1.predict(xtest)
    var = preds[1][0][0]
    return mean + kappa * var


## optimize acquisition function.
def optimize_acq(func, m, m1, fixed, num_f):

    print("Optimizing Acquisition Function...\n")

    opts = {'maxiter': 200, 'maxfun': 200, 'disp': False}

    T = 10
    best_value = -999
    best_theta = m1.X[0, :]

    bounds = [(0, 1) for _ in range(m.X.shape[1] - num_f)]

    for ii in range(T):
        x0 = np.random.uniform(0, 1, m.X.shape[1] - num_f)

        res = minimize(
            lambda x: -func(m, m1, x, fixed),
            x0,
            bounds=bounds,
            method="L-BFGS-B",
            options=opts)

        val = func(m, m1, res.x, fixed)
        if val > best_value:
            best_value = val
            best_theta = res.x

    return (np.clip(best_theta, 0, 1))


## Select the number of datapoints to keep, using cross validation
def select_length(Xraw, yraw, bounds, num_f):

    min_len = 200

    if Xraw.shape[0] < min_len:
        return (Xraw.shape[0])
    else:
        length = min_len - 10
        scores = []
        while length + 10 <= Xraw.shape[0]:
            length += 10

            base_vals = np.array(list(bounds.values())).T
            X_len = Xraw[-length:, :]
            y_len = yraw[-length:]
            oldpoints = X_len[:, :num_f]
            old_lims = np.concatenate((np.max(oldpoints, axis=0),
                                       np.min(oldpoints, axis=0))).reshape(
                                           2, oldpoints.shape[1])
            limits = np.concatenate((old_lims, base_vals), axis=1)

            X = normalize(X_len, limits)
            y = standardize(y_len).reshape(y_len.size, 1)

            kernel = TV_SquaredExp(
                input_dim=X.shape[1], variance=1., lengthscale=1., epsilon=0.1)
            m = GPy.models.GPRegression(X, y, kernel)
            m.optimize(messages=True)

            scores.append(m.log_likelihood())
        idx = np.argmax(scores)
        length = (idx + int((min_len / 10))) * 10
        return (length)
