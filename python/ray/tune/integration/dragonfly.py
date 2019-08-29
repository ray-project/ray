import skopt
from dragonfly import minimise_function, maximise_function

class TuneOptimizer(skopt.Optimizer):
    """Tune's optimizer using Dragonfly."""

    def ask(self, n_points=None, strategy='cl_min'):
        """
        Query point or multiple points at which objective should be evaluated.
        Modeled after `skopt.Optimizer`.
        
        * `n_points` [int or None, default=None]:
            Number of points returned by the ask method.
            Same as skopt.Optimizer.
        
        * `strategy` [string, default='cl_min']
            Method to use to sample multiple points. Ignored.
        """
        if n_points is None:
            # Reverts to skopt's ask
            return self._ask()

        X = []
        for i in range(n_points):
            X.append(self._ask())

        return X
        