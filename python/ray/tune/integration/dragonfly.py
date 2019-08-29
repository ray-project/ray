import skopt
from dragonfly import minimise_function, maximise_function

class TuneOptimizer(skopt.Optimizer):
    """Tune's optimizer using Dragonfly."""

    def ask(self, n_points=None, strategy='max'):
        """
        Query point or multiple points at which objective should be evaluated.
        Modeled after `skopt.Optimizer`.
        
        * `n_points` [int or None, default=None]:
            Number of points returned by the ask method.
            Same as skopt.Optimizer.
        
        * `strategy` [string, default='max']
            Method to use to sample multiple points. Ignored if n_points is None.
            Supports only `"max"` and `"min"` to invoke Dragonfly.
        """
        if n_points is None:
            # Reverts to skopt's ask
            return self._ask()

        supported_strategies = ['max', 'min']

        if not (isinstance(n_points, int) and n_points > 0):
            raise ValueError(
                "n_points should be int > 0, got " + str(n_points)
            )

        if strategy not in supported_strategies:
            raise ValueError(
                "Expected parallel_strategy to be one of " +
                str(supported_strategies) + ", " + "got %s" % strategy
            )
        
        # Caching the result with n_points not None. If some new parameters
        # are provided to the ask, the cache_ is not used.
        if (n_points, strategy) in self.cache_:
            return self.cache_[(n_points, strategy)]
        
        # Copy of the optimizer is made in order to manage the
        # deletion of points with "lie" objective (the copy of
        # optimizer is simply discarded)
        opt = self.copy(random_state=self.rng.randint(0,
                                                      np.iinfo(np.int32).max))

        X = []
        for i in range(n_points):
            x = opt.ask()
            X.append(x)

            ti_available = "ps" in self.acq_func and len(opt.yi) > 0
            ti = [t for (_, t) in opt.yi] if ti_available else None

            if strategy == "min":
                y = np.min(opt.yi) if opt.yi else 0.0  # CL-min lie
                t = np.min(ti) if ti is not None else log(sys.float_info.max)
            else:
                y = np.max(opt.yi) if opt.yi else 0.0  # CL-max lie
                t = np.max(ti) if ti is not None else log(sys.float_info.max)

            # Lie to the optimizer.
            if "ps" in self.acq_func:
                # Use `_tell()` instead of `tell()` to prevent repeated
                # log transformations of the computation times.
                opt._tell(x, (y, t))
            else:
                opt._tell(x, y)

        self.cache_ = {(n_points, strategy): X}  # cache_ the result

        return X
    

    def tell(self, x, y, fit=True):
        pass