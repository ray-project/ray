import numpy as np
import skopt

from dragonfly import minimise_function, maximise_function

from skopt.acquisition import _gaussian_acquisition
from skopt.utils import is_listlike, is_2Dlistlike

class TuneOptimizer(skopt.Optimizer):
    """Tune's optimizer using Dragonfly."""

    def ask(self, n_points=None, strategy='cl_min'):
        """Query point or multiple points at which objective should be evaluated.
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
    
    def _tell(self, x, y, fit=True):
        """Perform the actual work of incorporating one or more new points.
        Overridden to compute the next point differently to return when ask is called.
        """
        if "ps" in self.acq_func:
            if is_2Dlistlike(x):
                self.Xi.extend(x)
                self.yi.extend(y)
                self._n_initial_points -= len(y)
            elif is_listlike(x):
                self.Xi.append(x)
                self.yi.append(y)
                self._n_initial_points -= 1
        # if y isn't a scalar it means we have been handed a batch of points
        elif is_listlike(y) and is_2Dlistlike(x):
            self.Xi.extend(x)
            self.yi.extend(y)
            self._n_initial_points -= len(y)
        elif is_listlike(x):
            self.Xi.append(x)
            self.yi.append(y)
            self._n_initial_points -= 1
        else:
            raise ValueError("Type of arguments `x` (%s) and `y` (%s) "
                             "not compatible." % (type(x), type(y)))

        # optimizer learned something new - discard cache
        self.cache_ = {}

        # after being "told" n_initial_points we switch from sampling
        # random points to using a surrogate model
        if (fit and self._n_initial_points <= 0 and
           self.base_estimator_ is not None):
            transformed_bounds = np.array(self.space.transformed_bounds)
            est = clone(self.base_estimator_)

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                est.fit(self.space.transform(self.Xi), self.yi)

            if hasattr(self, "next_xs_") and self.acq_func == "gp_hedge":
                self.gains_ -= est.predict(np.vstack(self.next_xs_))
            self.models.append(est)

            # even with BFGS as optimizer we want to sample a large number
            # of points and then pick the best ones as starting points
            X = self.space.transform(self.space.rvs(
                n_samples=self.n_points, random_state=self.rng))

            self.next_xs_ = []
            max_captial = 60
            for cand_acq_func in self.cand_acq_funcs_:
                values = _gaussian_acquisition(
                    X=X, model=est, y_opt=np.min(self.yi),
                    acq_func=cand_acq_func,
                    acq_func_kwargs=self.acq_func_kwargs)
                _, next_x, _ = minimise_function(_gaussian_acquisition, self.acq_func_kwargs, max_capital)

            if self.acq_func == "gp_hedge":
                logits = np.array(self.gains_)
                logits -= np.max(logits)
                exp_logits = np.exp(self.eta * logits)
                probs = exp_logits / np.sum(exp_logits)
                next_x = self.next_xs_[np.argmax(self.rng.multinomial(1,
                                                                      probs))]
            else:
                next_x = self.next_xs_[0]

            # note the need for [0] at the end
            self._next_x = self.space.inverse_transform(
                next_x.reshape((1, -1)))[0]

        # Pack results
        return create_result(self.Xi, self.yi, self.space, self.rng,
                             models=self.models)
