import logging
import pickle
try:
    import nevergrad as ng
except ImportError:
    ng = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class NevergradSearch(Searcher):
    """Uses Nevergrad to optimize hyperparameters.

    Nevergrad is an open source tool from Facebook for derivative free
    optimization.  More info can be found at:
    https://github.com/facebookresearch/nevergrad.

    You will need to install Nevergrad via the following command:

    .. code-block:: bash

        $ pip install nevergrad

    This algorithm requires using an optimizer provided by Nevergrad, of
    which there are many options. A good rundown can be found on
    the `Nevergrad README's Optimization section`_.

    .. code-block:: python

        from nevergrad.optimization import optimizerlib

        instrumentation = 1
        optimizer = optimizerlib.OnePlusOne(instrumentation, budget=100)
        algo = NevergradSearch(
            optimizer, ["lr"], metric="mean_loss", mode="min")

    Parameters:
        optimizer (nevergrad.optimization.Optimizer): Optimizer provided
            from Nevergrad.
        parameter_names (list): List of parameter names. Should match
            the dimension of the optimizer output. Alternatively, set to None
            if the optimizer is already instrumented with kwargs
            (see nevergrad v0.2.0+).
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        use_early_stopped_trials: Deprecated.
        max_concurrent: Deprecated.

    Note:
        In nevergrad v0.2.0+, optimizers can be instrumented.
        For instance, the following will specifies searching
        for "lr" from 1 to 2.

        >>> from nevergrad.optimization import optimizerlib
        >>> from nevergrad import instrumentation as inst
        >>> lr = inst.var.Array(1).bounded(1, 2).asfloat()
        >>> instrumentation = inst.Instrumentation(lr=lr)
        >>> optimizer = optimizerlib.OnePlusOne(instrumentation, budget=100)
        >>> algo = NevergradSearch(
                optimizer, None, metric="mean_loss", mode="min")

    """

    def __init__(self,
                 optimizer,
                 parameter_names,
                 metric="episode_reward_mean",
                 mode="max",
                 max_concurrent=None,
                 **kwargs):
        assert ng is not None, "Nevergrad must be installed!"
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        self._parameters = parameter_names
        # nevergrad.tell internally minimizes, so "max" => -1
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        self._nevergrad_opt = optimizer
        self._live_trial_mapping = {}
        self.max_concurrent = max_concurrent
        super(NevergradSearch, self).__init__(
            metric=metric, mode=mode, max_concurrent=max_concurrent, **kwargs)
        # validate parameters
        if hasattr(optimizer, "instrumentation"):  # added in v0.2.0
            if optimizer.instrumentation.kwargs:
                if optimizer.instrumentation.args:
                    raise ValueError(
                        "Instrumented optimizers should use kwargs only")
                if parameter_names is not None:
                    raise ValueError("Instrumented optimizers should provide "
                                     "None as parameter_names")
            else:
                if parameter_names is None:
                    raise ValueError("Non-instrumented optimizers should have "
                                     "a list of parameter_names")
                if len(optimizer.instrumentation.args) != 1:
                    raise ValueError(
                        "Instrumented optimizers should use kwargs only")
        if parameter_names is not None and optimizer.dimension != len(
                parameter_names):
            raise ValueError("len(parameters_names) must match optimizer "
                             "dimension for non-instrumented optimizers")

    def suggest(self, trial_id):
        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None
        suggested_config = self._nevergrad_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        # in v0.2.0+, output of ask() is a Candidate,
        # with fields args and kwargs
        if not suggested_config.kwargs:
            print(suggested_config.args, suggested_config.kwargs)
            return dict(zip(self._parameters, suggested_config.args[0]))
        else:
            return suggested_config.kwargs

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        The result is internally negated when interacting with Nevergrad
        so that Nevergrad Optimizers can "maximize" this value,
        as it minimizes on default.
        """
        if result:
            self._process_result(trial_id, result)

        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id, result):
        ng_trial_info = self._live_trial_mapping[trial_id]
        self._nevergrad_opt.tell(ng_trial_info,
                                 self._metric_op * result[self._metric])

    def save(self, checkpoint_dir):
        trials_object = (self._nevergrad_opt, self._parameters)
        with open(checkpoint_dir, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self._nevergrad_opt = trials_object[0]
        self._parameters = trials_object[1]
