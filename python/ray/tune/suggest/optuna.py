import logging
import pickle

from ray.tune.result import TRAINING_ITERATION

try:
    import optuna as ot
except ImportError:
    ot = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class _Param:
    def __getattr__(self, item):
        def _inner(*args, **kwargs):
            return (item, args, kwargs)

        return _inner


param = _Param()


class OptunaSearch(Searcher):
    """A wrapper around Optuna to provide trial suggestions.

    `Optuna <https://optuna.org/>`_ is a hyperparameter optimization library.
    In contrast to other libraries, it employs define-by-run style
    hyperparameter definitions.

    This Searcher is a thin wrapper around Optuna's search algorithms.
    You can pass any Optuna sampler, which will be used to generate
    hyperparameter suggestions.

    Please note that this wrapper does not support define-by-run, so the
    search space will be configured before running the optimization. You will
    also need to use a Tune trainable (e.g. using the function API) with
    this wrapper.

    For defining the search space, use ``ray.tune.suggest.optuna.param``
    (see example).

    Args:
        space (list): Hyperparameter search space definition for Optuna's
            sampler. This is a list, and samples for the parameters will
            be obtained in order.
        metric (str): Metric that is reported back to Optuna on trial
            completion.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        sampler (optuna.samplers.BaseSampler): Optuna sampler used to
            draw hyperparameter configurations. Defaults to ``TPESampler``.

    Example:

    .. code-block:: python

        from ray.tune.suggest.optuna import OptunaSearch, param

        space = [
            param.suggest_uniform("a", 6, 8),
            param.suggest_uniform("b", 10, 20)
        ]

        algo = OptunaSearch(
            space,
            metric="loss",
            mode="min")

    .. versionadded:: 0.8.8

    """

    def __init__(
            self,
            space,
            metric="episode_reward_mean",
            mode="max",
            sampler=None,
    ):
        assert ot is not None, (
            "Optuna must be installed! Run `pip install optuna`.")
        super(OptunaSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=None,
            use_early_stopped_trials=None)

        self._space = space

        self._study_name = "optuna"  # Fixed study name for in-memory storage
        self._sampler = sampler or ot.samplers.TPESampler()
        assert isinstance(self._sampler, ot.samplers.BaseSampler), \
            "You can only pass an instance of `optuna.samplers.BaseSampler` " \
            "as a sampler to `OptunaSearcher`."

        self._pruner = ot.pruners.NopPruner()
        self._storage = ot.storages.InMemoryStorage()

        self._ot_trials = {}
        self._ot_study = ot.study.create_study(
            storage=self._storage,
            sampler=self._sampler,
            pruner=self._pruner,
            study_name=self._study_name,
            direction="minimize" if mode == "min" else "maximize",
            load_if_exists=True)

    def suggest(self, trial_id):
        if trial_id not in self._ot_trials:
            ot_trial_id = self._storage.create_new_trial(
                self._ot_study._study_id)
            self._ot_trials[trial_id] = ot.trial.Trial(self._ot_study,
                                                       ot_trial_id)
        ot_trial = self._ot_trials[trial_id]
        params = {}
        for (fn, args, kwargs) in self._space:
            param_name = args[0] if len(args) > 0 else kwargs["name"]
            params[param_name] = getattr(ot_trial, fn)(*args, **kwargs)
        return params

    def on_trial_result(self, trial_id, result):
        metric = result[self.metric]
        step = result[TRAINING_ITERATION]
        ot_trial = self._ot_trials[trial_id]
        ot_trial.report(metric, step)

    def on_trial_complete(self, trial_id, result=None, error=False):
        ot_trial = self._ot_trials[trial_id]
        ot_trial_id = ot_trial._trial_id
        self._storage.set_trial_value(ot_trial_id, result.get(
            self.metric, None))
        self._storage.set_trial_state(ot_trial_id,
                                      ot.trial.TrialState.COMPLETE)

    def save(self, checkpoint_path):
        save_object = (self._storage, self._pruner, self._sampler,
                       self._ot_trials, self._ot_study)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self._storage, self._pruner, self._sampler, \
            self._ot_trials, self._ot_study = save_object
