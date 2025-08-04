import copy
import logging
from functools import partial
from typing import Any, Dict, List, Optional

import numpy as np

# Use cloudpickle instead of pickle to make lambda funcs in HyperOpt pickleable
from ray import cloudpickle
from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_METRIC
from ray.tune.search import (
    UNDEFINED_METRIC_MODE,
    UNDEFINED_SEARCH_SPACE,
    UNRESOLVED_SEARCH_SPACE,
    Searcher,
)
from ray.tune.search.sample import (
    Categorical,
    Domain,
    Float,
    Integer,
    LogUniform,
    Normal,
    Quantized,
    Uniform,
)
from ray.tune.search.variant_generator import assign_value, parse_spec_vars
from ray.tune.utils import flatten_dict

try:
    hyperopt_logger = logging.getLogger("hyperopt")
    hyperopt_logger.setLevel(logging.WARNING)
    import hyperopt as hpo
    from hyperopt.pyll import Apply
except ImportError:
    hpo = None
    Apply = None


logger = logging.getLogger(__name__)


HYPEROPT_UNDEFINED_DETAILS = (
    " This issue can also come up with HyperOpt if your search space only "
    "contains constant variables, which is not supported by HyperOpt. In that case, "
    "don't pass any searcher or add sample variables to the search space."
)


class HyperOptSearch(Searcher):
    """A wrapper around HyperOpt to provide trial suggestions.

    HyperOpt a Python library for serial and parallel optimization
    over awkward search spaces, which may include real-valued, discrete,
    and conditional dimensions. More info can be found at
    http://hyperopt.github.io/hyperopt.

    HyperOptSearch uses the Tree-structured Parzen Estimators algorithm,
    though it can be trivially extended to support any algorithm HyperOpt
    supports.

    To use this search algorithm, you will need to install HyperOpt:

    .. code-block:: bash

        pip install -U hyperopt


    Parameters:
        space: HyperOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        metric: The training result objective value attribute. If None
            but a mode was passed, the anonymous metric `_metric` will be used
            per default.
        mode: One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate: Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want to run first to help the algorithm make better suggestions
            for future parameters. Needs to be a list of dicts containing the
            configurations.
        n_initial_points: number of random evaluations of the
            objective function before starting to approximate it with
            tree parzen estimators. Defaults to 20.
        random_state_seed: seed for reproducible
            results. Defaults to None.
        gamma: parameter governing the tree parzen
            estimators suggestion algorithm. Defaults to 0.25.

    Tune automatically converts search spaces to HyperOpt's format:

    .. code-block:: python

        config = {
            'width': tune.uniform(0, 20),
            'height': tune.uniform(-100, 100),
            'activation': tune.choice(["relu", "tanh"])
        }

        current_best_params = [{
            'width': 10,
            'height': 0,
            'activation': "relu",
        }]

        hyperopt_search = HyperOptSearch(
            metric="mean_loss", mode="min",
            points_to_evaluate=current_best_params)

        tuner = tune.Tuner(
            trainable,
            tune_config=tune.TuneConfig(
                search_alg=hyperopt_search
            ),
            param_space=config
        )
        tuner.fit()

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        space = {
            'width': hp.uniform('width', 0, 20),
            'height': hp.uniform('height', -100, 100),
            'activation': hp.choice("activation", ["relu", "tanh"])
        }

        current_best_params = [{
            'width': 10,
            'height': 0,
            'activation': "relu",
        }]

        hyperopt_search = HyperOptSearch(
            space, metric="mean_loss", mode="min",
            points_to_evaluate=current_best_params)

        tuner = tune.Tuner(
            trainable,
            tune_config=tune.TuneConfig(
                search_alg=hyperopt_search
            ),
        )
        tuner.fit()

    """

    def __init__(
        self,
        space: Optional[Dict] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        points_to_evaluate: Optional[List[Dict]] = None,
        n_initial_points: int = 20,
        random_state_seed: Optional[int] = None,
        gamma: float = 0.25,
    ):
        assert (
            hpo is not None
        ), "HyperOpt must be installed! Run `pip install hyperopt`."
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        super(HyperOptSearch, self).__init__(
            metric=metric,
            mode=mode,
        )
        # hyperopt internally minimizes, so "max" => -1
        if mode == "max":
            self.metric_op = -1.0
        elif mode == "min":
            self.metric_op = 1.0

        if n_initial_points is None:
            self.algo = hpo.tpe.suggest
        else:
            self.algo = partial(hpo.tpe.suggest, n_startup_jobs=n_initial_points)
        if gamma is not None:
            self.algo = partial(self.algo, gamma=gamma)

        self._points_to_evaluate = copy.deepcopy(points_to_evaluate)

        self._live_trial_mapping = {}
        self.rstate = np.random.RandomState(random_state_seed)

        self.domain = None
        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="space", cls=type(self))
                )
                space = self.convert_search_space(space)
            self._space = space
            self._setup_hyperopt()

    def _setup_hyperopt(self) -> None:
        from hyperopt.fmin import generate_trials_to_calculate

        if not self._space:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"
                )
                + HYPEROPT_UNDEFINED_DETAILS
            )

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        if self._points_to_evaluate is None:
            self._hpopt_trials = hpo.Trials()
            self._points_to_evaluate = 0
        else:
            assert isinstance(self._points_to_evaluate, (list, tuple))

            for i in range(len(self._points_to_evaluate)):
                config = self._points_to_evaluate[i]
                self._convert_categories_to_indices(config)
            # HyperOpt treats initial points as LIFO, reverse to get FIFO
            self._points_to_evaluate = list(reversed(self._points_to_evaluate))
            self._hpopt_trials = generate_trials_to_calculate(self._points_to_evaluate)
            self._hpopt_trials.refresh()
            self._points_to_evaluate = len(self._points_to_evaluate)

        self.domain = hpo.Domain(lambda spc: spc, self._space)

    def _convert_categories_to_indices(self, config) -> None:
        """Convert config parameters for categories into hyperopt-compatible
        representations where instead the index of the category is expected."""

        def _lookup(config_dict, space_dict, key):
            if isinstance(config_dict[key], dict):
                for k in config_dict[key]:
                    _lookup(config_dict[key], space_dict[key], k)
            else:
                if (
                    key in space_dict
                    and isinstance(space_dict[key], hpo.base.pyll.Apply)
                    and space_dict[key].name == "switch"
                ):
                    if len(space_dict[key].pos_args) > 0:
                        categories = [
                            a.obj
                            for a in space_dict[key].pos_args[1:]
                            if a.name == "literal"
                        ]
                        try:
                            idx = categories.index(config_dict[key])
                        except ValueError as exc:
                            msg = (
                                f"Did not find category with value "
                                f"`{config_dict[key]}` in "
                                f"hyperopt parameter `{key}`. "
                            )

                            if isinstance(config_dict[key], int):
                                msg += (
                                    "In previous versions, a numerical "
                                    "index was expected for categorical "
                                    "values of `points_to_evaluate`, "
                                    "but in ray>=1.2.0, the categorical "
                                    "value is expected to be directly "
                                    "provided. "
                                )

                            msg += "Please make sure the specified category is valid."
                            raise ValueError(msg) from exc
                        config_dict[key] = idx

        for k in config:
            _lookup(config, self._space, k)

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self.domain:
            return False
        space = self.convert_search_space(config)
        self._space = space

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self.metric_op = -1.0 if self._mode == "max" else 1.0

        self._setup_hyperopt()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self.domain:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"
                )
                + HYPEROPT_UNDEFINED_DETAILS
            )
        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__, metric=self._metric, mode=self._mode
                )
            )

        if self._points_to_evaluate > 0:
            using_point_to_evaluate = True
            new_trial = self._hpopt_trials.trials[self._points_to_evaluate - 1]
            self._points_to_evaluate -= 1
        else:
            using_point_to_evaluate = False
            new_ids = self._hpopt_trials.new_trial_ids(1)
            self._hpopt_trials.refresh()

            # Get new suggestion from Hyperopt
            new_trials = self.algo(
                new_ids,
                self.domain,
                self._hpopt_trials,
                self.rstate.randint(2**31 - 1),
            )
            self._hpopt_trials.insert_trial_docs(new_trials)
            self._hpopt_trials.refresh()
            new_trial = new_trials[0]
        self._live_trial_mapping[trial_id] = (new_trial["tid"], new_trial)

        # Taken from HyperOpt.base.evaluate
        config = hpo.base.spec_from_misc(new_trial["misc"])

        # We have to flatten nested spaces here so parameter names match
        config = flatten_dict(config, flatten_list=True)

        ctrl = hpo.base.Ctrl(self._hpopt_trials, current_trial=new_trial)
        memo = self.domain.memo_from_config(config)
        hpo.utils.use_obj_for_literal_in_memo(
            self.domain.expr, ctrl, hpo.base.Ctrl, memo
        )

        try:
            suggested_config = hpo.pyll.rec_eval(
                self.domain.expr,
                memo=memo,
                print_node_on_error=self.domain.rec_eval_print_node_on_error,
            )
        except (AssertionError, TypeError) as e:
            if using_point_to_evaluate and (
                isinstance(e, AssertionError) or "GarbageCollected" in str(e)
            ):
                raise ValueError(
                    "HyperOpt encountered a GarbageCollected switch argument. "
                    "Usually this is caused by a config in "
                    "`points_to_evaluate` "
                    "missing a key present in `space`. Ensure that "
                    "`points_to_evaluate` contains "
                    "all non-constant keys from `space`.\n"
                    "Config from `points_to_evaluate`: "
                    f"{config}\n"
                    "HyperOpt search space: "
                    f"{self._space}"
                ) from e
            raise e
        return copy.deepcopy(suggested_config)

    def on_trial_result(self, trial_id: str, result: Dict) -> None:
        ho_trial = self._get_hyperopt_trial(trial_id)
        if ho_trial is None:
            return
        now = hpo.utils.coarse_utcnow()
        ho_trial["book_time"] = now
        ho_trial["refresh_time"] = now

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ) -> None:
        """Notification for the completion of trial.

        The result is internally negated when interacting with HyperOpt
        so that HyperOpt can "maximize" this value, as it minimizes on default.
        """
        ho_trial = self._get_hyperopt_trial(trial_id)
        if ho_trial is None:
            return
        ho_trial["refresh_time"] = hpo.utils.coarse_utcnow()
        if error:
            ho_trial["state"] = hpo.base.JOB_STATE_ERROR
            ho_trial["misc"]["error"] = (str(TuneError), "Tune Error")
            self._hpopt_trials.refresh()
        elif result:
            self._process_result(trial_id, result)
        del self._live_trial_mapping[trial_id]

    def _process_result(self, trial_id: str, result: Dict) -> None:
        ho_trial = self._get_hyperopt_trial(trial_id)
        if not ho_trial:
            return
        ho_trial["refresh_time"] = hpo.utils.coarse_utcnow()

        ho_trial["state"] = hpo.base.JOB_STATE_DONE
        hp_result = self._to_hyperopt_result(result)
        ho_trial["result"] = hp_result
        self._hpopt_trials.refresh()

    def _to_hyperopt_result(self, result: Dict) -> Dict:
        try:
            return {"loss": self.metric_op * result[self.metric], "status": "ok"}
        except KeyError as e:
            raise RuntimeError(
                f"Hyperopt expected to see the metric `{self.metric}` in the "
                f"last result, but it was not found. To fix this, make "
                f"sure your call to `tune.report` or your return value of "
                f"your trainable class `step()` contains the above metric "
                f"as a key."
            ) from e

    def _get_hyperopt_trial(self, trial_id: str) -> Optional[Dict]:
        if trial_id not in self._live_trial_mapping:
            return
        hyperopt_tid = self._live_trial_mapping[trial_id][0]
        return [t for t in self._hpopt_trials.trials if t["tid"] == hyperopt_tid][0]

    def get_state(self) -> Dict:
        return {
            "hyperopt_trials": self._hpopt_trials,
            "rstate": self.rstate.get_state(),
        }

    def set_state(self, state: Dict) -> None:
        self._hpopt_trials = state["hyperopt_trials"]
        self.rstate.set_state(state["rstate"])

    def save(self, checkpoint_path: str) -> None:
        save_object = self.__dict__.copy()
        save_object["__rstate"] = self.rstate.get_state()
        with open(checkpoint_path, "wb") as f:
            cloudpickle.dump(save_object, f)

    def restore(self, checkpoint_path: str) -> None:
        with open(checkpoint_path, "rb") as f:
            save_object = cloudpickle.load(f)

        if "__rstate" not in save_object:
            # Backwards compatibility
            self.set_state(save_object)
        else:
            self.rstate.set_state(save_object.pop("__rstate"))
            self.__dict__.update(save_object)

    @staticmethod
    def convert_search_space(spec: Dict, prefix: str = "") -> Dict:
        spec = copy.deepcopy(spec)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if not domain_vars and not grid_vars:
            return {}

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a HyperOpt search space."
            )

        def resolve_value(par: str, domain: Domain) -> Any:
            quantize = None

            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                quantize = sampler.q
                sampler = sampler.sampler

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    if quantize:
                        return hpo.hp.qloguniform(
                            par, np.log(domain.lower), np.log(domain.upper), quantize
                        )
                    return hpo.hp.loguniform(
                        par, np.log(domain.lower), np.log(domain.upper)
                    )
                elif isinstance(sampler, Uniform):
                    if quantize:
                        return hpo.hp.quniform(
                            par, domain.lower, domain.upper, quantize
                        )
                    return hpo.hp.uniform(par, domain.lower, domain.upper)
                elif isinstance(sampler, Normal):
                    if quantize:
                        return hpo.hp.qnormal(par, sampler.mean, sampler.sd, quantize)
                    return hpo.hp.normal(par, sampler.mean, sampler.sd)

            elif isinstance(domain, Integer):
                if isinstance(sampler, LogUniform):
                    if quantize:
                        return hpo.base.pyll.scope.int(
                            hpo.hp.qloguniform(
                                par,
                                np.log(domain.lower),
                                np.log(domain.upper),
                                quantize,
                            )
                        )
                    return hpo.base.pyll.scope.int(
                        hpo.hp.qloguniform(
                            par, np.log(domain.lower), np.log(domain.upper - 1), 1.0
                        )
                    )
                elif isinstance(sampler, Uniform):
                    if quantize:
                        return hpo.base.pyll.scope.int(
                            hpo.hp.quniform(
                                par, domain.lower, domain.upper - 1, quantize
                            )
                        )
                    return hpo.hp.uniformint(par, domain.lower, high=domain.upper - 1)
            elif isinstance(domain, Categorical):
                if isinstance(sampler, Uniform):
                    return hpo.hp.choice(
                        par,
                        [
                            (
                                HyperOptSearch.convert_search_space(
                                    category, prefix=par
                                )
                                if isinstance(category, dict)
                                else (
                                    HyperOptSearch.convert_search_space(
                                        dict(enumerate(category)), prefix=f"{par}/{i}"
                                    )
                                    if isinstance(category, list)
                                    and len(category) > 0
                                    and isinstance(category[0], Domain)
                                    else (
                                        resolve_value(f"{par}/{i}", category)
                                        if isinstance(category, Domain)
                                        else category
                                    )
                                )
                            )
                            for i, category in enumerate(domain.categories)
                        ],
                    )

            raise ValueError(
                "HyperOpt does not support parameters of type "
                "`{}` with samplers of type `{}`".format(
                    type(domain).__name__, type(domain.sampler).__name__
                )
            )

        for path, domain in domain_vars:
            par = "/".join([str(p) for p in ((prefix,) + path if prefix else path)])
            value = resolve_value(par, domain)
            assign_value(spec, path, value)

        return spec
