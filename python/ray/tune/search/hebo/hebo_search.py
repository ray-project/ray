import logging
import pickle
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

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
    Quantized,
    Uniform,
)
from ray.tune.search.variant_generator import parse_spec_vars
from ray.tune.utils.util import is_nan_or_inf, unflatten_dict, validate_warmstart

try:  # Python 3 only -- needed for lint test.
    import hebo
    import torch  # hebo has torch as a dependency
except ImportError:
    hebo = None

logger = logging.getLogger(__name__)

SPACE_ERROR_MESSAGE = (
    "Space must be either a HEBO DesignSpace object"
    "or a dictionary with ONLY tune search spaces."
)


class HEBOSearch(Searcher):
    """Uses HEBO (Heteroscedastic Evolutionary Bayesian Optimization)
    to optimize hyperparameters.

    HEBO is a cutting edge black-box optimization framework created
    by Huawei's Noah Ark. More info can be found here:
    https://github.com/huawei-noah/HEBO/tree/master/HEBO.

    `space` can either be a HEBO's `DesignSpace` object or a dict of Tune
    search spaces.

    Please note that the first few trials will be random and used
    to kickstart the search process. In order to achieve good results,
    we recommend setting the number of trials to at least 16.

    Maximum number of concurrent trials is determined by ``max_concurrent``
    argument. Trials will be done in batches of ``max_concurrent`` trials.
    If this Searcher is used in a ``ConcurrencyLimiter``, the
    ``max_concurrent`` value passed to it will override the value passed
    here.

    Args:
        space: A dict mapping parameter names to Tune search spaces or a
            HEBO DesignSpace object.
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
        evaluated_rewards: If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate.
        random_state_seed: Seed for reproducible
            results. Defaults to None. Please note that setting this to a value
            will change global random states for `numpy` and `torch`
            on initalization and loading from checkpoint.
        max_concurrent: Number of maximum concurrent trials.
            If this Searcher is used in a ``ConcurrencyLimiter``, the
            ``max_concurrent`` value passed to it will override the
            value passed here.
        **kwargs: The keyword arguments will be passed to `HEBO()``.

    Tune automatically converts search spaces to HEBO's format:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.hebo import HEBOSearch

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100)
        }

        hebo = HEBOSearch(metric="mean_loss", mode="min")
        tuner = tune.Tuner(
            trainable_function,
            tune_config=tune.TuneConfig(
                search_alg=hebo
            ),
            param_space=config
        )
        tuner.fit()

    Alternatively, you can pass a HEBO `DesignSpace` object manually to the
    Searcher:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.hebo import HEBOSearch
        from hebo.design_space.design_space import DesignSpace

        space_config = [
            {'name' : 'width', 'type' : 'num', 'lb' : 0, 'ub' : 20},
            {'name' : 'height', 'type' : 'num', 'lb' : -100, 'ub' : 100},
        ]
        space = DesignSpace().parse(space_config)

        hebo = HEBOSearch(space, metric="mean_loss", mode="min")
        tuner = tune.Tuner(
            trainable_function,
            tune_config=tune.TuneConfig(
                search_alg=hebo
            )
        )
        tuner.fit()

    """

    def __init__(
        self,
        space: Optional[
            Union[Dict, "hebo.design_space.design_space.DesignSpace"]
        ] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        points_to_evaluate: Optional[List[Dict]] = None,
        evaluated_rewards: Optional[List] = None,
        random_state_seed: Optional[int] = None,
        max_concurrent: int = 8,
        **kwargs,
    ):
        assert hebo is not None, (
            "HEBO must be installed! You can install HEBO with"
            " the command: `pip install 'HEBO>=0.2.0'`."
            "This error may also be caused if HEBO"
            " dependencies have bad versions. Try updating HEBO"
            " first."
        )
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        assert (
            isinstance(max_concurrent, int) and max_concurrent >= 1
        ), "`max_concurrent` must be an integer and at least 1."
        if random_state_seed is not None:
            assert isinstance(
                random_state_seed, int
            ), "random_state_seed must be None or int, got '{}'.".format(
                type(random_state_seed)
            )
        super(HEBOSearch, self).__init__(metric=metric, mode=mode)

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if resolved_vars:
                raise TypeError(SPACE_ERROR_MESSAGE)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="space", cls=type(self))
                )
                space = self.convert_search_space(space)
        elif space is not None and not isinstance(
            space, hebo.design_space.design_space.DesignSpace
        ):
            raise TypeError(SPACE_ERROR_MESSAGE + " Got {}.".format(type(space)))

        self._hebo_config = kwargs
        self._random_state_seed = random_state_seed
        self._space = space
        self._points_to_evaluate = points_to_evaluate
        self._evaluated_rewards = evaluated_rewards
        self._initial_points = []
        self._live_trial_mapping = {}

        self._max_concurrent = max_concurrent
        self._suggestions_cache = []
        self._batch_filled = False

        self._opt = None
        if space:
            self._setup_optimizer()

    def set_max_concurrency(self, max_concurrent: int) -> bool:
        self._max_concurrent = max_concurrent
        return True

    def _setup_optimizer(self):
        # HEBO internally minimizes, so "max" => -1
        if self._mode == "max":
            self._metric_op = -1.0
        elif self._mode == "min":
            self._metric_op = 1.0

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        if not isinstance(self._space, hebo.design_space.design_space.DesignSpace):
            raise ValueError(
                f"Invalid search space: {type(self._space)}. Either pass a "
                f"valid search space to the `HEBOSearch` class or pass "
                f"a `param_space` parameter to `tune.Tuner()`"
            )

        if self._space.num_paras <= 0:
            raise ValueError(
                "Got empty search space. Please make sure to pass "
                "a valid search space with at least one parameter to "
                "`HEBOSearch`"
            )

        if self._random_state_seed is not None:
            np.random.seed(self._random_state_seed)
            torch.random.manual_seed(self._random_state_seed)

        self._opt = hebo.optimizers.hebo.HEBO(space=self._space, **self._hebo_config)

        if self._points_to_evaluate:
            validate_warmstart(
                self._space.para_names,
                self._points_to_evaluate,
                self._evaluated_rewards,
            )
            if self._evaluated_rewards:
                self._opt.observe(
                    pd.DataFrame(self._points_to_evaluate),
                    np.array(self._evaluated_rewards) * self._metric_op,
                )
            else:
                self._initial_points = self._points_to_evaluate

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self._opt:
            return False
        space = self.convert_search_space(config)
        self._space = space

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_optimizer()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._opt:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"
                )
            )

        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__, metric=self._metric, mode=self._mode
                )
            )

        if not self._live_trial_mapping:
            self._batch_filled = False

        if self._initial_points:
            params = self._initial_points.pop(0)
            suggestion = pd.DataFrame([params], index=[0])
        else:
            if (
                self._batch_filled
                or len(self._live_trial_mapping) >= self._max_concurrent
            ):
                return None
            if not self._suggestions_cache:
                suggestion = self._opt.suggest(n_suggestions=self._max_concurrent)
                self._suggestions_cache = suggestion.to_dict("records")
            params = self._suggestions_cache.pop(0)
            suggestion = pd.DataFrame([params], index=[0])
        self._live_trial_mapping[trial_id] = suggestion
        if len(self._live_trial_mapping) >= self._max_concurrent:
            self._batch_filled = True
        return unflatten_dict(params)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        """Notification for the completion of trial.

        HEBO always minimizes."""

        if result:
            self._process_result(trial_id, result)
        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id: str, result: Dict):
        trial_info = self._live_trial_mapping[trial_id]
        if result and not is_nan_or_inf(result[self._metric]):
            self._opt.observe(
                trial_info, np.array([self._metric_op * result[self._metric]])
            )

    def add_evaluated_point(
        self,
        parameters: Dict,
        value: float,
        error: bool = False,
        pruned: bool = False,
        intermediate_values: Optional[List[float]] = None,
    ):
        if intermediate_values:
            logger.warning("HEBO doesn't use intermediate_values. Ignoring.")
        if not error and not pruned:
            self._opt.observe(
                pd.DataFrame(
                    [
                        {
                            k: v
                            for k, v in parameters.items()
                            if k in self._opt.space.para_names
                        }
                    ]
                ),
                np.array([value]) * self._metric_op,
            )
        else:
            logger.warning(
                "Only non errored and non pruned points can be added to HEBO."
            )

    def save(self, checkpoint_path: str):
        """Storing current optimizer state."""
        if self._random_state_seed is not None:
            numpy_random_state = np.random.get_state()
            torch_random_state = torch.get_rng_state()
        else:
            numpy_random_state = None
            torch_random_state = None
        save_object = self.__dict__.copy()
        save_object["__numpy_random_state"] = numpy_random_state
        save_object["__torch_random_state"] = torch_random_state
        with open(checkpoint_path, "wb") as f:
            pickle.dump(save_object, f)

    def restore(self, checkpoint_path: str):
        """Restoring current optimizer state."""
        with open(checkpoint_path, "rb") as f:
            save_object = pickle.load(f)

        if isinstance(save_object, dict):
            numpy_random_state = save_object.pop("__numpy_random_state", None)
            torch_random_state = save_object.pop("__torch_random_state", None)
            self.__dict__.update(save_object)
        else:
            # Backwards compatibility
            (
                self._opt,
                self._initial_points,
                numpy_random_state,
                torch_random_state,
                self._live_trial_mapping,
                self._max_concurrent,
                self._suggestions_cache,
                self._space,
                self._hebo_config,
                self._batch_filled,
            ) = save_object
        if numpy_random_state is not None:
            np.random.set_state(numpy_random_state)
        if torch_random_state is not None:
            torch.random.set_rng_state(torch_random_state)

    @staticmethod
    def convert_search_space(spec: Dict, prefix: str = "") -> Dict:
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        params = []

        if not domain_vars and not grid_vars:
            return {}

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a HEBO search space."
            )

        def resolve_value(par: str, domain: Domain):
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning(
                    "HEBO search does not support quantization. "
                    "Dropped quantization."
                )
                sampler = sampler.get_sampler()

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    return {
                        "name": par,
                        "type": "pow",
                        "lb": domain.lower,
                        "ub": domain.upper,
                        "base": sampler.base,
                    }
                elif isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "num",
                        "lb": domain.lower,
                        "ub": domain.upper,
                    }

            elif isinstance(domain, Integer):
                if isinstance(sampler, LogUniform):
                    return {
                        "name": par,
                        "type": "pow_int",
                        "lb": domain.lower,
                        "ub": domain.upper - 1,  # Upper bound exclusive
                        "base": sampler.base,
                    }
                elif isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "int",
                        "lb": domain.lower,
                        "ub": domain.upper - 1,  # Upper bound exclusive
                    }
            elif isinstance(domain, Categorical):
                return {
                    "name": par,
                    "type": "cat",
                    "categories": list(domain.categories),
                }

            raise ValueError(
                "HEBO does not support parameters of type "
                "`{}` with samplers of type `{}`".format(
                    type(domain).__name__, type(domain.sampler).__name__
                )
            )

        for path, domain in domain_vars:
            par = "/".join([str(p) for p in ((prefix,) + path if prefix else path)])
            value = resolve_value(par, domain)
            params.append(value)

        return hebo.design_space.design_space.DesignSpace().parse(params)
