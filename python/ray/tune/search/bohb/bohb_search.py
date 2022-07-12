"""BOHB (Bayesian Optimization with HyperBand)"""

import copy
import logging
import math

# use cloudpickle instead of pickle to make BOHB obj
# pickleable
from ray import cloudpickle
from typing import Dict, List, Optional, Union

from ray.tune.result import DEFAULT_METRIC
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
from ray.tune.search import (
    UNRESOLVED_SEARCH_SPACE,
    UNDEFINED_METRIC_MODE,
    UNDEFINED_SEARCH_SPACE,
    Searcher,
)
from ray.tune.search.variant_generator import parse_spec_vars
from ray.tune.utils.util import flatten_dict, unflatten_list_dict

try:
    import ConfigSpace
    from hpbandster.optimizers.config_generators.bohb import BOHB
except ImportError:
    BOHB = ConfigSpace = None

logger = logging.getLogger(__name__)


class _BOHBJobWrapper:
    """Mock object for HpBandSter to process."""

    def __init__(self, loss: float, budget: float, config: Dict):
        self.result = {"loss": loss}
        self.kwargs = {"budget": budget, "config": config.copy()}
        self.exception = None


class TuneBOHB(Searcher):
    """BOHB suggestion component.


    Requires HpBandSter and ConfigSpace to be installed. You can install
    HpBandSter and ConfigSpace with: ``pip install hpbandster ConfigSpace``.

    This should be used in conjunction with HyperBandForBOHB.

    Args:
        space: Continuous ConfigSpace search space.
            Parameters will be sampled from this space which will be used
            to run trials.
        bohb_config: configuration for HpBandSter BOHB algorithm
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
        seed: Optional random seed to initialize the random number
            generator. Setting this should lead to identical initial
            configurations at each run.

    Tune automatically converts search spaces to TuneBOHB's format:

    .. code-block:: python

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            "activation": tune.choice(["relu", "tanh"])
        }

        algo = TuneBOHB(metric="mean_loss", mode="min")
        bohb = HyperBandForBOHB(
            time_attr="training_iteration",
            metric="mean_loss",
            mode="min",
            max_t=100)
        run(my_trainable, config=config, scheduler=bohb, search_alg=algo)

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        import ConfigSpace as CS

        config_space = CS.ConfigurationSpace()
        config_space.add_hyperparameter(
            CS.UniformFloatHyperparameter("width", lower=0, upper=20))
        config_space.add_hyperparameter(
            CS.UniformFloatHyperparameter("height", lower=-100, upper=100))
        config_space.add_hyperparameter(
            CS.CategoricalHyperparameter(
                name="activation", choices=["relu", "tanh"]))

        algo = TuneBOHB(
            config_space, metric="mean_loss", mode="min")
        bohb = HyperBandForBOHB(
            time_attr="training_iteration",
            metric="mean_loss",
            mode="min",
            max_t=100)
        run(my_trainable, scheduler=bohb, search_alg=algo)

    """

    def __init__(
        self,
        space: Optional[Union[Dict, "ConfigSpace.ConfigurationSpace"]] = None,
        bohb_config: Optional[Dict] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        points_to_evaluate: Optional[List[Dict]] = None,
        seed: Optional[int] = None,
    ):
        assert (
            BOHB is not None
        ), """HpBandSter must be installed!
            You can install HpBandSter with the command:
            `pip install hpbandster ConfigSpace`."""
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        self.trial_to_params = {}
        self._metric = metric

        self._bohb_config = bohb_config

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="space", cls=type(self))
                )
                space = self.convert_search_space(space)

        self._space = space
        self._seed = seed

        self._points_to_evaluate = points_to_evaluate

        super(TuneBOHB, self).__init__(
            metric=self._metric,
            mode=mode,
        )

        if self._space:
            self._setup_bohb()

    def _setup_bohb(self):
        from hpbandster.optimizers.config_generators.bohb import BOHB

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        if self._mode == "max":
            self._metric_op = -1.0
        elif self._mode == "min":
            self._metric_op = 1.0

        if self._seed is not None:
            self._space.seed(self._seed)

        bohb_config = self._bohb_config or {}
        self.bohber = BOHB(self._space, **bohb_config)

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self._space:
            return False
        space = self.convert_search_space(config)
        self._space = space

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_bohb()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._space:
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

        if self._points_to_evaluate:
            config = self._points_to_evaluate.pop(0)
        else:
            # This parameter is not used in hpbandster implementation.
            config, _ = self.bohber.get_config(None)
        self.trial_to_params[trial_id] = copy.deepcopy(config)
        return unflatten_list_dict(config)

    def on_trial_result(self, trial_id: str, result: Dict):
        if "hyperband_info" not in result:
            logger.warning(
                "BOHB Info not detected in result. Are you using "
                "HyperBandForBOHB as a scheduler?"
            )
        elif "budget" in result.get("hyperband_info", {}):
            hbs_wrapper = self.to_wrapper(trial_id, result)
            self.bohber.new_result(hbs_wrapper)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        del self.trial_to_params[trial_id]

    def to_wrapper(self, trial_id: str, result: Dict) -> _BOHBJobWrapper:
        return _BOHBJobWrapper(
            self._metric_op * result[self.metric],
            result["hyperband_info"]["budget"],
            self.trial_to_params[trial_id],
        )

    @staticmethod
    def convert_search_space(spec: Dict) -> "ConfigSpace.ConfigurationSpace":
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a TuneBOHB search space."
            )

        # Flatten and resolve again after checking for grid search.
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        def resolve_value(
            par: str, domain: Domain
        ) -> ConfigSpace.hyperparameters.Hyperparameter:
            quantize = None

            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                quantize = sampler.q
                sampler = sampler.sampler

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    lower = domain.lower
                    upper = domain.upper
                    if quantize:
                        lower = math.ceil(domain.lower / quantize) * quantize
                        upper = math.floor(domain.upper / quantize) * quantize
                    return ConfigSpace.UniformFloatHyperparameter(
                        par, lower=lower, upper=upper, q=quantize, log=True
                    )
                elif isinstance(sampler, Uniform):
                    lower = domain.lower
                    upper = domain.upper
                    if quantize:
                        lower = math.ceil(domain.lower / quantize) * quantize
                        upper = math.floor(domain.upper / quantize) * quantize
                    return ConfigSpace.UniformFloatHyperparameter(
                        par, lower=lower, upper=upper, q=quantize, log=False
                    )
                elif isinstance(sampler, Normal):
                    return ConfigSpace.hyperparameters.NormalFloatHyperparameter(
                        par, mu=sampler.mean, sigma=sampler.sd, q=quantize, log=False
                    )

            elif isinstance(domain, Integer):
                if isinstance(sampler, LogUniform):
                    lower = domain.lower
                    upper = domain.upper
                    if quantize:
                        lower = math.ceil(domain.lower / quantize) * quantize
                        upper = math.floor(domain.upper / quantize) * quantize
                    else:
                        # Tune search space integers are exclusive
                        upper -= 1
                    return ConfigSpace.UniformIntegerHyperparameter(
                        par, lower=lower, upper=upper, q=quantize, log=True
                    )
                elif isinstance(sampler, Uniform):
                    lower = domain.lower
                    upper = domain.upper
                    if quantize:
                        lower = math.ceil(domain.lower / quantize) * quantize
                        upper = math.floor(domain.upper / quantize) * quantize
                    else:
                        # Tune search space integers are exclusive
                        upper -= 1
                    return ConfigSpace.UniformIntegerHyperparameter(
                        par, lower=lower, upper=upper, q=quantize, log=False
                    )

            elif isinstance(domain, Categorical):
                if isinstance(sampler, Uniform):
                    return ConfigSpace.CategoricalHyperparameter(
                        par, choices=domain.categories
                    )

            raise ValueError(
                "TuneBOHB does not support parameters of type "
                "`{}` with samplers of type `{}`".format(
                    type(domain).__name__, type(domain.sampler).__name__
                )
            )

        cs = ConfigSpace.ConfigurationSpace()
        for path, domain in domain_vars:
            par = "/".join(str(p) for p in path)
            value = resolve_value(par, domain)
            cs.add_hyperparameter(value)

        return cs

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            cloudpickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = cloudpickle.load(inputFile)
        self.__dict__.update(save_object)
