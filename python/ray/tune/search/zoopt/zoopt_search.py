import copy
import logging
from typing import Dict, List, Optional, Tuple

import ray
import ray.cloudpickle as pickle
from ray.tune.result import DEFAULT_METRIC
from ray.tune.search.sample import (
    Categorical,
    Domain,
    Float,
    Integer,
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
from ray.tune.utils.util import unflatten_dict

try:
    import zoopt
    from zoopt import Solution, ValueType
except ImportError:
    zoopt = None
    Solution = ValueType = None

logger = logging.getLogger(__name__)


class ZOOptSearch(Searcher):
    """A wrapper around ZOOpt to provide trial suggestions.

    ZOOptSearch is a library for derivative-free optimization. It is backed by
    the `ZOOpt <https://github.com/polixir/ZOOpt>`__ package. Currently,
    Asynchronous Sequential RAndomized COordinate Shrinking (ASRacos)
    is implemented in Tune.

    To use ZOOptSearch, install zoopt (>=0.4.1): ``pip install -U zoopt``.

    Tune automatically converts search spaces to ZOOpt"s format:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.zoopt import ZOOptSearch

        "config": {
            "iterations": 10,  # evaluation times
            "width": tune.uniform(-10, 10),
            "height": tune.uniform(-10, 10)
        }

        zoopt_search_config = {
            "parallel_num": 8,  # how many workers to parallel
        }

        zoopt_search = ZOOptSearch(
            algo="Asracos",  # only support Asracos currently
            budget=20,  # must match `num_samples` in `tune.run()`.
            dim_dict=dim_dict,
            metric="mean_loss",
            mode="min",
            **zoopt_search_config
        )

        tune.run(my_objective,
            config=config,
            search_alg=zoopt_search,
            name="zoopt_search",
            num_samples=20,
            stop={"timesteps_total": 10})

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.zoopt import ZOOptSearch
        from zoopt import ValueType

        dim_dict = {
            "height": (ValueType.CONTINUOUS, [-10, 10], 1e-2),
            "width": (ValueType.DISCRETE, [-10, 10], False),
            "layers": (ValueType.GRID, [4, 8, 16])
        }

        "config": {
            "iterations": 10,  # evaluation times
        }

        zoopt_search_config = {
            "parallel_num": 8,  # how many workers to parallel
        }

        zoopt_search = ZOOptSearch(
            algo="Asracos",  # only support Asracos currently
            budget=20,  # must match `num_samples` in `tune.run()`.
            dim_dict=dim_dict,
            metric="mean_loss",
            mode="min",
            **zoopt_search_config
        )

        tune.run(my_objective,
            config=config,
            search_alg=zoopt_search,
            name="zoopt_search",
            num_samples=20,
            stop={"timesteps_total": 10})

    Parameters:
        algo: To specify an algorithm in zoopt you want to use.
            Only support ASRacos currently.
        budget: Number of samples.
        dim_dict: Dimension dictionary.
            For continuous dimensions: (continuous, search_range, precision);
            For discrete dimensions: (discrete, search_range, has_order);
            For grid dimensions: (grid, grid_list).
            More details can be found in zoopt package.
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
        parallel_num: How many workers to parallel. Note that initial
            phase may start less workers than this number. More details can
            be found in zoopt package.
    """

    optimizer = None

    def __init__(
        self,
        algo: str = "asracos",
        budget: Optional[int] = None,
        dim_dict: Optional[Dict] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        points_to_evaluate: Optional[List[Dict]] = None,
        parallel_num: int = 1,
        **kwargs
    ):
        assert (
            zoopt is not None
        ), "ZOOpt not found - please install zoopt by `pip install -U zoopt`."
        assert budget is not None, "`budget` should not be None!"
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        _algo = algo.lower()
        assert _algo in [
            "asracos",
            "sracos",
        ], "`algo` must be in ['asracos', 'sracos'] currently"

        self._algo = _algo

        if isinstance(dim_dict, dict) and dim_dict:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(dim_dict)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="dim_dict", cls=type(self))
                )
                dim_dict = self.convert_search_space(dim_dict, join=True)

        self._dim_dict = dim_dict
        self._budget = budget

        self._metric = metric
        if mode == "max":
            self._metric_op = -1.0
        elif mode == "min":
            self._metric_op = 1.0

        self._points_to_evaluate = copy.deepcopy(points_to_evaluate)

        self._live_trial_mapping = {}

        self._dim_keys = []
        self.solution_dict = {}
        self.best_solution_list = []
        self.optimizer = None

        self.kwargs = kwargs

        self.parallel_num = parallel_num

        super(ZOOptSearch, self).__init__(metric=self._metric, mode=mode)

        if self._dim_dict:
            self._setup_zoopt()

    def _setup_zoopt(self):
        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        _dim_list = []
        for k in self._dim_dict:
            self._dim_keys.append(k)
            _dim_list.append(self._dim_dict[k])

        init_samples = None
        if self._points_to_evaluate:
            logger.warning(
                "`points_to_evaluate` is ignored by ZOOpt in versions <= 0.4.1."
            )
            init_samples = [
                Solution(x=tuple(point[dim] for dim in self._dim_keys))
                for point in self._points_to_evaluate
            ]
        dim = zoopt.Dimension2(_dim_list)
        par = zoopt.Parameter(budget=self._budget, init_samples=init_samples)
        if self._algo == "sracos" or self._algo == "asracos":
            from zoopt.algos.opt_algorithms.racos.sracos import SRacosTune

            self.optimizer = SRacosTune(
                dimension=dim,
                parameter=par,
                parallel_num=self.parallel_num,
                **self.kwargs
            )

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self._dim_dict:
            return False
        space = self.convert_search_space(config)
        self._dim_dict = space

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        if self._mode == "max":
            self._metric_op = -1.0
        elif self._mode == "min":
            self._metric_op = 1.0

        self._setup_zoopt()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._dim_dict or not self.optimizer:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="dim_dict"
                )
            )
        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__, metric=self._metric, mode=self._mode
                )
            )

        _solution = self.optimizer.suggest()

        if _solution == "FINISHED":
            if ray.__version__ >= "0.8.7":
                return Searcher.FINISHED
            else:
                return None

        if _solution:
            self.solution_dict[str(trial_id)] = _solution
            _x = _solution.get_x()
            new_trial = dict(zip(self._dim_keys, _x))
            self._live_trial_mapping[trial_id] = new_trial
            return unflatten_dict(new_trial)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        """Notification for the completion of trial."""
        if result:
            _solution = self.solution_dict[str(trial_id)]
            _best_solution_so_far = self.optimizer.complete(
                _solution, self._metric_op * result[self._metric]
            )
            if _best_solution_so_far:
                self.best_solution_list.append(_best_solution_so_far)

        del self._live_trial_mapping[trial_id]

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self.__dict__.update(save_object)

    @staticmethod
    def convert_search_space(spec: Dict, join: bool = False) -> Dict[str, Tuple]:
        spec = copy.deepcopy(spec)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if not domain_vars and not grid_vars:
            return {}

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a ZOOpt search space."
            )

        def resolve_value(domain: Domain) -> Tuple:
            quantize = None

            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                quantize = sampler.q
                sampler = sampler.sampler

            if isinstance(domain, Float):
                precision = quantize or 1e-12
                if isinstance(sampler, Uniform):
                    return (
                        ValueType.CONTINUOUS,
                        [domain.lower, domain.upper],
                        precision,
                    )

            elif isinstance(domain, Integer):
                if isinstance(sampler, Uniform):
                    return (ValueType.DISCRETE, [domain.lower, domain.upper - 1], True)

            elif isinstance(domain, Categorical):
                # Categorical variables would use ValueType.DISCRETE with
                # has_partial_order=False, however, currently we do not
                # keep track of category values and cannot automatically
                # translate back and forth between them.
                if isinstance(sampler, Uniform):
                    return (ValueType.GRID, domain.categories)

            raise ValueError(
                "ZOOpt does not support parameters of type "
                "`{}` with samplers of type `{}`".format(
                    type(domain).__name__, type(domain.sampler).__name__
                )
            )

        conv_spec = {
            "/".join(path): resolve_value(domain) for path, domain in domain_vars
        }

        if join:
            spec.update(conv_spec)
            conv_spec = spec

        return conv_spec
