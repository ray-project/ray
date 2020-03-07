from ray.tune.suggest.search import SearchAlgorithm
from ray.tune.suggest.basic_variant import BasicVariantGenerator
from ray.tune.suggest.suggestion import SuggestionAlgorithm
from ray.tune.suggest.variant_generator import grid_search
from ray.tune.suggest.bohb import TuneBOHB

__all__ = [
    "SearchAlgorithm", "BasicVariantGenerator", "SuggestionAlgorithm",
    "grid_search", "TuneBOHB"
]


def BayesOptSearch(*args, **kwargs):
    raise DeprecationWarning("""This class has been moved. Please import via
        `from ray.tune.suggest.bayesopt import BayesOptSearch`""")


def HyperOptSearch(*args, **kwargs):
    raise DeprecationWarning("""This class has been moved. Please import via
        `from ray.tune.suggest.hyperopt import HyperOptSearch`""")


def NevergradSearch(*args, **kwargs):
    raise DeprecationWarning("""This class has been moved. Please import via
        `from ray.tune.suggest.nevergrad import NevergradSearch`""")


def SkOptSearch(*args, **kwargs):
    raise DeprecationWarning("""This class has been moved. Please import via
        `from ray.tune.suggest.skopt import SkOptSearch`""")


def SigOptSearch(*args, **kwargs):
    raise DeprecationWarning("""This class has been moved. Please import via
        `from ray.tune.suggest.sigopt import SigOptSearch`""")
