import logging
from typing import Dict, Optional, List

from ray.tune.experiment import Experiment

logger = logging.getLogger(__name__)


def with_try_catch(set_search_property_func,
                   metric: Optional[str],
                   mode: Optional[str],
                   config: Dict,
                   experiments: Optional[List[Experiment]] = None) -> bool:
    try:
        return set_search_property_func(metric, mode, config, experiments)
    except TypeError as e:
        if str(
                e
        ) == ("set_search_properties() takes 4 positional arguments but 5 "
              "were given"):
            logger.warn("Please update custom Searcher to take in a list of "
                        "experiments.")
            return set_search_property_func(metric, mode, config)
        else:
            raise e
