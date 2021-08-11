import logging
from typing import Dict, Optional

from ray.tune.experiment import Experiment

logger = logging.getLogger(__name__)


def set_search_properties_backwards_compatible(
        set_search_properties_func, metric: Optional[str], mode: Optional[str],
        config: Dict, experiment: Optional[Experiment]) -> bool:
    """Wraps around set_search_properties() so that it is backward compatible.

    Also outputs a warning to urge customm searchers to be updated.
    """
    try:
        return set_search_properties_func(metric, mode, config, experiment)
    except TypeError as e:
        if str(e).startswith("set_search_properties() takes"):
            logger.warning(
                "Please update custom Searcher to take in function signature "
                "as ``def set_search_properties(metric, mode, config, "
                "experiment=None) -> bool``.")
            return set_search_properties_func(metric, mode, config)
        else:
            raise e
