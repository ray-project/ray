import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _set_search_properties_backwards_compatible(
    set_search_properties_func,
    metric: Optional[str],
    mode: Optional[str],
    config: Dict,
    **spec
) -> bool:
    """Wraps around set_search_properties() so that it is backward compatible.

    Also outputs a warning to encourage custom searchers to be updated.
    """
    try:
        return set_search_properties_func(metric, mode, config, **spec)
    except TypeError as e:
        if "set_search_properties() got an unexpected keyword argument" in str(e):
            logger.warning(
                "Please update custom Searcher to take in function signature "
                "as ``def set_search_properties(metric, mode, config, "
                "**spec) -> bool``."
            )
            return set_search_properties_func(metric, mode, config)
        else:
            raise e
