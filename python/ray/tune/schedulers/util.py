import logging
from typing import Optional

logger = logging.getLogger(__name__)


def set_search_properties_backwards_compatible(
    set_search_properties_func, metric: Optional[str], mode: Optional[str], **spec
) -> bool:
    """Wraps around set_search_properties() so that it is backward compatible.

    Also outputs a warning to encourage custom schedulers to be updated.
    """
    try:
        return set_search_properties_func(metric, mode, **spec)
    except TypeError as e:
        if str(e).startswith(
            "set_search_properties() got an unexpected keyword argument"
        ):
            logger.warning(
                "Please update custom Scheduler to take in function signature "
                "as ``def set_search_properties(metric, mode, "
                "**spec) -> bool``."
            )
            return set_search_properties_func(metric, mode)
        else:
            raise e
