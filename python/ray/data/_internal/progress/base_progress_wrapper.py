import logging
import typing

from ray.data._internal.progress.base_progress import BaseExecutionProgressManager

if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class AsyncExecutionProgressManagerWrapper(BaseExecutionProgressManager):
    """
    Async wrapper for progress managers that prevents terminal I/O from blocking
    the streaming executor scheduling loop.
    """
