import logging
import typing
from typing import Optional

from ray.data._internal.progress.base_progress import BaseExecutionProgressManager

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)


class AsyncExecutionProgressManagerWrapper(BaseExecutionProgressManager):
    """
    Async wrapper for progress managers that prevents terminal I/O from blocking
    the streaming executor scheduling loop.
    """

    def __init__(
        self,
        wrapped_manager: BaseExecutionProgressManager,
    ):
        pass

    def start(self) -> None:
        pass

    def refresh(self) -> None:
        pass

    def close_with_finishing_description(self, desc: str, success: bool) -> None:
        pass

    def update_total_progress(self, new_rows: int, total_rows: Optional[int]) -> None:
        pass

    def update_total_resource_status(self, resource_status: str) -> None:
        pass

    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ) -> None:
        pass
