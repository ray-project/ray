from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
    from ray.llm._internal.serve.configs.server_models import LLMConfig


@dataclass
class CallbackCtx:
    """Context object passed to all callback hooks.

    This is a centralized place for all state during node initialization.
    Callbacks can read and modify fields as needed.

    The custom_data dict provides flexibility for callback-specific state.
    """

    llm_config: "LLMConfig"
    local_node_download_model: Optional["NodeModelDownloadable"] = None
    worker_node_download_model: Optional["NodeModelDownloadable"] = None
    placement_group: Optional[Any] = None
    runtime_env: Optional[Dict[str, Any]] = None
    custom_data: Dict[str, Any] = field(default_factory=dict)
    run_downloads: bool = True


@runtime_checkable
class CustomInitCallback(Protocol):
    """Protocol for custom initialization implementations.

    This protocol defines the interface for custom initialization logic
    for LLMEngine to be called in node_initialization.
    """

    async def on_before_init(self, ctx: CallbackCtx) -> None:
        """Called before node initialization begins.

        Args:
            ctx: The callback context containing configuration and state
                 that can be modified to influence initialization.
        """
        pass

    async def on_after_init(self, ctx: CallbackCtx) -> None:
        """Called after node initialization completes.

        Args:
            ctx: The callback context containing the final state after
                 initialization.
        """
        pass
