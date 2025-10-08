import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Dict, Optional, Type, Union

if TYPE_CHECKING:
    from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
    from ray.llm._internal.serve.configs.server_models import LLMConfig

logger = logging.getLogger(__name__)


@dataclass
class CallbackCtx:
    """
    Context object passed to all callback hooks.
    Callbacks can read and modify fields as needed.
    """

    llm_config: "LLMConfig"
    """The LLM configuration object containing model settings and parameters."""
    worker_node_download_model: Optional["NodeModelDownloadable"] = None
    """Model download configuration for worker nodes. Used to specify how
    models should be downloaded and cached on worker nodes in distributed
    deployments."""
    placement_group: Optional[Any] = None
    """Ray placement group for resource allocation and scheduling. Controls
    where and how resources are allocated across the cluster."""
    runtime_env: Optional[Dict[str, Any]] = None
    """Runtime environment configuration for the Ray workers. Includes
    dependencies, environment variables, and other runtime settings."""
    custom_data: Dict[str, Any] = field(default_factory=dict)
    """Flexible dictionary for callback-specific state and data. Allows
    callbacks to store and share custom information during initialization."""
    run_downloads: bool = True
    """Whether to run model downloads during initialization. Set to False
    to skip downloading models."""


class Callback:
    """Protocol for custom initialization implementations.

    This protocol defines the interface for custom initialization logic
    for LLMEngine to be called in node_initialization.
    """

    def __init__(self, raise_error_on_callback: bool = True, **kwargs):
        self.raise_error_on_callback = raise_error_on_callback
        self.kwargs = kwargs

    async def on_before_node_init(self, ctx: CallbackCtx) -> Awaitable[None]:
        """Called before node initialization begins."""
        pass

    async def on_after_node_init(self, ctx: CallbackCtx) -> Awaitable[None]:
        """Called after node initialization completes."""
        pass

    @staticmethod
    async def run_callback(
        method_name: str, callback: "Callback", ctx: CallbackCtx
    ) -> Awaitable[None]:
        """Run a callback method either synchronously or asynchronously.

        Args:
            method_name: The name of the method to call on the callback
            callback: The callback instance to call the method on
            ctx: The callback context to pass to the method

        Raises:
            AttributeError: If the method doesn't exist on the callback
            Exception: Any exception raised by the callback method

        Returns:
            None
        """
        if not hasattr(callback, method_name):
            raise AttributeError(
                f"Callback {type(callback).__name__} does not have method '{method_name}'"
            )

        method = getattr(callback, method_name)

        try:
            # Check if the method is a coroutine function
            if inspect.iscoroutinefunction(method):
                await method(ctx)
            else:
                # For sync methods, run them in the event loop
                method(ctx)
        except Exception as e:
            if callback.raise_error_on_callback:
                raise Exception(
                    f"Error running callback method '{method_name}' on {type(callback).__name__}: {str(e)}"
                ) from e
            else:
                logger.error(
                    f"Error running callback method '{method_name}' on {type(callback).__name__}: {str(e)}"
                )


@dataclass
class CallbackConfig:
    """Configuration for the callback to be used in LLMConfig"""

    callback_class: Union[str, Type[Callback]] = Callback
    """Class to use for the callback. Can be custom user defined class"""
    callback_kwargs: Dict[str, Any] = field(default_factory=dict)
    """Keyword arguments to pass to the Callback class at construction."""
    raise_error_on_callback: bool = True
    """Whether to raise an error if a callback method fails."""


class TestingCallback(Callback):
    def __init__(self, **kwargs):
        print("TestingCallback __init__ kwargs: ", kwargs)

    def on_before_node_init(self, ctx: CallbackCtx):
        print("TestingCallback on_before_node_init")

    def on_after_node_init(self, ctx: CallbackCtx):
        print("TestingCallback on_after_node_init")
