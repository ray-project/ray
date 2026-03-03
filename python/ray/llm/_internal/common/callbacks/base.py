import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Type, Union

if TYPE_CHECKING:
    from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

logger = logging.getLogger(__name__)


@dataclass
class CallbackCtx:
    """
    Context object passed to all callback hooks.
    Callbacks can read and modify fields as needed.
    """

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
    run_init_node: bool = True
    """Whether to run model downloads during initialization. Set to False
    to skip downloading models."""


class CallbackBase:
    """Base class for custom initialization implementations.

    This class defines the interface for custom initialization logic
    for LLMEngine to be called in node_initialization.
    """

    def __init__(
        self,
        llm_config: "LLMConfig",
        raise_error_on_callback: bool = True,
        ctx_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.raise_error_on_callback = raise_error_on_callback
        self.kwargs = kwargs
        self.llm_config = llm_config

        # Create and store CallbackCtx internally using ctx_kwargs
        ctx_kwargs = ctx_kwargs or {}
        self.ctx = CallbackCtx(**ctx_kwargs)

    async def on_before_node_init(self) -> None:
        """Called before node initialization begins."""
        pass

    async def on_after_node_init(self) -> None:
        """Called after node initialization completes."""
        pass

    def on_before_download_model_files_distributed(self) -> None:
        """Called before model files are downloaded on each node."""
        pass

    def _get_method(self, method_name: str) -> Tuple[Callable, bool]:
        """Get a callback method."""
        if not hasattr(self, method_name):
            raise AttributeError(
                f"Callback {type(self).__name__} does not have method '{method_name}'"
            )
        return getattr(self, method_name), inspect.iscoroutinefunction(
            getattr(self, method_name)
        )

    def _handle_callback_error(self, method_name: str, e: Exception) -> None:
        if self.raise_error_on_callback:
            raise Exception(
                f"Error running callback method '{method_name}' on {type(self).__name__}: {str(e)}"
            ) from e
        else:
            logger.error(
                f"Error running callback method '{method_name}' on {type(self).__name__}: {str(e)}"
            )

    async def run_callback(self, method_name: str) -> None:
        """Run a callback method either synchronously or asynchronously.

        Args:
            method_name: The name of the method to call on the callback

        Returns:
            None
        """
        method, is_async = self._get_method(method_name)

        try:
            if is_async:
                await method()
            else:
                method()
        except Exception as e:
            self._handle_callback_error(method_name, e)

    def run_callback_sync(self, method_name: str) -> None:
        """Run a callback method synchronously

        Args:
            method_name: The name of the method to call on the callback
        Returns:
            None
        """
        method, is_async = self._get_method(method_name)

        try:
            if is_async:
                try:
                    loop = asyncio.get_running_loop()
                    loop.run_until_complete(method())
                except RuntimeError:
                    asyncio.run(method())
            else:
                method()
        except Exception as e:
            self._handle_callback_error(method_name, e)


@dataclass
class CallbackConfig:
    """Configuration for the callback to be used in LLMConfig"""

    callback_class: Union[str, Type[CallbackBase]] = CallbackBase
    """Class to use for the callback. Can be custom user defined class"""
    callback_kwargs: Dict[str, Any] = field(default_factory=dict)
    """Keyword arguments to pass to the Callback class at construction."""
    raise_error_on_callback: bool = True
    """Whether to raise an error if a callback method fails."""
