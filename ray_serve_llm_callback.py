"""
RayServeLLMCallback - Callback system for LLM node initialization in Ray Serve.

This provides a lean, extensible way to customize node initialization behavior.
Start with essential hooks, add more over time as needed.
"""

from typing import Any, Optional, Protocol, runtime_checkable, Dict
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# CALLBACK CONTEXT: Centralized mutable state
# =============================================================================

@dataclass
class CallbackCtx:
    """Context object passed to all callback hooks.
    
    This is a centralized place for all state during node initialization.
    Callbacks can read and modify fields as needed.
    
    Design: Start lean with only essential fields. Add more over time as needed.
    The custom_data dict provides flexibility for callback-specific state.
    """
    
    # Config (read-only - don't modify this)
    llm_config: Any  # LLMConfig type
    
    # Model download customization
    local_node_download_model: Optional[str] = None
    worker_node_download_model: Optional[str] = None
    
    # Placement group (can be overridden by callback)
    placement_group: Optional[Any] = None
    
    # Extra initialization kwargs (mutable - callbacks can add to this)
    extra_init_kwargs: Dict[str, Any] = field(default_factory=dict)
    
    # Extensibility: For callback-specific data that needs to persist across hooks
    custom_data: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# CALLBACK PROTOCOL: Well-defined hooks
# =============================================================================

@runtime_checkable
class RayServeLLMCallback(Protocol):
    """Protocol for LLM node initialization callbacks.
    
    Users implement this protocol to customize initialization behavior.
    All methods are optional - only implement the hooks you need.
    
    Design philosophy:
    - Start lean: Only essential hooks for current needs
    - Future-proof: Easy to add new hooks without breaking existing callbacks
    - Clear naming: Method names clearly indicate when they're called
    
    Each hook receives CallbackCtx which contains all initialization state.
    Hooks can read from ctx and modify fields as needed.
    """
    
    def on_before_init(self, ctx: CallbackCtx) -> Optional[bool]:
        """Called at the very beginning of node initialization.
        
        Use this to:
        - Set up any initial state you need
        - Modify download parameters (ctx.local_node_download_model, etc.)
        - Store data in ctx.custom_data for later hooks
        
        Args:
            ctx: The callback context with all initialization state
            
        Returns:
            False to skip all default initialization (callback takes over completely)
            None or True to continue with default initialization
        """
        ...
    
    def on_before_download(self, ctx: CallbackCtx) -> Optional[bool]:
        """Called before model download begins.
        
        Use this to:
        - Modify download parameters
        - Prepare for download (e.g., pre-allocate disk space, clear cache)
        
        Args:
            ctx: The callback context
            
        Returns:
            False to skip model download
            None or True to proceed with default download
        """
        ...
    
    def on_after_init(self, ctx: CallbackCtx) -> None:
        """Called after all initialization is complete.
        
        Use this to:
        - Perform final setup or validation
        - Clean up resources
        - Log final state or metrics
        
        Args:
            ctx: The callback context with final initialization state
        """
        ...


# =============================================================================
# LLMCONFIG INTEGRATION
# =============================================================================

class LLMConfig:
    """LLM configuration with callback support.
    
    Example:
        config = LLMConfig(
            model_id="meta-llama/Llama-2-7b",
            callback="my_module.MyCallback",
            callback_kwargs={"param": "value"}
        )
    """
    
    def __init__(
        self,
        model_id: str,
        callback: Optional[str] = None,
        callback_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Args:
            model_id: Model identifier
            callback: Callback class (string path like "module.ClassName") or None
            callback_kwargs: Kwargs to pass to callback constructor
            **kwargs: Other LLM config options
        """
        self.model_id = model_id
        self.callback = callback
        self.callback_kwargs = callback_kwargs or {}
        self._callback_instance = None  # Per-process singleton
        
        # Other config fields
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def get_or_create_callback(self) -> Optional[RayServeLLMCallback]:
        """Get or create the callback instance for this process.
        
        This ensures one callback instance per process (singleton pattern).
        The instance is cached so the same object is used across all hooks.
        
        Returns:
            RayServeLLMCallback instance or None if no callback configured
        """
        if self.callback is None:
            return None
        
        # Return cached instance if exists
        if self._callback_instance is not None:
            return self._callback_instance
        
        # Create new instance
        callback_class = self._load_callback_class()
        self._callback_instance = callback_class(**self.callback_kwargs)
        
        logger.info(f"Created callback instance: {self._callback_instance}")
        return self._callback_instance
    
    def _load_callback_class(self):
        """Load callback class from string path."""
        import importlib
        
        if ":" in self.callback:
            module_path, class_name = self.callback.rsplit(":", 1)
        else:
            module_path, class_name = self.callback.rsplit(".", 1)
        
        try:
            module = importlib.import_module(module_path)
            callback_class = getattr(module, class_name)
            
            logger.info(f"Loaded callback class: {self.callback}")
            return callback_class
            
        except Exception as e:
            raise ImportError(
                f"Failed to load callback class '{self.callback}': {e}"
            ) from e


# =============================================================================
# NODE INITIALIZATION WITH CALLBACKS
# =============================================================================

async def initialize_node(llm_config: LLMConfig) -> Dict[str, Any]:
    """Initialize node with optional callback customization.
    
    This is the main initialization function that invokes callbacks at each phase.
    
    Args:
        llm_config: LLM configuration with optional callback
        
    Returns:
        Dict with placement_group, extra_init_kwargs, etc.
    """
    # Get callback instance (if configured)
    callback = llm_config.get_or_create_callback()
    
    # Create context object with defaults
    ctx = CallbackCtx(
        llm_config=llm_config,
        local_node_download_model="TOKENIZER_ONLY",
        worker_node_download_model="MODEL_AND_TOKENIZER",
    )
    
    # Hook: on_before_init
    if callback:
        skip_default = await _invoke_hook(callback, "on_before_init", ctx)
        if skip_default is False:
            logger.info("Callback requested to skip default initialization")
            return _build_output(ctx)
    
    # Hook: on_before_download
    skip_download = False
    if callback:
        skip_download = await _invoke_hook(callback, "on_before_download", ctx)
    
    if not skip_download:
        # Perform model download using parameters from context
        logger.info(
            f"Downloading model: local={ctx.local_node_download_model}, "
            f"worker={ctx.worker_node_download_model}"
        )
        
        # Original download logic here...
        await _download_model(
            llm_config,
            ctx.local_node_download_model,
            ctx.worker_node_download_model,
        )
    
    # Setup placement group (if not provided by callback)
    if ctx.placement_group is None:
        ctx.placement_group = await _setup_placement_group(llm_config)
    
    # Original initialization logic continues...
    # (All existing Ray LLM initialization code)
    
    # Hook: on_after_init
    if callback:
        await _invoke_hook(callback, "on_after_init", ctx)
    
    return _build_output(ctx)


async def _invoke_hook(
    callback: RayServeLLMCallback,
    method_name: str,
    ctx: CallbackCtx
) -> Optional[bool]:
    """Invoke a callback hook method if it exists.
    
    Args:
        callback: The callback instance
        method_name: Name of the hook method (e.g., "on_before_download")
        ctx: The context object
        
    Returns:
        The return value from the hook (if any)
    """
    method = getattr(callback, method_name, None)
    if method is None:
        return None
    
    try:
        logger.debug(f"Invoking callback hook: {method_name}")
        result = method(ctx)
        
        # Handle async methods
        if hasattr(result, "__await__"):
            result = await result
        
        return result
        
    except Exception as e:
        logger.error(f"Callback hook {method_name} failed: {e}", exc_info=True)
        raise RuntimeError(f"Callback hook {method_name} failed") from e


def _build_output(ctx: CallbackCtx) -> Dict[str, Any]:
    """Build output dict from context."""
    return {
        "placement_group": ctx.placement_group,
        "extra_init_kwargs": ctx.extra_init_kwargs,
    }


# Placeholder functions (would be real implementations in Ray)
async def _download_model(llm_config, local_download, worker_download):
    """Original model download logic."""
    logger.info("Model download complete")

async def _setup_placement_group(llm_config):
    """Original placement group setup logic."""
    return "default_placement_group"


# =============================================================================
# USAGE EXAMPLES
# =============================================================================

class CustomDownloadCallback:
    """Example: Force download model on all nodes."""
    
    def on_before_download(self, ctx: CallbackCtx) -> None:
        """Modify download behavior."""
        logger.info(f"CustomDownloadCallback: Forcing MODEL_AND_TOKENIZER on all nodes")
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"


class MetricsCallback:
    """Example: Track initialization metrics."""
    
    def __init__(self, output_path: str = "/tmp/metrics.json"):
        self.output_path = output_path
        self.start_time = None
        self.metrics = {}
    
    def on_before_init(self, ctx: CallbackCtx) -> None:
        """Start timing."""
        import time
        self.start_time = time.time()
        logger.info("MetricsCallback: Starting initialization")
    
    def on_after_init(self, ctx: CallbackCtx) -> None:
        """Save metrics."""
        import time
        import json
        
        elapsed = time.time() - self.start_time
        self.metrics["total_time"] = elapsed
        self.metrics["model_id"] = ctx.llm_config.model_id
        
        with open(self.output_path, "w") as f:
            json.dump(self.metrics, f)
        
        logger.info(f"MetricsCallback: Initialization took {elapsed:.2f}s")


class CustomPlacementGroupCallback:
    """Example: Provide custom placement group."""
    
    def __init__(self, num_gpus: int = 4):
        self.num_gpus = num_gpus
    
    def on_before_init(self, ctx: CallbackCtx) -> None:
        """Set custom placement group."""
        logger.info(f"CustomPlacementGroupCallback: Creating PG with {self.num_gpus} GPUs")
        
        # In real implementation, would create actual placement group
        ctx.placement_group = f"custom_pg_{self.num_gpus}_gpus"


class FullCustomInitCallback:
    """Example: Completely replace default initialization."""
    
    def on_before_init(self, ctx: CallbackCtx) -> bool:
        """Take over all initialization."""
        logger.info("FullCustomInitCallback: Using fully custom initialization")
        
        # Set up everything yourself
        ctx.placement_group = "my_custom_pg"
        ctx.extra_init_kwargs = {
            "custom_init": True,
            "custom_param": "value"
        }
        
        # Return False to skip all default behavior
        return False


# =============================================================================
# TESTS / EXAMPLES
# =============================================================================

async def example_1_custom_download():
    """Example 1: Force download on all nodes."""
    print("\n" + "=" * 80)
    print("Example 1: Custom Download Behavior")
    print("=" * 80)
    
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback="__main__.CustomDownloadCallback"
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_2_metrics():
    """Example 2: Track initialization metrics."""
    print("\n" + "=" * 80)
    print("Example 2: Track Metrics")
    print("=" * 80)
    
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback="__main__.MetricsCallback",
        callback_kwargs={"output_path": "/tmp/llm_init_metrics.json"}
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_3_custom_pg():
    """Example 3: Custom placement group."""
    print("\n" + "=" * 80)
    print("Example 3: Custom Placement Group")
    print("=" * 80)
    
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback="__main__.CustomPlacementGroupCallback",
        callback_kwargs={"num_gpus": 8}
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_4_full_custom():
    """Example 4: Complete custom initialization."""
    print("\n" + "=" * 80)
    print("Example 4: Full Custom Initialization")
    print("=" * 80)
    
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback="__main__.FullCustomInitCallback"
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


if __name__ == "__main__":
    import asyncio
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )
    
    asyncio.run(example_1_custom_download())
    asyncio.run(example_2_metrics())
    asyncio.run(example_3_custom_pg())
    asyncio.run(example_4_full_custom())
    
    print("\n" + "=" * 80)
    print("✓ All examples completed successfully!")
    print("=" * 80)
