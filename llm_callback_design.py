"""
Node Initialization Callback System for ray.llm._internal.serve

This provides a structured way to customize LLM node initialization with:
1. Protocol class with well-defined hook methods
2. Typed HookContext object (future-proof)
3. Per-process singleton via get_or_create_callback()
"""

from typing import Any, Optional, Protocol, Type, Union, Dict, runtime_checkable
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# HOOK CONTEXT: Typed, flexible, future-proof data object
# =============================================================================

@dataclass
class NodeInitializationContext:
    """Context object passed to all hook methods.
    
    This is a typed data container that holds all state during node initialization.
    It's future-proof: new fields can be added without breaking existing callbacks.
    
    Attributes:
        llm_config: The LLM configuration (read-only, don't modify)
        
        # Download phase
        local_node_download_model: Type of download for local node
        worker_node_download_model: Type of download for worker nodes
        download_result: Result from model download (set by system)
        
        # Placement group phase
        placement_group: The placement group (can be overridden by callback)
        
        # Runtime environment phase
        runtime_env: The runtime environment (can be overridden by callback)
        
        # Engine initialization phase
        extra_init_kwargs: Extra kwargs for engine initialization (mutable)
        
        # Extensibility
        custom_data: Dict for callback-specific data that needs to persist across hooks
    """
    
    # Config (read-only)
    llm_config: Any  # LLMConfig type
    
    # Download phase
    local_node_download_model: Optional[str] = None
    worker_node_download_model: Optional[str] = None
    download_result: Optional[Any] = None
    
    # Placement group phase
    placement_group: Optional[Any] = None
    
    # Runtime environment phase
    runtime_env: Optional[Dict[str, Any]] = None
    
    # Engine initialization phase
    extra_init_kwargs: Dict[str, Any] = field(default_factory=dict)
    
    # For callback-specific data that persists across hooks
    custom_data: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set defaults from llm_config if not provided."""
        if self.local_node_download_model is None:
            self.local_node_download_model = "TOKENIZER_ONLY"
        if self.worker_node_download_model is None:
            self.worker_node_download_model = "MODEL_AND_TOKENIZER"


# =============================================================================
# CALLBACK PROTOCOL: Well-defined hook methods
# =============================================================================

@runtime_checkable
class NodeInitializationCallback(Protocol):
    """Protocol for node initialization callbacks.
    
    Users implement this protocol to customize node initialization behavior.
    All methods are optional - only override the hooks you need.
    
    Each method receives a NodeInitializationContext with all state.
    Methods can:
    - Read from context (e.g., ctx.llm_config, ctx.download_result)
    - Modify context fields (e.g., ctx.placement_group = my_pg)
    - Store custom state in ctx.custom_data for later hooks
    
    Return False from any hook to skip default behavior for that phase.
    """
    
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> Optional[bool]:
        """Called before any initialization begins.
        
        Use this to:
        - Modify download parameters (ctx.local_node_download_model, etc.)
        - Set up any state you need
        - Store data in ctx.custom_data
        
        Returns:
            False to skip all default initialization (you handle everything)
            None or True to continue with default initialization
        """
        ...
    
    def on_pre_download(self, ctx: NodeInitializationContext) -> Optional[bool]:
        """Called before model download.
        
        Use this to:
        - Modify download parameters
        - Prepare for download (e.g., clear cache)
        
        Returns:
            False to skip model download
            None or True to continue with default download
        """
        ...
    
    def on_post_download(self, ctx: NodeInitializationContext) -> None:
        """Called after model download completes.
        
        Use this to:
        - Inspect ctx.download_result
        - Perform post-download setup (e.g., validate files)
        - Modify downloaded artifacts
        """
        ...
    
    def on_pre_placement_group(self, ctx: NodeInitializationContext) -> Optional[bool]:
        """Called before placement group setup.
        
        Use this to:
        - Provide custom placement group via ctx.placement_group
        
        Returns:
            False to skip default placement group creation
            None or True to continue with default
        """
        ...
    
    def on_post_placement_group(self, ctx: NodeInitializationContext) -> None:
        """Called after placement group is set up.
        
        Use this to:
        - Inspect ctx.placement_group
        - Validate placement group configuration
        """
        ...
    
    def on_pre_engine_init(self, ctx: NodeInitializationContext) -> None:
        """Called before engine initialization.
        
        Use this to:
        - Add extra_init_kwargs via ctx.extra_init_kwargs.update({...})
        - Set custom runtime_env via ctx.runtime_env
        """
        ...
    
    def on_post_engine_init(self, ctx: NodeInitializationContext) -> None:
        """Called after engine initialization.
        
        Use this to:
        - Perform any post-initialization setup
        - Validate engine state
        """
        ...
    
    def on_post_initialization(self, ctx: NodeInitializationContext) -> None:
        """Called after all initialization is complete.
        
        Use this to:
        - Clean up resources
        - Log final state
        - Perform validation
        """
        ...


# =============================================================================
# CONFIGURATION
# =============================================================================

class LLMConfig:
    """LLM configuration with callback support."""
    
    def __init__(
        self,
        model_id: str,
        callback_class: Optional[Union[str, Type[NodeInitializationCallback]]] = None,
        callback_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Args:
            model_id: Model identifier
            callback_class: Callback class (string path or type) or None for no callback
            callback_kwargs: Kwargs to pass to callback constructor
            **kwargs: Other LLM config options
        """
        self.model_id = model_id
        self.callback_class = callback_class
        self.callback_kwargs = callback_kwargs or {}
        self._callback_instance = None  # Per-process singleton
        
        # Other config fields...
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def get_or_create_callback(self) -> Optional[NodeInitializationCallback]:
        """Get or create the callback instance for this process.
        
        This ensures one callback instance per process (singleton pattern).
        The instance is cached so the same object is used across all hooks.
        
        Returns:
            NodeInitializationCallback instance or None if no callback configured
        """
        if self.callback_class is None:
            return None
        
        # Return cached instance if exists
        if self._callback_instance is not None:
            return self._callback_instance
        
        # Create new instance
        callback_class = self._load_callback_class()
        self._callback_instance = callback_class(**self.callback_kwargs)
        
        logger.info(f"Created callback instance: {self._callback_instance}")
        return self._callback_instance
    
    def _load_callback_class(self) -> Type[NodeInitializationCallback]:
        """Load callback class from string or return type directly."""
        if isinstance(self.callback_class, str):
            # Load from string path like "my_module.MyCallback"
            import importlib
            
            if ":" in self.callback_class:
                module_path, class_name = self.callback_class.rsplit(":", 1)
            else:
                module_path, class_name = self.callback_class.rsplit(".", 1)
            
            try:
                module = importlib.import_module(module_path)
                callback_class = getattr(module, class_name)
                
                logger.info(f"Loaded callback class: {self.callback_class}")
                return callback_class
                
            except Exception as e:
                raise ImportError(
                    f"Failed to load callback class '{self.callback_class}': {e}"
                ) from e
        else:
            # Already a type
            return self.callback_class


# =============================================================================
# INITIALIZATION WITH CALLBACKS
# =============================================================================

async def initialize_node(llm_config: LLMConfig) -> "InitializeNodeOutput":
    """Initialize node with optional callback hooks.
    
    This is the main initialization function that invokes callbacks at each phase.
    
    Args:
        llm_config: LLM configuration with optional callback
        
    Returns:
        InitializeNodeOutput with placement_group, runtime_env, extra_init_kwargs
    """
    # Get callback instance (if configured)
    callback = llm_config.get_or_create_callback()
    
    # Create context object
    ctx = NodeInitializationContext(llm_config=llm_config)
    
    # Hook: Pre-initialization
    if callback:
        skip_default = await _invoke_hook(callback, "on_pre_initialization", ctx)
        if skip_default is False:
            logger.info("Callback requested to skip default initialization")
            # Callback must provide everything in context
            return _build_output_from_context(ctx)
    
    # Hook: Pre-download
    skip_download = False
    if callback:
        skip_download = await _invoke_hook(callback, "on_pre_download", ctx)
    
    if not skip_download:
        # Perform model download using parameters from context
        logger.info(
            f"Downloading model: local={ctx.local_node_download_model}, "
            f"worker={ctx.worker_node_download_model}"
        )
        
        # Original download logic here...
        download_result = await _download_model_internal(
            llm_config,
            ctx.local_node_download_model,
            ctx.worker_node_download_model,
        )
        ctx.download_result = download_result
    
    # Hook: Post-download
    if callback:
        await _invoke_hook(callback, "on_post_download", ctx)
    
    # Hook: Pre-placement group
    skip_pg = False
    if callback:
        skip_pg = await _invoke_hook(callback, "on_pre_placement_group", ctx)
    
    if not skip_pg and ctx.placement_group is None:
        # Create default placement group
        pg = await _setup_placement_group_internal(llm_config)
        ctx.placement_group = pg
    
    # Hook: Post-placement group
    if callback:
        await _invoke_hook(callback, "on_post_placement_group", ctx)
    
    # Hook: Pre-engine init
    if callback:
        await _invoke_hook(callback, "on_pre_engine_init", ctx)
    
    # Set runtime env if not already set by callback
    if ctx.runtime_env is None:
        ctx.runtime_env = _build_runtime_env(llm_config)
    
    # Original engine initialization logic...
    # Uses ctx.extra_init_kwargs which callback may have modified
    
    # Hook: Post-engine init
    if callback:
        await _invoke_hook(callback, "on_post_engine_init", ctx)
    
    # Hook: Post-initialization
    if callback:
        await _invoke_hook(callback, "on_post_initialization", ctx)
    
    return _build_output_from_context(ctx)


async def _invoke_hook(
    callback: NodeInitializationCallback,
    method_name: str,
    ctx: NodeInitializationContext
) -> Optional[bool]:
    """Invoke a callback hook method if it exists.
    
    Args:
        callback: The callback instance
        method_name: Name of the hook method (e.g., "on_pre_download")
        ctx: The context object
        
    Returns:
        The return value from the hook (if any)
    """
    method = getattr(callback, method_name, None)
    if method is None:
        return None
    
    try:
        result = method(ctx)
        # Handle async methods
        if hasattr(result, "__await__"):
            result = await result
        return result
    except Exception as e:
        logger.error(
            f"Callback hook {method_name} failed: {e}",
            exc_info=True
        )
        raise


def _build_output_from_context(ctx: NodeInitializationContext) -> Dict[str, Any]:
    """Build InitializeNodeOutput from context."""
    return {
        "placement_group": ctx.placement_group,
        "runtime_env": ctx.runtime_env,
        "extra_init_kwargs": ctx.extra_init_kwargs,
    }


# Placeholder functions (would be real implementations in Ray)
async def _download_model_internal(llm_config, local_download, worker_download):
    return "simulated_download_result"

async def _setup_placement_group_internal(llm_config):
    return "simulated_placement_group"

def _build_runtime_env(llm_config):
    return {"env_vars": {}}


# =============================================================================
# USAGE EXAMPLES
# =============================================================================

class CustomDownloadCallback:
    """Example: Force download on all nodes."""
    
    def on_pre_download(self, ctx: NodeInitializationContext) -> None:
        """Modify download parameters."""
        print(f"Custom download for model: {ctx.llm_config.model_id}")
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"


class MetricsCallback:
    """Example: Track initialization metrics."""
    
    def __init__(self, output_path: str = "/tmp/metrics.json"):
        self.output_path = output_path
        self.start_time = None
        self.metrics = {}
    
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> None:
        import time
        self.start_time = time.time()
        ctx.custom_data["start_time"] = self.start_time
    
    def on_post_download(self, ctx: NodeInitializationContext) -> None:
        import time
        elapsed = time.time() - self.start_time
        self.metrics["download_time"] = elapsed
        print(f"Download took {elapsed:.2f}s")
    
    def on_post_initialization(self, ctx: NodeInitializationContext) -> None:
        import time
        import json
        
        total_time = time.time() - self.start_time
        self.metrics["total_time"] = total_time
        
        with open(self.output_path, "w") as f:
            json.dump(self.metrics, f)
        
        print(f"Initialization took {total_time:.2f}s")
        print(f"Metrics saved to {self.output_path}")


class CustomPlacementGroupCallback:
    """Example: Provide custom placement group."""
    
    def __init__(self, num_gpus: int = 4):
        self.num_gpus = num_gpus
    
    def on_pre_placement_group(self, ctx: NodeInitializationContext) -> bool:
        """Override placement group creation."""
        print(f"Creating custom placement group with {self.num_gpus} GPUs")
        
        # Create your custom placement group
        ctx.placement_group = f"custom_pg_with_{self.num_gpus}_gpus"
        
        # Return False to skip default placement group creation
        return False


class FullCustomInitializationCallback:
    """Example: Completely replace default initialization."""
    
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> bool:
        """Skip all default initialization."""
        print("Using fully custom initialization")
        
        # Set up everything yourself
        ctx.placement_group = "my_custom_pg"
        ctx.runtime_env = {"env_vars": {"CUSTOM": "true"}}
        ctx.extra_init_kwargs = {"custom_init": True}
        
        # Return False to skip all default behavior
        return False


class GPUOptimizationCallback:
    """Example: Add GPU-specific optimizations."""
    
    def on_pre_engine_init(self, ctx: NodeInitializationContext) -> None:
        """Add GPU optimization parameters."""
        print("Applying GPU optimizations")
        
        ctx.extra_init_kwargs.update({
            "gpu_memory_utilization": 0.95,
            "tensor_parallel_size": 4,
            "max_num_batched_tokens": 8192,
        })
        
        # Store some state for later hooks
        ctx.custom_data["gpu_optimized"] = True
    
    def on_post_initialization(self, ctx: NodeInitializationContext) -> None:
        """Verify GPU optimization was applied."""
        if ctx.custom_data.get("gpu_optimized"):
            print("✓ GPU optimization applied successfully")


# =============================================================================
# USAGE
# =============================================================================

async def example_1_custom_download():
    """Example 1: Override download behavior."""
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback_class=CustomDownloadCallback,
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_2_metrics():
    """Example 2: Track metrics during initialization."""
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback_class=MetricsCallback,
        callback_kwargs={"output_path": "/tmp/init_metrics.json"}
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_3_from_string():
    """Example 3: Load callback from string path."""
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback_class="my_company.callbacks.CustomCallback",
        callback_kwargs={"some_param": "value"}
    )
    
    result = await initialize_node(config)


async def example_4_custom_pg():
    """Example 4: Custom placement group."""
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback_class=CustomPlacementGroupCallback,
        callback_kwargs={"num_gpus": 8}
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_5_full_custom():
    """Example 5: Complete custom initialization."""
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback_class=FullCustomInitializationCallback,
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_6_gpu_opts():
    """Example 6: GPU optimizations."""
    config = LLMConfig(
        model_id="meta-llama/Llama-2-7b",
        callback_class=GPUOptimizationCallback,
    )
    
    result = await initialize_node(config)
    print(f"Result: {result}")


if __name__ == "__main__":
    import asyncio
    
    print("=" * 80)
    print("Example 1: Custom Download")
    print("=" * 80)
    asyncio.run(example_1_custom_download())
    
    print("\n" + "=" * 80)
    print("Example 2: Metrics Tracking")
    print("=" * 80)
    asyncio.run(example_2_metrics())
    
    print("\n" + "=" * 80)
    print("Example 4: Custom Placement Group")
    print("=" * 80)
    asyncio.run(example_4_custom_pg())
    
    print("\n" + "=" * 80)
    print("Example 5: Full Custom Init")
    print("=" * 80)
    asyncio.run(example_5_full_custom())
    
    print("\n" + "=" * 80)
    print("Example 6: GPU Optimizations")
    print("=" * 80)
    asyncio.run(example_6_gpu_opts())
