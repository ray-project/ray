"""
Simplified Generic Callback System Implementation

This shows the COMPLETE implementation with just ONE core concept: Callbacks
"""

from typing import Any, Dict, Optional, List, Union, Callable, Protocol, runtime_checkable
import importlib
import logging
from abc import ABC

logger = logging.getLogger(__name__)


# =============================================================================
# CORE PRIMITIVE: Just this one concept!
# =============================================================================

@runtime_checkable
class Callback(Protocol):
    """A callback is anything callable with this signature.
    
    It receives:
    - phase: what point in execution (e.g., "pre_init", "post_download")
    - config: the configuration object (read-only, don't modify)
    - data: shared mutable state (read and write)
    
    It returns:
    - Optional dict to merge into data
    """
    
    def __call__(
        self,
        phase: str,
        config: Any,
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        ...


# =============================================================================
# RUNNER: Executes callbacks at different phases
# =============================================================================

class CallbackRunner:
    """Executes callbacks and manages shared data."""
    
    def __init__(self, callbacks: List[Callable]):
        self.callbacks = callbacks
        self.data: Dict[str, Any] = {}
    
    async def run(self, phase: str, config: Any) -> Dict[str, Any]:
        """Run all callbacks for this phase."""
        logger.debug(f"Phase: {phase} (data keys: {list(self.data.keys())})")
        
        for callback in self.callbacks:
            try:
                result = callback(phase, config, self.data)
                if result:
                    self.data.update(result)
            except Exception as e:
                logger.error(f"Callback {callback} failed at {phase}: {e}", exc_info=True)
                raise
        
        return self.data
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from shared data."""
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set value in shared data."""
        self.data[key] = value


# =============================================================================
# HELPERS: Load callbacks from strings
# =============================================================================

def load_callback(callback_spec: Union[str, Callable]) -> Callable:
    """Load a callback from a string path or return the callable."""
    
    if callable(callback_spec):
        return callback_spec
    
    # Parse string: "module.submodule:function" or "module.submodule.function"
    if ":" in callback_spec:
        module_path, name = callback_spec.rsplit(":", 1)
    else:
        module_path, name = callback_spec.rsplit(".", 1)
    
    try:
        module = importlib.import_module(module_path)
        callback = getattr(module, name)
        
        if not callable(callback):
            raise TypeError(f"{callback_spec} is not callable")
        
        logger.info(f"Loaded callback: {callback_spec}")
        return callback
        
    except ImportError as e:
        raise ImportError(
            f"Failed to import module '{module_path}' for callback '{callback_spec}'. "
            f"Make sure it's in PYTHONPATH. Error: {e}"
        ) from e
    except AttributeError as e:
        available = [x for x in dir(module) if not x.startswith("_")]
        raise AttributeError(
            f"Module '{module_path}' has no attribute '{name}'. "
            f"Available: {available}"
        ) from e


# =============================================================================
# USAGE EXAMPLE: Node Initialization with Callbacks
# =============================================================================

async def initialize_node(llm_config: "LLMConfig") -> "InitializeNodeOutput":
    """
    Initialize a node with callback support.
    
    This is the ONLY change needed to the existing code:
    1. Load callbacks from config
    2. Call runner.run() at key phases
    3. Read from runner.data instead of local variables
    """
    
    # Create runner with callbacks
    callbacks = [load_callback(cb) for cb in (llm_config.callbacks or [])]
    runner = CallbackRunner(callbacks)
    
    # PHASE 1: Pre-initialization
    await runner.run("pre_initialization", llm_config)
    
    # Check if callback wants to skip default behavior
    if runner.get("skip_default"):
        logger.info("Callbacks requested skip_default, using custom initialization")
        await runner.run("custom_initialization", llm_config)
        return build_output_from_data(runner.data)
    
    # PHASE 2: Setup download parameters
    # Callbacks can override these via data dict
    local_node_download = runner.get(
        "local_node_download_model",
        "TOKENIZER_ONLY"  # default
    )
    worker_node_download = runner.get(
        "worker_node_download_model",
        "MODEL_AND_TOKENIZER"  # default
    )
    
    await runner.run("pre_download", llm_config)
    
    # PHASE 3: Download model
    # (Existing code here)
    download_result = "simulated_download_result"
    runner.set("download_result", download_result)
    
    await runner.run("post_download", llm_config)
    
    # PHASE 4: Setup placement group
    # Callbacks can provide their own via data dict
    placement_group = runner.get("placement_group")
    if not placement_group:
        # Default behavior
        placement_group = "simulated_placement_group"
        runner.set("placement_group", placement_group)
    
    await runner.run("post_placement_group", llm_config)
    
    # PHASE 5: Engine initialization
    await runner.run("pre_engine_init", llm_config)
    
    # Engine init code...
    
    await runner.run("post_engine_init", llm_config)
    
    # PHASE 6: Final
    await runner.run("post_initialization", llm_config)
    
    # Build output from runner data
    return build_output_from_data(runner.data)


def build_output_from_data(data: Dict[str, Any]) -> "InitializeNodeOutput":
    """Build output object from callback data."""
    return {
        "placement_group": data.get("placement_group"),
        "runtime_env": data.get("runtime_env"),
        "extra_init_kwargs": data.get("extra_init_kwargs", {}),
    }


# =============================================================================
# EXAMPLE CALLBACKS: Show different patterns
# =============================================================================

# Pattern 1: Simple function
def logging_callback(phase: str, config: Any, data: Dict[str, Any]) -> None:
    """Just log each phase."""
    print(f"[{phase}] Starting with config: {getattr(config, 'model_id', 'unknown')}")


# Pattern 2: Function that modifies data
def custom_download_callback(
    phase: str,
    config: Any,
    data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Change download behavior."""
    if phase == "pre_download":
        return {
            "local_node_download_model": "MODEL_AND_TOKENIZER",
            "worker_node_download_model": "MODEL_AND_TOKENIZER",
        }


# Pattern 3: Stateful class-based callback
class MetricsCallback:
    """Track metrics across phases."""
    
    def __init__(self, output_path: str = "/tmp/metrics.json"):
        self.output_path = output_path
        self.metrics = {}
        self.start_time = None
    
    def __call__(
        self,
        phase: str,
        config: Any,
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        import time
        
        if phase == "pre_initialization":
            self.start_time = time.time()
        
        if phase.startswith("post_"):
            elapsed = time.time() - self.start_time
            self.metrics[phase] = elapsed
            print(f"{phase} took {elapsed:.2f}s")
        
        if phase == "post_initialization":
            # Save metrics
            import json
            with open(self.output_path, "w") as f:
                json.dump(self.metrics, f)
            print(f"Metrics saved to {self.output_path}")


# Pattern 4: Full replacement callback
def custom_initialization_callback(
    phase: str,
    config: Any,
    data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Completely replace default initialization."""
    
    if phase == "pre_initialization":
        # Signal to skip default
        return {"skip_default": True}
    
    if phase == "custom_initialization":
        # Do your own initialization
        print("Running CUSTOM initialization!")
        
        return {
            "placement_group": "my_custom_pg",
            "runtime_env": {"env_vars": {"CUSTOM": "true"}},
            "extra_init_kwargs": {"custom_init": True},
        }


# Pattern 5: Conditional callback (only acts on certain configs)
def gpu_optimization_callback(
    phase: str,
    config: Any,
    data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Only activate for GPU models."""
    
    # Check if this config needs GPU optimization
    if not getattr(config, 'use_gpu', False):
        return None  # Skip for non-GPU configs
    
    if phase == "pre_engine_init":
        print("Applying GPU optimizations...")
        return {
            "extra_init_kwargs": {
                "gpu_memory_utilization": 0.95,
                "tensor_parallel_size": 4,
            }
        }


# =============================================================================
# CONFIGURATION
# =============================================================================

class LLMConfig:
    """Example config showing how callbacks are specified."""
    
    def __init__(
        self,
        model_id: str,
        callbacks: Optional[List[Union[str, Callable]]] = None,
        use_gpu: bool = False,
    ):
        self.model_id = model_id
        self.callbacks = callbacks or []
        self.use_gpu = use_gpu


# =============================================================================
# USAGE EXAMPLES
# =============================================================================

async def example_1_simple():
    """Use a simple logging callback."""
    config = LLMConfig(
        model_id="llama-2-7b",
        callbacks=[logging_callback]
    )
    result = await initialize_node(config)
    print(f"Result: {result}")


async def example_2_from_string():
    """Load callback from string path."""
    config = LLMConfig(
        model_id="llama-2-7b",
        callbacks=["my_module.my_callback"]  # Would be loaded dynamically
    )
    result = await initialize_node(config)


async def example_3_stateful():
    """Use a stateful callback instance."""
    metrics = MetricsCallback(output_path="/tmp/init_metrics.json")
    
    config = LLMConfig(
        model_id="llama-2-7b",
        callbacks=[metrics]
    )
    result = await initialize_node(config)


async def example_4_multiple_callbacks():
    """Combine multiple callbacks."""
    metrics = MetricsCallback()
    
    config = LLMConfig(
        model_id="llama-2-7b",
        use_gpu=True,
        callbacks=[
            logging_callback,              # Log phases
            custom_download_callback,       # Modify download
            metrics,                        # Track timing
            gpu_optimization_callback,      # GPU opts
        ]
    )
    result = await initialize_node(config)


async def example_5_full_replacement():
    """Replace entire initialization."""
    config = LLMConfig(
        model_id="llama-2-7b",
        callbacks=[custom_initialization_callback]
    )
    result = await initialize_node(config)


# =============================================================================
# EXTENDING TO OTHER PARTS OF RAY
# =============================================================================

async def train_model(training_config: "TrainingConfig"):
    """Same pattern for training."""
    callbacks = [load_callback(cb) for cb in (training_config.callbacks or [])]
    runner = CallbackRunner(callbacks)
    
    await runner.run("pre_training", training_config)
    
    for epoch in range(training_config.num_epochs):
        await runner.run("pre_epoch", training_config)
        
        # Training logic...
        
        await runner.run("post_epoch", training_config)
    
    await runner.run("post_training", training_config)


async def process_dataset(dataset_config: "DatasetConfig"):
    """Same pattern for datasets."""
    callbacks = [load_callback(cb) for cb in (dataset_config.callbacks or [])]
    runner = CallbackRunner(callbacks)
    
    await runner.run("pre_load", dataset_config)
    
    # Load data...
    
    await runner.run("post_load", dataset_config)
    await runner.run("pre_transform", dataset_config)
    
    # Transform data...
    
    await runner.run("post_transform", dataset_config)


if __name__ == "__main__":
    import asyncio
    
    print("=" * 80)
    print("Example 1: Simple logging")
    print("=" * 80)
    asyncio.run(example_1_simple())
    
    print("\n" + "=" * 80)
    print("Example 3: Stateful metrics")
    print("=" * 80)
    asyncio.run(example_3_stateful())
    
    print("\n" + "=" * 80)
    print("Example 4: Multiple callbacks")
    print("=" * 80)
    asyncio.run(example_4_multiple_callbacks())
