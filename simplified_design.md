# Simplified Callback Hook System for Ray

## Core Concept: Single Generic Hook Primitive

Instead of Strategy + Hooks + Context, we have ONE concept: **Callbacks**

A callback is simply a function (or callable) that gets invoked at specific points in the code.

## Design

### 1. Generic Callback Interface

```python
from typing import Any, Dict, Optional, Protocol, runtime_checkable

@runtime_checkable
class Callback(Protocol):
    """Protocol for a generic callback.
    
    A callback is any callable that accepts:
    - phase: str - identifier for where in execution we are
    - config: Any - the configuration object (e.g., LLMConfig)
    - data: Dict[str, Any] - mutable data that can be read/modified
    
    Returns:
    - Dict[str, Any] - data to merge back into the shared data dict
    """
    
    def __call__(
        self,
        phase: str,
        config: Any,
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        ...
```

### 2. Simple Callback Runner

```python
from typing import List, Union, Any, Dict
import importlib
import logging

logger = logging.getLogger(__name__)


class CallbackRunner:
    """Runs callbacks at various phases with shared data."""
    
    def __init__(self, callbacks: List[Callback]):
        self.callbacks = callbacks
        self.data: Dict[str, Any] = {}
    
    async def run(
        self,
        phase: str,
        config: Any,
    ) -> Dict[str, Any]:
        """Run all callbacks for a given phase.
        
        Args:
            phase: Identifier for the execution phase (e.g., "pre_init", "post_download")
            config: Configuration object passed to callbacks
            
        Returns:
            The shared data dict after all callbacks have run
        """
        logger.debug(f"Running callbacks for phase: {phase}")
        
        for i, callback in enumerate(self.callbacks):
            try:
                result = callback(phase, config, self.data)
                
                # If callback returns data, merge it
                if result:
                    self.data.update(result)
                    
            except Exception as e:
                logger.error(
                    f"Callback {i} ({callback}) failed at phase '{phase}': {e}",
                    exc_info=True
                )
                # Continue with other callbacks or raise depending on policy
                raise
        
        return self.data
```

### 3. Enhanced Configuration (Single Field!)

```python
from pydantic import Field, validator
from typing import List, Union, Callable, Any

class LLMConfig(BaseModelExtended):
    # ... existing fields ...
    
    callbacks: Optional[List[Union[str, Callable]]] = Field(
        default_factory=list,
        description=(
            "List of callbacks to customize behavior at various execution phases. "
            "Can be function/class paths like 'my_module.my_callback' or callable objects. "
            "Each callback receives (phase: str, config: Any, data: Dict) and returns "
            "optional Dict to merge into shared data."
        ),
    )
    
    @validator("callbacks", each_item=True)
    def validate_callback(cls, v):
        """Ensure each callback is valid."""
        if v is None:
            return v
        
        # String paths are valid
        if isinstance(v, str):
            if "." not in v:
                raise ValueError(
                    f"Callback path must be fully qualified (module.function), got: {v}"
                )
            return v
        
        # Callable objects are valid
        if callable(v):
            return v
        
        raise ValueError(
            f"Callback must be a string path or callable, got: {type(v)}"
        )
```

### 4. Usage in Node Initialization

```python
def load_callback(callback_spec: Union[str, Callable]) -> Callable:
    """Load a callback from string or return callable directly."""
    
    if callable(callback_spec):
        return callback_spec
    
    # Load from string
    if ":" in callback_spec:
        # Support module:function syntax
        module_path, func_name = callback_spec.rsplit(":", 1)
    else:
        # Support module.function syntax
        module_path, func_name = callback_spec.rsplit(".", 1)
    
    try:
        module = importlib.import_module(module_path)
        callback = getattr(module, func_name)
        
        if not callable(callback):
            raise TypeError(f"{callback_spec} is not callable")
        
        return callback
        
    except Exception as e:
        raise ImportError(
            f"Failed to load callback '{callback_spec}': {e}"
        ) from e


async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
    """Initialize node with optional callbacks."""
    
    # Load callbacks
    callbacks = [load_callback(cb) for cb in llm_config.callbacks]
    runner = CallbackRunner(callbacks)
    
    # Phase 1: Pre-initialization
    await runner.run("pre_initialization", llm_config)
    
    # Phase 2: Model download setup
    local_node_download = runner.data.get(
        "local_node_download_model",
        NodeModelDownloadable.TOKENIZER_ONLY
    )
    worker_node_download = runner.data.get(
        "worker_node_download_model", 
        NodeModelDownloadable.MODEL_AND_TOKENIZER
    )
    
    await runner.run("pre_download", llm_config)
    
    # Download model
    download_result = await download_model(...)
    runner.data["download_result"] = download_result
    
    await runner.run("post_download", llm_config)
    
    # Phase 3: Placement group setup
    pg = runner.data.get("placement_group")
    if not pg:
        pg = await setup_placement_group(...)
        runner.data["placement_group"] = pg
    
    await runner.run("post_placement_group", llm_config)
    
    # Phase 4: Engine initialization
    await runner.run("pre_engine_init", llm_config)
    
    # ... rest of initialization ...
    
    await runner.run("post_initialization", llm_config)
    
    # Build output from data
    output = InitializeNodeOutput(
        placement_group=runner.data["placement_group"],
        runtime_env=runner.data.get("runtime_env"),
        extra_init_kwargs=runner.data.get("extra_init_kwargs", {}),
    )
    
    return output
```

## Usage Examples

### Example 1: Simple Function Callback

```python
# my_callbacks.py

def log_initialization(phase: str, config: Any, data: Dict[str, Any]) -> None:
    """Log each phase of initialization."""
    if phase.startswith("pre_"):
        print(f"Starting: {phase}")
    elif phase.startswith("post_"):
        print(f"Completed: {phase}")

# Usage:
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=["my_callbacks.log_initialization"]
)
```

### Example 2: Class-Based Callback (Stateful)

```python
# my_callbacks.py

class CustomInitializationCallback:
    """Stateful callback that can track initialization progress."""
    
    def __init__(self, device_type: str = "cuda"):
        self.device_type = device_type
        self.start_time = None
    
    def __call__(
        self, 
        phase: str, 
        config: Any, 
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Handle different phases."""
        
        if phase == "pre_initialization":
            import time
            self.start_time = time.time()
            print(f"Initializing with device: {self.device_type}")
            
            # Modify initialization behavior
            return {
                "local_node_download_model": NodeModelDownloadable.MODEL_AND_TOKENIZER,
                "custom_device": self.device_type,
            }
        
        elif phase == "post_download":
            # Access shared data from previous phases
            download_result = data.get("download_result")
            print(f"Model downloaded: {download_result}")
            
        elif phase == "pre_engine_init":
            # Inject custom engine parameters
            return {
                "extra_init_kwargs": {
                    "device": self.device_type,
                    "custom_param": "value",
                }
            }
        
        elif phase == "post_initialization":
            import time
            elapsed = time.time() - self.start_time
            print(f"Initialization took {elapsed:.2f}s")

# Usage - instantiate and pass:
callback_instance = CustomInitializationCallback(device_type="cuda")

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=[callback_instance]
)
```

### Example 3: Lambda for Quick Modifications

```python
# Quick inline callback
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=[
        lambda phase, config, data: {"custom_flag": True} if phase == "pre_init" else None
    ]
)
```

### Example 4: Replace Entire Initialization

```python
# my_callbacks.py

def custom_full_initialization(
    phase: str,
    config: Any,
    data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Completely replace the default initialization."""
    
    if phase == "pre_initialization":
        # Signal to skip default initialization
        return {"skip_default_init": True}
    
    elif phase == "pre_engine_init":
        # Do custom initialization here
        pg = create_custom_placement_group()
        env = create_custom_runtime_env()
        
        return {
            "placement_group": pg,
            "runtime_env": env,
            "extra_init_kwargs": {"custom": True}
        }

# Usage:
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=["my_callbacks.custom_full_initialization"]
)
```

## Benefits of This Design

### 1. Single Concept
- Only ONE thing to learn: callbacks
- No confusion between Strategy vs Hook vs Context
- "Context" is just the shared `data` dict

### 2. Maximum Flexibility
- Can modify behavior incrementally (change one thing)
- Can replace behavior entirely (take over a phase)
- Can track state across phases (class-based callbacks)
- Can access/modify shared data

### 3. Generic & Reusable
This same pattern can be used EVERYWHERE in Ray:

```python
# In dataset loading
DatasetConfig(
    callbacks=["my_callbacks.custom_loader"]
)

# In training
TrainingConfig(
    callbacks=["my_callbacks.checkpoint_hook"]
)

# In serving
ServingConfig(
    callbacks=["my_callbacks.request_preprocessor"]
)
```

### 4. Simple to Test

```python
def test_callback():
    data = {}
    config = LLMConfig(model_id="test")
    
    def my_callback(phase, config, data):
        return {"test_key": "test_value"}
    
    runner = CallbackRunner([my_callback])
    result = await runner.run("test_phase", config)
    
    assert result["test_key"] == "test_value"
```

### 5. Backward Compatible
- No callbacks = default behavior
- Empty callback list = default behavior
- Callbacks only run if provided

## What Goes Into "Data"?

The `data` dict is the shared state between:
1. The system (Ray's default initialization)
2. All callbacks

It contains:
- **Inputs**: Things the system provides (config, downloaded models, etc.)
- **Outputs**: Things callbacks want to customize (placement_group, runtime_env, etc.)
- **Intermediate state**: Anything callbacks want to share with each other

Example data dict during initialization:
```python
{
    # System provides:
    "download_result": ModelDownloadResult(...),
    "default_placement_group": PlacementGroup(...),
    
    # Callback overrides:
    "placement_group": CustomPlacementGroup(...),
    "extra_init_kwargs": {"custom": True},
    
    # Callback-to-callback communication:
    "my_custom_state": {...},
}
```

## Comparison

| Aspect | Current PR | My Previous Design | This Simplified Design |
|--------|-----------|-------------------|----------------------|
| **# of Concepts** | 1 (CustomInitialization) | 3 (Strategy + Hooks + Context) | 1 (Callback) |
| **Lines of Code** | ~40 | ~200 | ~60 |
| **Flexibility** | Replace all | Hooks at phases | Hooks at phases + modify data |
| **Reusable?** | No | Somewhat | Yes - generic pattern |
| **Learning Curve** | Easy | Medium | Easy |
| **Power** | Medium | High | High |

The simplified design gives you the same power as my previous design but with ONE simple concept instead of three!
