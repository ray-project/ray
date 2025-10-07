# Node Initialization Callback System for Ray LLM

## Design Overview

A focused callback system for `ray.llm._internal.serve` with:

1. ✅ **Protocol class** with well-defined hook methods
2. ✅ **Typed context object** (`NodeInitializationContext`) that's future-proof
3. ✅ **Per-process singleton** via `get_or_create_callback()`
4. ✅ **LLM-specific** - not generic, tailored to node initialization needs

## Core Components

### 1. NodeInitializationContext (Typed Hook Context)

```python
@dataclass
class NodeInitializationContext:
    """Typed data container passed to all hook methods."""
    
    # Read-only config
    llm_config: LLMConfig
    
    # Download phase (read/write)
    local_node_download_model: Optional[str]
    worker_node_download_model: Optional[str]
    download_result: Optional[Any]
    
    # Placement group phase (read/write)
    placement_group: Optional[Any]
    
    # Runtime environment phase (read/write)
    runtime_env: Optional[Dict[str, Any]]
    
    # Engine init phase (read/write)
    extra_init_kwargs: Dict[str, Any]
    
    # For callback-specific data
    custom_data: Dict[str, Any]
```

**Why a dataclass?**
- ✅ Typed fields with autocomplete
- ✅ Future-proof: add new fields without breaking existing callbacks
- ✅ `custom_data` dict for callback-specific state

### 2. NodeInitializationCallback (Protocol)

```python
@runtime_checkable
class NodeInitializationCallback(Protocol):
    """Protocol with well-defined hook methods."""
    
    # All methods are optional - only implement what you need
    
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> Optional[bool]:
        """Before any initialization. Return False to skip defaults."""
        ...
    
    def on_pre_download(self, ctx: NodeInitializationContext) -> Optional[bool]:
        """Before model download. Modify ctx.local_node_download_model, etc."""
        ...
    
    def on_post_download(self, ctx: NodeInitializationContext) -> None:
        """After download. Access ctx.download_result."""
        ...
    
    def on_pre_placement_group(self, ctx: NodeInitializationContext) -> Optional[bool]:
        """Before PG setup. Set ctx.placement_group to override."""
        ...
    
    def on_post_placement_group(self, ctx: NodeInitializationContext) -> None:
        """After PG setup. Inspect ctx.placement_group."""
        ...
    
    def on_pre_engine_init(self, ctx: NodeInitializationContext) -> None:
        """Before engine init. Modify ctx.extra_init_kwargs."""
        ...
    
    def on_post_engine_init(self, ctx: NodeInitializationContext) -> None:
        """After engine init."""
        ...
    
    def on_post_initialization(self, ctx: NodeInitializationContext) -> None:
        """After all initialization complete."""
        ...
```

**Why a Protocol?**
- ✅ Well-defined hooks (no arbitrary string phases)
- ✅ Type checking and autocomplete
- ✅ Clear contract for what each hook does
- ✅ Duck typing: any class with these methods works

### 3. LLMConfig Integration

```python
class LLMConfig:
    def __init__(
        self,
        model_id: str,
        callback_class: Optional[Union[str, Type[NodeInitializationCallback]]] = None,
        callback_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        self.model_id = model_id
        self.callback_class = callback_class
        self.callback_kwargs = callback_kwargs or {}
        self._callback_instance = None  # Singleton per process
    
    def get_or_create_callback(self) -> Optional[NodeInitializationCallback]:
        """Get or create callback instance (singleton per process)."""
        if self.callback_class is None:
            return None
        
        if self._callback_instance is not None:
            return self._callback_instance
        
        # Load and instantiate
        callback_class = self._load_callback_class()
        self._callback_instance = callback_class(**self.callback_kwargs)
        
        return self._callback_instance
```

**Why `get_or_create_callback()`?**
- ✅ One callback instance per process (singleton pattern)
- ✅ Callbacks can maintain state across hooks
- ✅ Same instance used for all initialization phases
- ✅ Each worker process gets its own instance

## Usage Examples

### Example 1: Modify Download Behavior

```python
class CustomDownloadCallback:
    """Force download on all nodes."""
    
    def on_pre_download(self, ctx: NodeInitializationContext) -> None:
        # Modify download parameters
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class=CustomDownloadCallback,
)
```

### Example 2: Track Metrics (Stateful)

```python
class MetricsCallback:
    """Track initialization timing."""
    
    def __init__(self, output_path: str = "/tmp/metrics.json"):
        self.output_path = output_path
        self.start_time = None
        self.metrics = {}
    
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> None:
        import time
        self.start_time = time.time()
    
    def on_post_download(self, ctx: NodeInitializationContext) -> None:
        import time
        self.metrics["download_time"] = time.time() - self.start_time
    
    def on_post_initialization(self, ctx: NodeInitializationContext) -> None:
        import time, json
        self.metrics["total_time"] = time.time() - self.start_time
        
        with open(self.output_path, "w") as f:
            json.dump(self.metrics, f)

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class=MetricsCallback,
    callback_kwargs={"output_path": "/tmp/my_metrics.json"}
)
```

### Example 3: Custom Placement Group

```python
class CustomPlacementGroupCallback:
    """Provide custom placement group."""
    
    def __init__(self, num_gpus: int = 4):
        self.num_gpus = num_gpus
    
    def on_pre_placement_group(self, ctx: NodeInitializationContext) -> bool:
        # Create custom placement group
        ctx.placement_group = ray.util.placement_group(
            bundles=[{"GPU": self.num_gpus}],
            strategy="STRICT_PACK"
        )
        
        # Return False to skip default placement group creation
        return False

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class=CustomPlacementGroupCallback,
    callback_kwargs={"num_gpus": 8}
)
```

### Example 4: GPU Optimizations

```python
class GPUOptimizationCallback:
    """Add GPU-specific engine parameters."""
    
    def on_pre_engine_init(self, ctx: NodeInitializationContext) -> None:
        # Add extra kwargs for engine initialization
        ctx.extra_init_kwargs.update({
            "gpu_memory_utilization": 0.95,
            "tensor_parallel_size": 4,
            "max_num_batched_tokens": 8192,
        })

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class=GPUOptimizationCallback,
)
```

### Example 5: Complete Custom Initialization

```python
class FullCustomInitializationCallback:
    """Replace entire initialization."""
    
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> bool:
        # Set up everything yourself
        ctx.placement_group = my_custom_pg()
        ctx.runtime_env = {"env_vars": {"CUSTOM": "true"}}
        ctx.extra_init_kwargs = {"custom_init": True}
        
        # Return False to skip all default behavior
        return False

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class=FullCustomInitializationCallback,
)
```

### Example 6: Load from String (for Deployment)

```python
# For YAML configs or deployments
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class="my_company.callbacks.ProductionCallback",
    callback_kwargs={
        "monitoring_endpoint": "https://monitoring.company.com",
        "team": "ml-platform"
    }
)
```

## Context Object Design

### Why Dataclass + custom_data?

The `NodeInitializationContext` is designed to be:

1. **Typed for common fields** (placement_group, runtime_env, etc.)
   - IDE autocomplete
   - Type checking
   - Clear API

2. **Extensible via custom_data** 
   - Callback-specific state
   - Forward compatibility
   - No field name conflicts

Example:
```python
class MyCallback:
    def on_pre_initialization(self, ctx: NodeInitializationContext) -> None:
        # Use typed fields
        ctx.extra_init_kwargs["my_param"] = "value"
        
        # Use custom_data for callback-specific state
        ctx.custom_data["my_callback_state"] = {
            "start_time": time.time(),
            "custom_metric": 123
        }
    
    def on_post_initialization(self, ctx: NodeInitializationContext) -> None:
        # Access state from earlier hook
        state = ctx.custom_data["my_callback_state"]
        elapsed = time.time() - state["start_time"]
```

### Future-Proof Design

When Ray adds new features, just add fields to the context:

```python
@dataclass
class NodeInitializationContext:
    # ... existing fields ...
    
    # NEW in Ray 2.x (doesn't break existing callbacks!)
    model_cache_dir: Optional[str] = None
    distributed_backend: Optional[str] = None
```

Existing callbacks continue to work because:
- ✅ New fields have defaults
- ✅ Callbacks only access fields they need
- ✅ Protocol methods are all optional

## Implementation in Ray

### Changes to ray.llm._internal.serve

**File:** `python/ray/llm/_internal/serve/configs/server_models.py`

```python
from typing import Optional, Union, Type, Dict, Any
from pydantic import Field, validator

class LLMConfig(BaseModelExtended):
    # ... existing fields ...
    
    callback_class: Optional[Union[str, Type]] = Field(
        default=None,
        description="Node initialization callback class (string path or type)",
    )
    
    callback_kwargs: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Keyword arguments for callback constructor",
    )
    
    _callback_instance: Optional[Any] = PrivateAttr(default=None)
    
    def get_or_create_callback(self):
        """Get or create callback instance for this process."""
        if self.callback_class is None:
            return None
        
        if self._callback_instance is not None:
            return self._callback_instance
        
        callback_class = self._load_callback_class()
        self._callback_instance = callback_class(**self.callback_kwargs)
        return self._callback_instance
    
    def _load_callback_class(self):
        # Implementation here (see llm_callback_design.py)
        ...
```

**File:** `python/ray/llm/_internal/serve/utils/node_initialization_callback.py` (NEW)

```python
"""Node initialization callback protocol and context."""

from typing import Optional, Protocol, Any, Dict, runtime_checkable
from dataclasses import dataclass, field

@dataclass
class NodeInitializationContext:
    """Context passed to all callback hooks."""
    # ... (see llm_callback_design.py)

@runtime_checkable
class NodeInitializationCallback(Protocol):
    """Protocol for node initialization callbacks."""
    # ... (see llm_callback_design.py)

__all__ = ["NodeInitializationContext", "NodeInitializationCallback"]
```

**File:** `python/ray/llm/_internal/serve/deployments/utils/node_initialization_utils.py`

```python
from ray.llm._internal.serve.utils.node_initialization_callback import (
    NodeInitializationContext,
    NodeInitializationCallback,
)

async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
    """Initialize node with optional callback hooks."""
    
    # Get callback (singleton per process)
    callback = llm_config.get_or_create_callback()
    
    # Create context
    ctx = NodeInitializationContext(llm_config=llm_config)
    
    # Invoke hooks at each phase
    # ... (see llm_callback_design.py)
    
    return _build_output_from_context(ctx)
```

## Testing

```python
# python/ray/llm/_internal/serve/tests/test_node_initialization_callback.py

def test_callback_modifies_download():
    class TestCallback:
        def on_pre_download(self, ctx):
            ctx.local_node_download_model = "CUSTOM"
    
    config = LLMConfig(
        model_id="test",
        callback_class=TestCallback
    )
    
    result = await initialize_node(config)
    # Verify custom value was used


def test_callback_singleton_per_process():
    config = LLMConfig(
        model_id="test",
        callback_class=TestCallback
    )
    
    cb1 = config.get_or_create_callback()
    cb2 = config.get_or_create_callback()
    
    assert cb1 is cb2  # Same instance


def test_callback_stateful():
    class StatefulCallback:
        def __init__(self):
            self.call_count = 0
        
        def on_pre_initialization(self, ctx):
            self.call_count += 1
        
        def on_post_initialization(self, ctx):
            ctx.custom_data["calls"] = self.call_count
    
    config = LLMConfig(
        model_id="test",
        callback_class=StatefulCallback
    )
    
    result = await initialize_node(config)
    callback = config.get_or_create_callback()
    assert callback.call_count > 0
```

## Comparison with Original PR

| Aspect | Original PR | This Design |
|--------|-------------|-------------|
| **API** | Abstract class with 1 method | Protocol with 8 hook methods |
| **Granularity** | Replace everything | Hook at specific points |
| **Context** | N/A | Typed `NodeInitializationContext` |
| **State** | Via `__init__` | Via `self` + `ctx.custom_data` |
| **Singleton** | Manual | Built-in via `get_or_create_callback()` |
| **Flexibility** | All or nothing | Incremental or complete |
| **Type Safety** | Low | High (typed context + protocol) |
| **Extensibility** | Low | High (add fields to context) |

## Benefits

### 1. Well-Defined Hooks
- ✅ Clear contract for each phase
- ✅ IDE autocomplete for hook names
- ✅ Type checking

### 2. Typed Context
- ✅ Autocomplete for fields
- ✅ Clear API
- ✅ Future-proof (add fields without breaking)

### 3. Per-Process Singleton
- ✅ One callback instance per process
- ✅ Callbacks can maintain state
- ✅ Consistent across all initialization phases

### 4. LLM-Specific
- ✅ Not overly generic
- ✅ Tailored to node initialization
- ✅ Clear use case

### 5. Flexible
- ✅ Override one thing (e.g., download params)
- ✅ Override everything (skip defaults)
- ✅ Track state across hooks
- ✅ Stateful callbacks via `self`

## Summary

This design provides:

- **Protocol class** with 8 well-defined hook methods
- **Typed context** (`NodeInitializationContext`) with typed fields + `custom_data`
- **Per-process singleton** via `get_or_create_callback()`
- **LLM-specific** for `ray.llm._internal.serve`

It's more structured than generic callbacks, more flexible than the original PR, and designed specifically for LLM node initialization needs.
