# Integration Guide: Adding Callbacks to Ray

## Overview

This guide shows the **exact changes** needed to integrate the simplified callback system into Ray's codebase.

## Changes Required

### 1. Add Generic Callback Infrastructure

**File:** `python/ray/_internal/callbacks.py` (NEW)

```python
"""Generic callback system for Ray.

This module provides a simple, reusable callback mechanism that can be used
throughout Ray for customization and extension points.
"""

from typing import Any, Dict, Optional, List, Union, Callable, Protocol, runtime_checkable
import importlib
import logging

logger = logging.getLogger(__name__)


@runtime_checkable
class Callback(Protocol):
    """Protocol for a generic callback.
    
    A callback receives:
        phase: str - execution phase identifier
        config: Any - configuration object for the operation
        data: Dict[str, Any] - shared mutable state
    
    Returns:
        Optional[Dict[str, Any]] - data to merge into shared state
    """
    
    def __call__(
        self,
        phase: str,
        config: Any,
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        ...


class CallbackRunner:
    """Executes callbacks at various phases with shared data."""
    
    def __init__(self, callbacks: List[Callable]):
        self.callbacks = callbacks
        self.data: Dict[str, Any] = {}
    
    async def run(self, phase: str, config: Any) -> Dict[str, Any]:
        """Run all callbacks for a given phase."""
        logger.debug(f"Running callbacks for phase: {phase}")
        
        for i, callback in enumerate(self.callbacks):
            try:
                result = callback(phase, config, self.data)
                if result:
                    self.data.update(result)
            except Exception as e:
                logger.error(
                    f"Callback {i} ({callback}) failed at phase '{phase}': {e}",
                    exc_info=True
                )
                raise RuntimeError(f"Callback failed at phase {phase}") from e
        
        return self.data
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from shared data."""
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set value in shared data."""
        self.data[key] = value


def load_callback(callback_spec: Union[str, Callable]) -> Callable:
    """Load a callback from string path or return callable directly.
    
    Args:
        callback_spec: Either a callable or a string like "module.path:function_name"
                      or "module.path.function_name"
    
    Returns:
        Callable callback function
    
    Raises:
        ImportError: If module cannot be imported
        AttributeError: If function not found in module
        TypeError: If the loaded object is not callable
    """
    if callable(callback_spec):
        return callback_spec
    
    # Parse string path
    if ":" in callback_spec:
        module_path, func_name = callback_spec.rsplit(":", 1)
    elif "." in callback_spec:
        module_path, func_name = callback_spec.rsplit(".", 1)
    else:
        raise ValueError(
            f"Callback path must be module.function or module:function, got: {callback_spec}"
        )
    
    try:
        module = importlib.import_module(module_path)
        callback = getattr(module, func_name)
        
        if not callable(callback):
            raise TypeError(
                f"Object '{callback_spec}' is not callable (type: {type(callback)})"
            )
        
        logger.info(f"Loaded callback: {callback_spec}")
        return callback
        
    except ImportError as e:
        raise ImportError(
            f"Failed to import module '{module_path}' for callback '{callback_spec}'. "
            f"Ensure the module is installed and in PYTHONPATH. Error: {e}"
        ) from e
    except AttributeError as e:
        available = [x for x in dir(module) if not x.startswith("_") and callable(getattr(module, x))]
        raise AttributeError(
            f"Module '{module_path}' has no callable '{func_name}'. "
            f"Available callables: {available}"
        ) from e


__all__ = ["Callback", "CallbackRunner", "load_callback"]
```

---

### 2. Update LLMConfig

**File:** `python/ray/llm/_internal/serve/configs/server_models.py`

Add this field to `LLMConfig`:

```python
from typing import List, Union, Callable
from pydantic import Field, validator

class LLMConfig(BaseModelExtended):
    # ... existing fields ...
    
    callbacks: Optional[List[Union[str, Callable]]] = Field(
        default_factory=list,
        description=(
            "List of callbacks to customize initialization behavior. "
            "Each callback receives (phase: str, config: LLMConfig, data: Dict) "
            "and can return a dict to merge into shared state. "
            "Can be function paths like 'my_module.my_callback' or callable objects."
        ),
    )
    
    @validator("callbacks", each_item=True)
    def validate_callback(cls, v):
        """Validate each callback is a string path or callable."""
        if v is None:
            return v
        
        if isinstance(v, str):
            if "." not in v and ":" not in v:
                raise ValueError(
                    f"Callback string must be a module path (module.func or module:func), got: {v}"
                )
            return v
        
        if callable(v):
            return v
        
        raise ValueError(
            f"Callback must be a string path or callable, got: {type(v)}"
        )
```

---

### 3. Update Node Initialization

**File:** `python/ray/llm/_internal/serve/deployments/utils/node_initialization_utils.py`

Replace the entire `initialize_node` function:

```python
from ray._internal.callbacks import CallbackRunner, load_callback
import logging

logger = logging.getLogger(__name__)


async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
    """Initialize a node with optional callback customization.
    
    Callbacks can customize behavior at various phases:
        - pre_initialization: Before any initialization
        - pre_download: Before model download (can modify download params)
        - post_download: After model download
        - post_placement_group: After placement group setup (can override)
        - pre_engine_init: Before engine initialization (can add kwargs)
        - post_engine_init: After engine initialization
        - post_initialization: After all initialization complete
        - custom_initialization: For full custom initialization (requires skip_default=True)
    
    Args:
        llm_config: LLM configuration with optional callbacks
        
    Returns:
        InitializeNodeOutput with placement group, runtime env, and extra kwargs
    """
    
    # Setup callbacks
    callbacks = [load_callback(cb) for cb in llm_config.callbacks]
    runner = CallbackRunner(callbacks)
    
    # Phase 1: Pre-initialization
    await runner.run("pre_initialization", llm_config)
    
    # Check if callbacks want to skip default initialization
    if runner.get("skip_default", False):
        logger.info("Callbacks requested custom initialization, skipping defaults")
        await runner.run("custom_initialization", llm_config)
        return _build_output_from_data(runner.data)
    
    # Phase 2: Setup download parameters
    # Callbacks can override via data dict
    local_node_download_model = runner.get(
        "local_node_download_model",
        NodeModelDownloadable.TOKENIZER_ONLY  # default
    )
    worker_node_download_model = runner.get(
        "worker_node_download_model",
        NodeModelDownloadable.MODEL_AND_TOKENIZER  # default
    )
    extra_init_kwargs = runner.get("extra_init_kwargs", {})
    
    await runner.run("pre_download", llm_config)
    
    # Phase 3: Model download
    logger.info(f"Downloading model: local={local_node_download_model}, worker={worker_node_download_model}")
    
    # Original download logic here...
    # (Keep existing implementation)
    download_result = await _download_model_internal(
        llm_config,
        local_node_download_model,
        worker_node_download_model,
    )
    
    runner.set("download_result", download_result)
    await runner.run("post_download", llm_config)
    
    # Phase 4: Placement group setup
    # Check if callback provided custom placement group
    placement_group = runner.get("placement_group")
    
    if not placement_group:
        # Original placement group logic
        # (Keep existing implementation)
        placement_group = await _setup_placement_group_internal(llm_config)
        runner.set("placement_group", placement_group)
    else:
        logger.info("Using custom placement group from callback")
    
    await runner.run("post_placement_group", llm_config)
    
    # Phase 5: Runtime environment
    runtime_env = runner.get("runtime_env")
    if not runtime_env:
        # Original runtime env logic
        # (Keep existing implementation)
        runtime_env = _build_runtime_env(llm_config)
        runner.set("runtime_env", runtime_env)
    
    # Phase 6: Engine initialization
    await runner.run("pre_engine_init", llm_config)
    
    # Merge any extra kwargs from callbacks
    extra_init_kwargs.update(runner.get("extra_init_kwargs", {}))
    runner.set("extra_init_kwargs", extra_init_kwargs)
    
    # Original engine init logic...
    # (Keep existing implementation)
    
    await runner.run("post_engine_init", llm_config)
    
    # Phase 7: Finalization
    await runner.run("post_initialization", llm_config)
    
    return _build_output_from_data(runner.data)


def _build_output_from_data(data: Dict[str, Any]) -> InitializeNodeOutput:
    """Build InitializeNodeOutput from callback data."""
    return InitializeNodeOutput(
        placement_group=data.get("placement_group"),
        runtime_env=data.get("runtime_env"),
        extra_init_kwargs=data.get("extra_init_kwargs", {}),
    )


# Keep all existing helper functions (_download_model_internal, etc.)
```

---

### 4. Documentation

**File:** `docs/source/llm/customization.md` (NEW)

```markdown
# Customizing LLM Initialization

Ray LLM provides a flexible callback system for customizing initialization behavior.

## Basic Usage

Add callbacks to your `LLMConfig`:

```python
from ray.llm import LLMConfig

def my_callback(phase: str, config: LLMConfig, data: dict):
    if phase == "pre_download":
        print(f"About to download {config.model_id}")

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=[my_callback]
)
```

## Phases

Callbacks are invoked at these phases:

- `pre_initialization` - Before any setup
- `pre_download` - Before downloading model
- `post_download` - After model download
- `post_placement_group` - After placement group setup
- `pre_engine_init` - Before engine initialization
- `post_engine_init` - After engine initialization  
- `post_initialization` - After all initialization

## Modifying Behavior

Callbacks can modify behavior by returning a dict:

```python
def custom_download(phase, config, data):
    if phase == "pre_download":
        # Force download on all nodes
        return {
            "local_node_download_model": "MODEL_AND_TOKENIZER",
            "worker_node_download_model": "MODEL_AND_TOKENIZER",
        }

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=[custom_download]
)
```

## Stateful Callbacks

Use classes for stateful callbacks:

```python
class MetricsCollector:
    def __init__(self):
        self.timings = {}
        self.start = None
    
    def __call__(self, phase, config, data):
        import time
        if phase == "pre_initialization":
            self.start = time.time()
        elif phase.startswith("post_"):
            self.timings[phase] = time.time() - self.start

metrics = MetricsCollector()
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=[metrics]
)
```

## Loading from String

For deployments, use string paths:

```python
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=["my_company.callbacks.custom_init"]
)
```

## Complete Replacement

Replace entire initialization:

```python
def custom_init(phase, config, data):
    if phase == "pre_initialization":
        return {"skip_default": True}
    
    if phase == "custom_initialization":
        # Your custom initialization here
        return {
            "placement_group": my_pg,
            "runtime_env": my_env,
        }

config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callbacks=[custom_init]
)
```
```

---

### 5. Tests

**File:** `python/ray/llm/_internal/serve/tests/test_callbacks.py` (NEW)

```python
import pytest
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    initialize_node
)


def test_callback_invoked():
    """Test that callbacks are invoked during initialization."""
    phases_seen = []
    
    def tracking_callback(phase, config, data):
        phases_seen.append(phase)
    
    config = LLMConfig(
        model_id="test-model",
        callbacks=[tracking_callback]
    )
    
    await initialize_node(config)
    
    assert "pre_initialization" in phases_seen
    assert "post_initialization" in phases_seen


def test_callback_modifies_data():
    """Test that callbacks can modify initialization behavior."""
    
    def custom_download(phase, config, data):
        if phase == "pre_download":
            return {"local_node_download_model": "CUSTOM_VALUE"}
    
    config = LLMConfig(
        model_id="test-model",
        callbacks=[custom_download]
    )
    
    result = await initialize_node(config)
    # Verify the custom value was used
    # (Add specific assertion based on your implementation)


def test_stateful_callback():
    """Test that stateful callbacks work correctly."""
    
    class Counter:
        def __init__(self):
            self.count = 0
        
        def __call__(self, phase, config, data):
            self.count += 1
    
    counter = Counter()
    config = LLMConfig(
        model_id="test-model",
        callbacks=[counter]
    )
    
    await initialize_node(config)
    
    assert counter.count > 0  # Called multiple times


def test_custom_initialization():
    """Test complete initialization replacement."""
    
    def custom_init(phase, config, data):
        if phase == "pre_initialization":
            return {"skip_default": True}
        if phase == "custom_initialization":
            return {
                "placement_group": "custom_pg",
                "runtime_env": {},
            }
    
    config = LLMConfig(
        model_id="test-model",
        callbacks=[custom_init]
    )
    
    result = await initialize_node(config)
    
    assert result.placement_group == "custom_pg"


def test_callback_from_string():
    """Test loading callbacks from string paths."""
    
    config = LLMConfig(
        model_id="test-model",
        callbacks=["ray.llm.tests.fixtures.test_callback"]
    )
    
    # Should not raise
    result = await initialize_node(config)
```

---

## Migration from Current PR

If you've already implemented the CustomInitialization approach, here's how to migrate:

```python
# Old approach (from the PR):
class MyInit(CustomInitialization):
    async def initialize(self, llm_config):
        # ... 100 lines of code ...
        return InitializeNodeOutput(...)

config = LLMConfig(
    initialization_class=MyInit,
    initialization_kwargs={}
)

# New approach (callbacks):
def my_init(phase, config, data):
    if phase == "pre_initialization":
        return {"skip_default": True}
    if phase == "custom_initialization":
        # ... your custom logic ...
        return {
            "placement_group": pg,
            "runtime_env": env,
        }

config = LLMConfig(
    callbacks=[my_init]
)
```

Benefits:
- Less code
- More flexible (can customize incrementally)
- Reusable pattern across Ray
