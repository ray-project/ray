# Node Initialization Callback System - Final Design

## TL;DR

A callback system for `ray.llm._internal.serve` with:

✅ **Protocol class** with 8 well-defined hook methods  
✅ **Typed context object** (`NodeInitializationContext`) that's future-proof  
✅ **Per-process singleton** via `get_or_create_callback()`  
✅ **LLM-specific** - tailored to node initialization, not overly generic  

## Quick Example

```python
class MyCallback:
    """Customize LLM node initialization."""
    
    def on_pre_download(self, ctx: NodeInitializationContext) -> None:
        """Modify download behavior."""
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
    
    def on_pre_engine_init(self, ctx: NodeInitializationContext) -> None:
        """Add GPU optimizations."""
        ctx.extra_init_kwargs.update({
            "gpu_memory_utilization": 0.95,
            "tensor_parallel_size": 4,
        })

# Usage
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback_class=MyCallback,
)
```

## Files in This Directory

### 📘 Core Documents

1. **`LLM_CALLBACK_DESIGN.md`** - **START HERE!**
   - Complete design specification
   - All usage examples
   - Integration guide
   - Testing approach

2. **`llm_callback_design.py`** - **Working implementation** (tested ✅)
   - Full implementation with examples
   - Run `python3 llm_callback_design.py` to see it work!

3. **`DESIGN_EVOLUTION.md`** - The journey
   - Original PR review
   - Why generic callback was too complex
   - How we arrived at this design

## The Design

### Two Core Concepts

#### 1. NodeInitializationContext (Typed Hook Context)

```python
@dataclass
class NodeInitializationContext:
    """Typed data container passed to all hooks."""
    
    # Read-only
    llm_config: LLMConfig
    
    # Read/write - callbacks can modify these
    local_node_download_model: Optional[str]
    worker_node_download_model: Optional[str]
    download_result: Optional[Any]
    placement_group: Optional[Any]
    runtime_env: Optional[Dict]
    extra_init_kwargs: Dict[str, Any]
    
    # For callback-specific state
    custom_data: Dict[str, Any]
```

**Benefits:**
- ✅ Typed fields with IDE autocomplete
- ✅ Clear API - know exactly what's available
- ✅ Future-proof - add new fields without breaking existing callbacks
- ✅ `custom_data` for callback-specific state

#### 2. NodeInitializationCallback (Protocol)

```python
class NodeInitializationCallback(Protocol):
    """Well-defined hooks - all optional."""
    
    def on_pre_initialization(self, ctx) -> Optional[bool]: ...
    def on_pre_download(self, ctx) -> Optional[bool]: ...
    def on_post_download(self, ctx) -> None: ...
    def on_pre_placement_group(self, ctx) -> Optional[bool]: ...
    def on_post_placement_group(self, ctx) -> None: ...
    def on_pre_engine_init(self, ctx) -> None: ...
    def on_post_engine_init(self, ctx) -> None: ...
    def on_post_initialization(self, ctx) -> None: ...
```

**Benefits:**
- ✅ Well-defined hooks (not arbitrary strings)
- ✅ IDE autocomplete for method names
- ✅ Type checking
- ✅ Clear when each hook is called

### Integration with LLMConfig

```python
class LLMConfig:
    def __init__(
        self,
        model_id: str,
        callback_class: Optional[Union[str, Type]] = None,
        callback_kwargs: Optional[Dict] = None,
    ):
        self.callback_class = callback_class
        self.callback_kwargs = callback_kwargs or {}
        self._callback_instance = None  # Singleton per process
    
    def get_or_create_callback(self):
        """Returns callback instance (singleton per process)."""
        if self._callback_instance is None:
            callback_class = self._load_callback_class()
            self._callback_instance = callback_class(**self.callback_kwargs)
        return self._callback_instance
```

## Common Use Cases

### 1. Modify Download Behavior

```python
class ForceDownloadEverywhere:
    def on_pre_download(self, ctx):
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"
```

### 2. Track Metrics (Stateful)

```python
class MetricsTracker:
    def __init__(self, output_path="/tmp/metrics.json"):
        self.output_path = output_path
        self.start = None
        self.metrics = {}
    
    def on_pre_initialization(self, ctx):
        self.start = time.time()
    
    def on_post_download(self, ctx):
        self.metrics["download_time"] = time.time() - self.start
    
    def on_post_initialization(self, ctx):
        with open(self.output_path, "w") as f:
            json.dump(self.metrics, f)
```

### 3. Custom Placement Group

```python
class CustomPG:
    def __init__(self, num_gpus=8):
        self.num_gpus = num_gpus
    
    def on_pre_placement_group(self, ctx):
        ctx.placement_group = ray.util.placement_group(
            bundles=[{"GPU": self.num_gpus}],
            strategy="STRICT_PACK"
        )
        return False  # Skip default PG creation
```

### 4. GPU Optimizations

```python
class GPUOptimizer:
    def on_pre_engine_init(self, ctx):
        ctx.extra_init_kwargs.update({
            "gpu_memory_utilization": 0.95,
            "tensor_parallel_size": 4,
            "max_num_batched_tokens": 8192,
        })
```

### 5. Complete Custom Initialization

```python
class FullCustom:
    def on_pre_initialization(self, ctx):
        # Provide everything yourself
        ctx.placement_group = my_custom_pg()
        ctx.runtime_env = {"env_vars": {"CUSTOM": "true"}}
        ctx.extra_init_kwargs = {"custom": True}
        
        return False  # Skip all default initialization
```

### 6. Load from String (Deployment Config)

```python
config = LLMConfig(
    model_id="llama-2-7b",
    callback_class="my_company.callbacks.ProductionCallback",
    callback_kwargs={"monitoring_url": "https://..."}
)
```

## Why This Design?

### Addresses Your Requirements

1. ✅ **"Protocol class with well-defined hooks"**
   - 8 concrete methods, not arbitrary strings

2. ✅ **"Hook-ctx object that is flexible typed data"**
   - `NodeInitializationContext` with typed fields + `custom_data`

3. ✅ **"get_or_create_callback() per process"**
   - Built-in singleton pattern

4. ✅ **"Callback system for ray.llm._internal.serve"**
   - Not overly generic, tailored to LLM serving

5. ✅ **"Least number of new concepts"**
   - Just two: Protocol + Context

6. ✅ **"Generic and flexible primitives"**
   - BUT focused on the specific use case

### Better Than Original PR

| Aspect | Original PR | This Design |
|--------|-------------|-------------|
| Granular control | ❌ All or nothing | ✅ Hook at 8 points |
| Access intermediate state | ❌ No | ✅ Via context |
| Type safety | ⭐ | ⭐⭐⭐ |
| IDE support | ⭐ | ⭐⭐⭐ |
| Flexibility | ⭐ | ⭐⭐⭐ |
| Must reimplement all | ✅ Yes | ❌ No |

### Better Than Generic Callback

| Aspect | Generic Callback | This Design |
|--------|------------------|-------------|
| Hook names | Strings (no autocomplete) | Methods (typed) |
| Context | Untyped dict | Typed dataclass |
| Scope | All of Ray | LLM-specific |
| Complexity | High (too generic) | Medium (focused) |

## Integration Checklist

To add this to Ray:

- [ ] Create `python/ray/llm/_internal/serve/utils/node_initialization_callback.py`
  - Add `NodeInitializationContext` dataclass
  - Add `NodeInitializationCallback` protocol
  
- [ ] Update `python/ray/llm/_internal/serve/configs/server_models.py`
  - Add `callback_class` field to `LLMConfig`
  - Add `callback_kwargs` field
  - Add `get_or_create_callback()` method
  
- [ ] Update `python/ray/llm/_internal/serve/deployments/utils/node_initialization_utils.py`
  - Update `initialize_node()` to invoke callbacks
  - Create `NodeInitializationContext` at start
  - Call hooks at appropriate phases
  
- [ ] Add tests in `python/ray/llm/_internal/serve/tests/test_node_initialization_callback.py`
  - Test hook invocation
  - Test context modifications
  - Test singleton behavior
  - Test stateful callbacks
  
- [ ] Add documentation with examples

**Estimated effort:** ~2-3 hours for implementation + tests

## Testing

Run the working implementation:

```bash
python3 llm_callback_design.py
```

You'll see 5 examples:
1. Custom download behavior
2. Metrics tracking (stateful)
3. Custom placement group
4. Full custom initialization
5. GPU optimizations

## Next Steps

1. ✅ Review the design (you're here!)
2. ✅ Run `python3 llm_callback_design.py` to see it work
3. Read `LLM_CALLBACK_DESIGN.md` for full details
4. Read `DESIGN_EVOLUTION.md` to understand the journey
5. Integrate into Ray codebase
6. Add tests
7. Ship it! 🚀

## Questions?

- **"Why Protocol instead of ABC?"**
  - Duck typing: any class with these methods works
  - All methods are optional
  - Better for testing (no inheritance required)

- **"Why dataclass for context?"**
  - Typed fields with defaults
  - Immutable by default (but fields can be modified)
  - Clear API with autocomplete
  - Easy to extend (add fields without breaking)

- **"Why per-process singleton?"**
  - Callbacks can maintain state across hooks
  - Consistent behavior within a process
  - Explicit with `get_or_create_callback()`

- **"Can I skip default initialization entirely?"**
  - Yes! Return `False` from `on_pre_initialization()`
  - Then provide everything in the context
  - See "Full Custom Initialization" example

- **"How do I share state between hooks?"**
  - Use `self` for callback instance state
  - Use `ctx.custom_data` for hook-specific data
  - Both work great!

## Summary

This design provides a **focused, typed, flexible** callback system for Ray LLM node initialization:

- ✅ **Protocol** with 8 well-defined hooks
- ✅ **Typed context** with fields + extensibility
- ✅ **Per-process singleton** via `get_or_create_callback()`
- ✅ **LLM-specific** - not overly generic
- ✅ **Working implementation** - tested and ready!

Read `LLM_CALLBACK_DESIGN.md` for complete details and run `llm_callback_design.py` to see it in action! 🎉
