# RayServeLLMCallback - Lean Callback System

**A focused, production-ready callback system for Ray Serve LLM node initialization.**

## TL;DR

```python
# 1. Implement callback with hooks you need
class MyCallback:
    def on_before_download(self, ctx: CallbackCtx):
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"

# 2. Configure it
config = LLMConfig(
    model_id="llama-2-7b",
    callback="my_module.MyCallback"
)

# 3. Done! Callback runs automatically during initialization
```

## Design Principles

✅ **Start Lean** - Only 3 hooks needed for the original PR use case  
✅ **Stay Future-Proof** - Easy to add more hooks/fields over time  
✅ **Clear Naming** - `RayServeLLMCallback` and `CallbackCtx`  
✅ **Production-Ready** - Tested, documented, ready to integrate  

## The Two Core Concepts

### 1. CallbackCtx (Centralized State)

```python
@dataclass
class CallbackCtx:
    """Mutable context passed to all hooks."""
    
    llm_config: Any                           # Read-only
    local_node_download_model: Optional[str]  # Read/write
    worker_node_download_model: Optional[str] # Read/write
    placement_group: Optional[Any]            # Read/write
    extra_init_kwargs: Dict[str, Any]         # Read/write
    custom_data: Dict[str, Any]               # Your state
```

### 2. RayServeLLMCallback (Protocol)

```python
class RayServeLLMCallback(Protocol):
    """3 hooks - all optional."""
    
    def on_before_init(self, ctx: CallbackCtx) -> Optional[bool]:
        """Called at start. Return False to skip defaults."""
        ...
    
    def on_before_download(self, ctx: CallbackCtx) -> Optional[bool]:
        """Called before download. Return False to skip."""
        ...
    
    def on_after_init(self, ctx: CallbackCtx) -> None:
        """Called after everything complete."""
        ...
```

## Quick Examples

### Custom Download
```python
class CustomDownload:
    def on_before_download(self, ctx):
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"
```

### Track Metrics
```python
class Metrics:
    def __init__(self):
        self.start = None
    
    def on_before_init(self, ctx):
        self.start = time.time()
    
    def on_after_init(self, ctx):
        print(f"Took {time.time() - self.start}s")
```

### Custom Placement Group
```python
class CustomPG:
    def on_before_init(self, ctx):
        ctx.placement_group = my_custom_pg()
```

## Files

📘 **`FINAL_DESIGN.md`** - Complete specification, usage, integration guide  
💻 **`ray_serve_llm_callback.py`** - Working implementation (tested!)  
📖 **`DESIGN_EVOLUTION.md`** - The journey from generic → focused design  

## Test It

```bash
python3 ray_serve_llm_callback.py
```

Runs 4 examples showing different use cases!

## Why This Design?

### Addresses Your Requirements

✅ **"Protocol class with well-defined hooks"** → 3 typed methods  
✅ **"Hook-ctx object that is flexible typed data"** → `CallbackCtx` dataclass  
✅ **"get_or_create_callback() per process"** → Built-in singleton  
✅ **"For ray.llm._internal.serve not entire ray"** → Focused on LLM serving  
✅ **"Least number of concepts"** → Just 2: Protocol + Context  
✅ **"Start lean, add hooks over time"** → Only 3 hooks now, easy to add more  

### Better Than Original PR

| Aspect | Original PR | This Design |
|--------|-------------|-------------|
| **Granularity** | All or nothing | 3 hook points + extensible |
| **Access state** | ❌ No | ✅ Via `CallbackCtx` |
| **Type safety** | ⭐ | ⭐⭐⭐ |
| **Extensibility** | Low | High (add hooks/fields) |
| **Must rewrite all** | ✅ | ❌ |

## Integration Summary

**3 files to update:**

1. Create `python/ray/llm/_internal/serve/callbacks/base.py`
   - Add `CallbackCtx` dataclass
   - Add `RayServeLLMCallback` protocol

2. Update `python/ray/llm/_internal/serve/configs/server_models.py`
   - Add `callback` and `callback_kwargs` fields
   - Add `get_or_create_callback()` method

3. Update `python/ray/llm/_internal/serve/deployments/utils/node_initialization_utils.py`
   - Create `CallbackCtx` at start
   - Invoke hooks at appropriate phases

**Estimated effort:** 2-3 hours

## Future Extensibility

### Add More Hooks (Easy!)

```python
class RayServeLLMCallback(Protocol):
    # Existing
    def on_before_init(self, ctx): ...
    def on_before_download(self, ctx): ...
    def on_after_init(self, ctx): ...
    
    # NEW - add when needed
    def on_before_placement_group(self, ctx): ...  # ← Just add methods!
    def on_before_engine_init(self, ctx): ...
```

✅ Backward compatible - existing callbacks keep working!

### Add More Context Fields (Easy!)

```python
@dataclass
class CallbackCtx:
    # Existing
    llm_config: Any
    local_node_download_model: Optional[str] = None
    
    # NEW - add when needed
    runtime_env: Optional[Dict] = None  # ← Just add fields!
    engine_config: Optional[Any] = None
```

✅ Backward compatible - fields have defaults!

## Summary

A **lean, focused** callback system:

- 🎯 **3 hooks** covering the original PR use case
- 📦 **Typed context** with essential fields
- 🔄 **Per-process singleton** for stateful callbacks
- 🚀 **Future-proof** - easy to extend without breaking changes
- ✅ **Production-ready** - tested and documented

Read **`FINAL_DESIGN.md`** for complete details!
