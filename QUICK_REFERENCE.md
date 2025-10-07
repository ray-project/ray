# Quick Reference: LLM Node Initialization Callbacks

## The Design in 30 Seconds

```python
# 1. Define your callback with hooks you need
class MyCallback:
    def on_pre_download(self, ctx: NodeInitializationContext):
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"
    
    def on_pre_engine_init(self, ctx: NodeInitializationContext):
        ctx.extra_init_kwargs["gpu_memory_utilization"] = 0.95

# 2. Configure it
config = LLMConfig(
    model_id="llama-2-7b",
    callback_class=MyCallback
)

# 3. That's it! Callback runs automatically during initialization
```

## Hook Methods (All Optional)

| Method | When | Return False To |
|--------|------|-----------------|
| `on_pre_initialization(ctx)` | Before anything | Skip all defaults |
| `on_pre_download(ctx)` | Before download | Skip download |
| `on_post_download(ctx)` | After download | - |
| `on_pre_placement_group(ctx)` | Before PG setup | Skip PG creation |
| `on_post_placement_group(ctx)` | After PG setup | - |
| `on_pre_engine_init(ctx)` | Before engine | - |
| `on_post_engine_init(ctx)` | After engine | - |
| `on_post_initialization(ctx)` | After everything | - |

## Context Fields

```python
ctx.llm_config                    # LLMConfig (read-only)
ctx.local_node_download_model     # str (read/write)
ctx.worker_node_download_model    # str (read/write)
ctx.download_result              # Any (set by system)
ctx.placement_group              # Any (read/write)
ctx.runtime_env                  # Dict (read/write)
ctx.extra_init_kwargs            # Dict (read/write)
ctx.custom_data                  # Dict (your state)
```

## Common Patterns

### Pattern 1: Modify One Thing
```python
class ChangeDownload:
    def on_pre_download(self, ctx):
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
```

### Pattern 2: Track Metrics
```python
class Metrics:
    def __init__(self):
        self.start = None
    
    def on_pre_initialization(self, ctx):
        self.start = time.time()
    
    def on_post_initialization(self, ctx):
        print(f"Took {time.time() - self.start}s")
```

### Pattern 3: Custom Placement Group
```python
class CustomPG:
    def on_pre_placement_group(self, ctx):
        ctx.placement_group = my_custom_pg()
        return False  # Skip default
```

### Pattern 4: Add Engine Parameters
```python
class GPUOpts:
    def on_pre_engine_init(self, ctx):
        ctx.extra_init_kwargs.update({
            "gpu_memory_utilization": 0.95,
            "tensor_parallel_size": 4,
        })
```

### Pattern 5: Replace Everything
```python
class FullCustom:
    def on_pre_initialization(self, ctx):
        ctx.placement_group = my_pg()
        ctx.runtime_env = my_env()
        ctx.extra_init_kwargs = my_kwargs()
        return False  # Skip all defaults
```

## Configuration

### Direct Class
```python
config = LLMConfig(
    model_id="llama-2-7b",
    callback_class=MyCallback,
    callback_kwargs={"param": "value"}
)
```

### String Path (for YAML configs)
```python
config = LLMConfig(
    model_id="llama-2-7b",
    callback_class="my_module.MyCallback",
    callback_kwargs={"param": "value"}
)
```

## Key Features

✅ **Typed context** - IDE autocomplete  
✅ **Well-defined hooks** - Know exactly when each runs  
✅ **Singleton per process** - State persists across hooks  
✅ **All optional** - Only implement hooks you need  
✅ **Flexible** - Modify one thing OR replace everything  

## Files to Read

1. **`README.md`** - Overview and examples
2. **`LLM_CALLBACK_DESIGN.md`** - Full specification
3. **`llm_callback_design.py`** - Working code (run it!)
4. **`DESIGN_EVOLUTION.md`** - Why this design

## Test It

```bash
python3 llm_callback_design.py
```

Runs 5 examples showing different patterns!

## That's It!

Simple, typed, flexible callback system for Ray LLM node initialization. 🚀
