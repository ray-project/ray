# Quick Start - RayServeLLMCallback

## 1. Create Your Callback

```python
class MyCallback:
    """Implement only the hooks you need."""
    
    def on_before_download(self, ctx: CallbackCtx):
        # Modify download behavior
        ctx.local_node_download_model = "MODEL_AND_TOKENIZER"
```

## 2. Configure It

```python
config = LLMConfig(
    model_id="meta-llama/Llama-2-7b",
    callback="my_module.MyCallback"
)
```

## 3. Done!

The callback runs automatically during `initialize_node(config)`.

---

## The 3 Hooks (All Optional)

| Hook | When Called | Use For |
|------|-------------|---------|
| `on_before_init(ctx)` | Start of initialization | Setup, modify params, skip defaults |
| `on_before_download(ctx)` | Before model download | Modify download params |
| `on_after_init(ctx)` | After everything | Cleanup, logging, metrics |

## The Context Object

```python
ctx.llm_config                    # LLMConfig (read-only)
ctx.local_node_download_model     # Modify download behavior
ctx.worker_node_download_model    # Modify download behavior
ctx.placement_group               # Override placement group
ctx.extra_init_kwargs             # Add engine parameters
ctx.custom_data                   # Your state
```

## Common Patterns

### Modify One Thing
```python
class MyCallback:
    def on_before_download(self, ctx):
        ctx.worker_node_download_model = "MODEL_AND_TOKENIZER"
```

### Track State
```python
class MyCallback:
    def __init__(self):
        self.start = None
    
    def on_before_init(self, ctx):
        self.start = time.time()
    
    def on_after_init(self, ctx):
        print(f"Took {time.time() - self.start}s")
```

### Skip Defaults
```python
class MyCallback:
    def on_before_init(self, ctx):
        ctx.placement_group = my_custom_pg()
        return False  # Skip all defaults
```

## Test the Examples

```bash
python3 ray_serve_llm_callback.py
```

## Read More

- **README.md** - Overview and design principles
- **FINAL_DESIGN.md** - Complete specification and integration guide
- **ray_serve_llm_callback.py** - Working implementation

That's it! 🚀
