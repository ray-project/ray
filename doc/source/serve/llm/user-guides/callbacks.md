(callbacks-guide)=
# Callbacks

Customize LLM deployment initialization with callbacks to register custom components, download models from custom storage, or integrate models outside the standard vLLM tree.

Ray Serve LLM provides a callback system that lets you inject custom logic at key points during deployment initialization. This is useful when the default initialization behavior does not cover your use case, such as:

- **Registering custom reasoning parsers** for models that require non-standard output parsing.
- **Downloading models from custom storage** backends not supported out of the box.
- **Integrating custom models** that are not in the vLLM model tree.
- **Setting up custom runtime environments** or dependencies before the engine starts.

## Callback hooks

Callbacks are subclasses of `CallbackBase` that override one or more lifecycle hooks. The hooks fire in the order listed below during deployment startup.

| Hook | When | Where | Common use cases |
|---|---|---|---|
| `on_before_node_init` | Before the default model download begins | Serve replica process | Download models from custom storage, set up dependencies, skip default init via `self.ctx.run_init_node = False` |
| `on_after_node_init` | After the default model download completes | Serve replica process | Register reasoning parsers, custom tokenizers, or plugins |
| `on_before_download_model_files_distributed` | Before each node downloads its model files | Remote task on each node in the placement group | Download auxiliary files (torch compile caches, configs) from cloud storage |

## Example: Loading a reasoning parser

Reasoning models require parsers to extract thinking tokens from the output. Use a callback to ensure the parser is loaded inside the Serve replica process before the engine starts:

```{literalinclude} ../../../llm/doc_code/serve/callbacks/reasoning_parser_example.py
:language: python
:start-after: __reasoning_parser_example_start__
:end-before: __reasoning_parser_example_end__
```

:::{note}
Because vLLM uses spawn multiprocessing, lazy parser registrations in the parent process don't transfer to spawned worker processes. The `on_after_node_init` callback runs inside the Serve replica process, ensuring the parser is available where it's needed.
:::

## Example: Bringing custom models outside the vLLM tree

When serving a model that requires custom download logic or is not available through standard channels, use a callback to handle the download and setup yourself:

```{literalinclude} ../../../llm/doc_code/serve/callbacks/custom_model_example.py
:language: python
:start-after: __custom_model_example_start__
:end-before: __custom_model_example_end__
```

## Callback configuration

### Python configuration

Attach callbacks to an `LLMConfig` using `CallbackConfig`:

```python
from ray.serve.llm import LLMConfig
from ray.serve.llm.callbacks import CallbackBase, CallbackConfig

class MyCallback(CallbackBase):
    async def on_before_node_init(self) -> None:
        print(f"Custom param: {self.kwargs['my_param']}")

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-model",
        model_source="org/model-name",
    ),
    callback_config=CallbackConfig(
        callback_class=MyCallback,
        callback_kwargs={"my_param": "value"},
        raise_error_on_callback=True,
    ),
)
```

You can also pass `callback_config` as a dictionary:

```python
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-model",
        model_source="org/model-name",
    ),
    callback_config={
        "callback_class": "my_module.MyCallback",
        "callback_kwargs": {"my_param": "value"},
    },
)
```

### YAML configuration

```yaml
applications:
- args:
    llm_configs:
        - model_loading_config:
            model_id: my-model
            model_source: org/model-name
          callback_config:
            callback_class: my_module.MyCallback
            callback_kwargs:
              my_param: value
  import_path: ray.serve.llm:build_openai_app
  name: llm_app
  route_prefix: "/"
```

## Built-in callbacks

### CloudDownloader

`CloudDownloader` is a built-in callback that downloads files from cloud storage (S3, GCS, Azure Blob) before model initialization. This is useful for downloading torch compile caches, custom tokenizers, or other auxiliary files. Supported URI schemes: `s3://`, `gs://`, `abfss://`, `azure://`.

#### Example: Caching torch compile artifacts

Torch compile incurs latency during initialization. You can eliminate this on subsequent startups by caching the compile artifacts:

1. Run vLLM once and find the cache directory in the logs:

```
(RayWorkerWrapper pid=126782) INFO 10-15 11:57:04 [backends.py:608] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/131ee5c6d9/rank_1_0/backbone for vLLM's torch.compile
```

2. Upload the cache folder (e.g., `/home/ray/.cache/vllm/torch_compile_cache/131ee5c6d9`) to your cloud storage bucket.

3. Use `CloudDownloader` to retrieve the cache at startup:

```python
from ray.serve.llm import LLMConfig
from ray.serve.llm.callbacks import CloudDownloader

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-model",
        model_source="org/model-name",
    ),
    callback_config={
        "callback_class": CloudDownloader,
        "callback_kwargs": {
            "paths": [
                ("s3://my-bucket/compile-cache", "/home/ray/.cache/vllm/torch_compile_cache/my-cache"),
            ]
        },
    },
    engine_kwargs={
        "compilation_config": {
            "cache_dir": "/home/ray/.cache/vllm/torch_compile_cache/my-cache",
        }
    },
)
```

Other storage options (distributed filesystem, block storage) also work as long as `compilation_config.cache_dir` points to the correct local path.

## Error handling

By default, callback errors cause the deployment to fail (`raise_error_on_callback=True`). To log errors without failing:

```python
callback_config=CallbackConfig(
    callback_class=MyCallback,
    raise_error_on_callback=False,
)
```

## See also

- {doc}`deployment-initialization` - Deployment initialization overview including model loading from Hugging Face and remote storage
