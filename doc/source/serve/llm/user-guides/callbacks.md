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
| `on_before_node_init` | Before the default model download begins | Head node (replica process) | Download models from custom storage, set up dependencies, skip default init via `self.ctx.run_init_node = False` |
| `on_after_node_init` | After the default model download completes | Head node (replica process) | Register reasoning parsers, custom tokenizers, or plugins |
| `on_before_download_model_files_distributed` | Before each worker node downloads its model files | Each distributed worker node | Download auxiliary files (torch compile caches, configs) from cloud storage |

## Example: Registering a custom reasoning parser

Some models (e.g., Nemotron) require custom reasoning parsers that are not bundled with vLLM. Use a callback to register the parser after node initialization:

```{literalinclude} ../../../llm/doc_code/serve/callbacks/reasoning_parser_example.py
:language: python
:start-after: __reasoning_parser_example_start__
:end-before: __reasoning_parser_example_end__
```

:::{note}
Because vLLM uses spawn multiprocessing, parser registrations in the parent process don't transfer to spawned worker processes. The `on_after_node_init` callback runs inside the replica process, ensuring the parser is available where it's needed.

For vLLM's spawned worker processes, you may also need to provide a file-based plugin via `runtime_env` with `working_dir`. See the [vLLM plugin documentation](https://docs.vllm.ai/en/latest/design/plugin_system.html) for details.
:::

## Example: Bringing custom models outside the vLLM tree

When serving a model that requires custom download logic or is not available through standard channels, use a callback to handle the download and setup yourself:

```{literalinclude} ../../../llm/doc_code/serve/callbacks/custom_model_example.py
:language: python
:start-after: __custom_model_example_start__
:end-before: __custom_model_example_end__
```

Setting `self.ctx.run_init_node = False` in `on_before_node_init` tells the system to skip the default model download, giving you full control over the process.

## Callback context

The `CallbackCtx` object (`self.ctx`) is shared across all hooks in a single callback instance. It provides:

| Field | Description |
|---|---|
| `run_init_node` | Set to `False` to skip the default model download during initialization. |
| `custom_data` | A dictionary for storing and sharing state across hooks. |
| `worker_node_download_model` | Controls how models are downloaded on worker nodes. |
| `placement_group` | The Ray placement group for resource allocation. |
| `runtime_env` | The runtime environment configuration. |

## Using `CallbackConfig`

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
    accelerator_type="A10G",
    callback_config=CallbackConfig(
        callback_class=MyCallback,
        callback_kwargs={"my_param": "value"},
        raise_error_on_callback=True,
    ),
)
```

You can also pass `callback_config` as a dictionary, which is useful for YAML configurations:

```python
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-model",
        model_source="org/model-name",
    ),
    accelerator_type="A10G",
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
          accelerator_type: A10G
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

`CloudDownloader` is a built-in callback that downloads files from cloud storage (S3, GCS, Azure Blob) before model initialization. This is useful for downloading torch compile caches, custom tokenizers, or other auxiliary files.

Supported URI schemes: `s3://`, `gs://`, `abfss://`, `azure://`.

```python
from ray.serve.llm import LLMConfig
from ray.serve.llm.callbacks import CloudDownloader

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-model",
        model_source="org/model-name",
    ),
    accelerator_type="A10G",
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
