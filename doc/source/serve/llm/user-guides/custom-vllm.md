(custom-vllm-guide)=
# Custom vLLM models

This page provides an example of serving a custom vLLM model with Ray Serve LLM.

The example model is a custom reward model composed of Qwen3-0.6B with its vocabulary LM head replaced by a scalar reward head. The model reuses vLLM's Qwen3 backbone, scores the last token, and returns the reward through vLLM's `/classify` endpoint.

## How does Ray Serve LLM support custom vLLM models?

Ray Serve LLM serves any architecture that vLLM supports. When your model isn't a [built-in vLLM architecture](https://docs.vllm.ai/en/stable/models/supported_models.html), you add it with a [vLLM plugin](https://docs.vllm.ai/en/stable/design/plugin_system.html): a custom package that registers the architecture in vLLM's model registry without patching vLLM. Ray Serve LLM then serves it through the same OpenAI-compatible API as any other model.


## Prerequisites

Install Ray with the LLM extra:

```
pip install "ray[llm]"
```

## Write the plugin

Compose the plugin as an installable Python package. The following is an example layout:

```text
qwen3-reward-plugin/          # project directory -- run `pip install .` here
├── setup.py                  # packaging metadata and the vLLM entry point
└── qwen3_reward_plugin/      # the importable package
    ├── __init__.py           # register(): adds the architecture to vLLM's model registry
    ├── qwen3_rm.py           # Qwen3CustomRewardModel: the model class
    └── serve_hook.py         # RewardModelServer: registers the plugin in the Ray Serve LLM replica
```

### Model class

Subclass vLLM's `Qwen3ForCausalLM` to reuse its backbone, then attach the reward head and a last-token classification pooler. See the full model class [here](https://github.com/ray-project/ray/tree/master/doc/source/llm/doc_code/serve/custom_vllm/qwen3_reward_plugin/qwen3_rm.py).

### Register the architecture

vLLM calls `register()` in every process it starts, the driver, the engine core, and each rank worker, through vLLM's `load_general_plugins()`.

```{literalinclude} ../../../llm/doc_code/serve/custom_vllm/qwen3_reward_plugin/__init__.py
:language: python
:start-after: __register_start__
:end-before: __register_end__
```

### Package it

Declare the `vllm.general_plugins` entry point in `setup.py`. vLLM discovers the package and runs `register()` when the package installs.

```{literalinclude} ../../../llm/doc_code/serve/custom_vllm/setup.py
:language: python
:start-after: __setup_start__
:end-before: __setup_end__
```

Install the plugin into the image your Ray cluster runs, alongside vLLM, so every node and every vLLM engine and worker process can discover it:

```bash
pip install .  # from the plugin project directory
```

## Deploy the model

Configure the model as a pooling deployment, enable direct streaming, then launch it with the Python API or a YAML config.

### Configure the model

Set `runner="pooling"` because a reward model encodes rather than generates, and use `engine_kwargs.hf_overrides` to select the custom architecture and configure a single regression score (`num_labels=1`, `problem_type="regression"` for an identity activation).

The plugin entry point registers the architecture in the vLLM engine and worker processes. Ray Serve LLM also resolves and validates the architecture while it builds the engine configuration, so register it there too: point `server_cls` at an `LLMServer` subclass whose import calls `register()`.

```{literalinclude} ../../../llm/doc_code/serve/custom_vllm/qwen3_reward_plugin/serve_hook.py
:language: python
:start-after: __serve_hook_start__
:end-before: __serve_hook_end__
```

### Enable direct streaming

Serve with {doc}`direct streaming <direct-streaming>` so vLLM's native `/classify` route is exposed. Export both environment variables before starting Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
```

### Deploy

Deploy with the Python API or an equivalent YAML config:

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../../llm/doc_code/serve/custom_vllm/custom_vllm_example.py
:language: python
:start-after: __custom_vllm_example_start__
:end-before: __custom_vllm_example_end__
```

Save this as `app.py` and run it to deploy.
:::

:::{tab-item} YAML
:sync: yaml

```{literalinclude} ../../../llm/doc_code/serve/custom_vllm/custom_vllm_config.yaml
:language: yaml
```

Deploy it with `serve run custom_vllm_config.yaml`.
:::

::::

## Query the model

Run the following command to verify your model is serving. This query sends text to the `/classify` endpoint and reads the scalar reward from `data[0].probs[0]`. Until you provide trained reward-head weights (see the next section), this value is arbitrary.

::::{tab-set}

:::{tab-item} Python
:sync: python

```python
import requests

response = requests.post(
    "http://localhost:8000/classify",
    json={"model": "qwen3-reward", "input": "The capital of France is Paris."},
).json()
print(response["data"][0]["probs"][0])  # scalar reward
```
:::

:::{tab-item} cURL
:sync: curl

```bash
curl -X POST http://localhost:8000/classify \
     -H "Content-Type: application/json" \
     -d '{
           "model": "qwen3-reward",
           "input": "The capital of France is Paris."
         }'
```
:::

::::

## Provide the reward-head weights

The reward head is a separate `Linear(hidden_size, 1)` whose weights are not part of the base Hugging Face checkpoint, so the model loads them from a file system or an object store. The path is plugin-specific. In this example, the model reads it from the `RM_REWARD_HEAD_PATH` environment variable. Include the weights file in your image and set the variable through the deployment's `runtime_env`:

```python
llm_config = LLMConfig(
    # ... same fields as above ...
    runtime_env=dict(env_vars=dict(RM_REWARD_HEAD_PATH="/path/to/reward_head.pt")),
)
```

## See also

- [vLLM plugin system](https://docs.vllm.ai/en/stable/design/plugin_system.html)
- [Registering a model to vLLM](https://docs.vllm.ai/en/stable/contributing/model/registration.html)
- {doc}`Direct streaming <direct-streaming>`: how Ray Serve LLM serves vLLM's native routes.
