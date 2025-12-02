(quick-start)=
# Quickstart examples

## Deployment through OpenAiIngress

You can deploy LLM models using either the builder pattern or bind pattern.

::::{tab-set}

:::{tab-item} Builder Pattern
:sync: builder

```{literalinclude} ../../llm/doc_code/serve/qwen/qwen_example.py
:language: python
:start-after: __qwen_example_start__
:end-before: __qwen_example_end__
```
:::

:::{tab-item} Bind Pattern
:sync: bind

```python
from ray import serve
from ray.serve.llm import LLMConfig
from ray.serve.llm.deployment import LLMServer
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    # Pass the desired accelerator type (e.g. A10G, L4, etc.)
    accelerator_type="A10G",
    # You can customize the engine arguments (e.g. vLLM engine kwargs)
    engine_kwargs=dict(
        tensor_parallel_size=2,
    ),
)

# Deploy the application
server_options = LLMServer.get_deployment_options(llm_config)
server_deployment = serve.deployment(LLMServer).options(
    **server_options).bind(llm_config)

ingress_options = OpenAiIngress.get_deployment_options(
    llm_configs=[llm_config])
ingress_cls = make_fastapi_ingress(OpenAiIngress)
ingress_deployment = serve.deployment(ingress_cls).options(
    **ingress_options).bind([server_deployment])

serve.run(ingress_deployment, blocking=True)
```
:::

::::

You can query the deployed models with either cURL or the OpenAI Python client:

::::{tab-set}

:::{tab-item} cURL
:sync: curl

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer fake-key" \
     -d '{
           "model": "qwen-0.5b",
           "messages": [{"role": "user", "content": "Hello!"}]
         }'
```
:::

:::{tab-item} Python
:sync: python

```python
from openai import OpenAI

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Basic chat completion with streaming
response = client.chat.completions.create(
    model="qwen-0.5b",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)

for chunk in response:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)
```
:::

::::


For deploying multiple models, you can pass a list of {class}`LLMConfig <ray.serve.llm.LLMConfig>` objects to the {class}`OpenAiIngress <ray.serve.llm.ingress.OpenAiIngress>` deployment:

::::{tab-set}

:::{tab-item} Builder Pattern
:sync: builder

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app


llm_config1 = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    accelerator_type="A10G",
)

llm_config2 = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-1.5b",
        model_source="Qwen/Qwen2.5-1.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    accelerator_type="A10G",
)

app = build_openai_app({"llm_configs": [llm_config1, llm_config2]})
serve.run(app, blocking=True)
```
:::

:::{tab-item} Bind Pattern
:sync: bind

```python
from ray import serve
from ray.serve.llm import LLMConfig
from ray.serve.llm.deployment import LLMServer
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress

llm_config1 = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    accelerator_type="A10G",
)

llm_config2 = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-1.5b",
        model_source="Qwen/Qwen2.5-1.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    accelerator_type="A10G",
)

# deployment #1
server_options1 = LLMServer.get_deployment_options(llm_config1)
server_deployment1 = serve.deployment(LLMServer).options(
    **server_options1).bind(llm_config1)

# deployment #2
server_options2 = LLMServer.get_deployment_options(llm_config2)
server_deployment2 = serve.deployment(LLMServer).options(
    **server_options2).bind(llm_config2)

# ingress
ingress_options = OpenAiIngress.get_deployment_options(
    llm_configs=[llm_config1, llm_config2])
ingress_cls = make_fastapi_ingress(OpenAiIngress)
ingress_deployment = serve.deployment(ingress_cls).options(
    **ingress_options).bind([server_deployment1, server_deployment2])

# run
serve.run(ingress_deployment, blocking=True)
```
:::

::::

## Production deployment

For production deployments, Ray Serve LLM provides utilities for config-driven deployments. You can specify your deployment configuration with YAML files:

::::{tab-set}

:::{tab-item} Inline Config
:sync: inline

```{literalinclude} ../../llm/doc_code/serve/qwen/llm_config_example.yaml
:language: yaml
```
:::

:::{tab-item} Standalone Config
:sync: standalone

```yaml
# config.yaml
applications:
- args:
    llm_configs:
        - models/qwen-0.5b.yaml
        - models/qwen-1.5b.yaml
  import_path: ray.serve.llm:build_openai_app
  name: llm_app
  route_prefix: "/"
```

```yaml
# models/qwen-0.5b.yaml
model_loading_config:
  model_id: qwen-0.5b
  model_source: Qwen/Qwen2.5-0.5B-Instruct
accelerator_type: A10G
deployment_config:
  autoscaling_config:
    min_replicas: 1
    max_replicas: 2
```

```yaml
# models/qwen-1.5b.yaml
model_loading_config:
  model_id: qwen-1.5b
  model_source: Qwen/Qwen2.5-1.5B-Instruct
accelerator_type: A10G
deployment_config:
  autoscaling_config:
    min_replicas: 1
    max_replicas: 2
```
:::

::::

To deploy with either configuration file:

```bash
serve run config.yaml
```

For monitoring and observability, see {doc}`Observability <user-guides/observability>`.

## Advanced usage patterns

For each usage pattern, Ray Serve LLM provides a server and client code snippet.

### Cross-node parallelism

Ray Serve LLM supports cross-node tensor parallelism (TP) and pipeline parallelism (PP), allowing you to distribute model inference across multiple GPUs and nodes. See {doc}`Cross-node parallelism <user-guides/cross-node-parallelism>` for a comprehensive guide on configuring and deploying models with cross-node parallelism.

