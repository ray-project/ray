(data-parallel-attention-guide)=
# Data parallel attention

Deploy LLMs with data parallel attention for increased throughput and better resource utilization, especially for sparse MoE (Mixture of Experts) models.

Data parallel attention creates multiple coordinated inference engine replicas that process requests in parallel. This pattern is most effective when combined with expert parallelism for sparse MoE models, where attention (QKV) layers are replicated across replicas while MoE experts are sharded. This separation provides:

- **Increased throughput**: Process more concurrent requests by distributing them across multiple replicas.
- **Better resource utilization**: Especially beneficial for sparse MoE models where not all experts are active for each request.
- **KV cache scalability**: Add more KV cache capacity across replicas to handle larger batch sizes.
- **Expert saturation**: Achieve higher effective batch sizes during decoding to better saturate MoE layers.

## When to use data parallel attention

Consider this pattern when:

- **Sparse MoE models with MLA**: You're serving models with Multi-head Latent Attention (MLA) where KV cache can't be sharded along the head dimension. MLA reduces KV cache memory requirements, making data parallel replication more efficient.
- **High throughput requirements**: You need to serve many concurrent requests and want to maximize throughput.
- **KV-cache limited**: Adding more KV cache capacity increases throughput, and data parallel attention effectively increases KV cache capacity across replicas.


**When not to use data parallel attention:**

- **Low to medium throughput**: If you can't saturate the MoE layers, data parallel attention adds unnecessary complexity.
- **Non-MoE models**: The main benefit is lifting effective batch size for saturating experts, which doesn't apply to dense models.
- **Sufficient tensor parallelism**: For models with GQA (Grouped Query Attention), use tensor parallelism (TP) first to shard KV cache up to `TP_size <= num_kv_heads`. Beyond that, TP requires KV cache replication—at that point, data parallel attention becomes a better choice.

## Basic deployment

The following example shows how to deploy with data parallel attention:

```python
from ray.serve.llm import LLMConfig, build_dp_openai_app
import ray.serve as serve

# Configure the model with data parallel settings
config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 2,  # Number of DP replicas
        "tensor_parallel_size": 1,  # TP size per replica
    },
    experimental_configs={
        # This is a temporary required config. We will remove this in future versions.
        "dp_size_per_node": 2,  # DP replicas per node
    },
    accelerator_type="A10G",
)

app = build_dp_openai_app({
    "llm_config": config
})

serve.run(app)
```

### Deployment without OpenAI ingress

If you only need the deployment without the OpenAI-compatible ingress, use `build_dp_deployment`:

```python
from ray.serve.llm import LLMConfig, build_dp_deployment
import ray.serve as serve

config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 4,
        "tensor_parallel_size": 1,
    },
    experimental_configs={
        "dp_size_per_node": 4,
    },
)

dp_deployment = build_dp_deployment(config)
serve.run(dp_deployment)
```

## Production YAML configuration

For production deployments, use a YAML configuration file:

```yaml
applications:
- name: dp_llm_app
  route_prefix: /
  import_path: ray.serve.llm:build_dp_openai_app
  args:
    llm_config:
      model_loading_config:
        model_id: Qwen/Qwen2.5-0.5B-Instruct
      engine_kwargs:
        data_parallel_size: 4
        tensor_parallel_size: 2
      experimental_configs:
        dp_size_per_node: 4
```

Deploy with:

```bash
serve deploy dp_config.yaml
```

:::{note}
The `num_replicas` in `deployment_config` must equal `data_parallel_size` in `engine_kwargs`. Autoscaling is not supported for data parallel attention deployments since all replicas must be present and coordinated.
:::

## Configuration parameters

### Required parameters

- `data_parallel_size`: Number of data parallel replicas to create. Must be a positive integer.
- `dp_size_per_node`: Number of DP replicas per node. Must be set in `experimental_configs`. This controls how replicas are distributed across nodes. This is a temporary required config that we will remove in future versions. 

### Deployment configuration

- `num_replicas`: Must be set to `data_parallel_size`. Data parallel attention requires a fixed number of replicas.
- `placement_group_strategy`: Automatically set to `"STRICT_PACK"` to ensure replicas are properly placed.

## Understanding replica coordination

In data parallel attention, all replicas work together as a cohesive unit:

1. **Rank assignment**: Each replica receives a unique rank (0 to `dp_size-1`) from a coordinator.
2. **Request distribution**: Ray Serve's request router distributes requests across replicas using load balancing.
3. **Collective operations**: Replicas coordinate for collective operations (e.g., all-reduce) required by the model.
4. **Synchronization**: All replicas must be present and healthy for the deployment to function correctly.

The coordination overhead is minimal:
- **Startup**: Each replica makes one RPC call to get its rank.
- **Runtime**: No coordination overhead during request processing.

For more details, see {doc}`../architecture/serving-patterns/data-parallel`.

## Test your deployment

Test with a chat completion request:

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer fake-key" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "messages": [
      {"role": "user", "content": "Explain data parallel attention"}
    ],
    "max_tokens": 100,
    "temperature": 0.7
  }'
```

You can also test programmatically:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8000/v1",
    api_key="fake-key"
)

response = client.chat.completions.create(
    model="Qwen/Qwen2.5-0.5B-Instruct",
    messages=[
        {"role": "user", "content": "Explain data parallel attention"}
    ],
    max_tokens=100
)

print(response.choices[0].message.content)
```


## Combining with other patterns

### Data parallel + Prefill-decode disaggregation

You can combine data parallel attention with prefill-decode disaggregation to scale both phases independently while using DP within each phase. This pattern is useful when you need high throughput for both prefill and decode phases.

The following example shows a complete, functional deployment:

```python
from ray.serve.llm import LLMConfig, build_dp_deployment
from ray.serve.llm.deployment import PDProxyServer
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress
import ray.serve as serve

# Configure prefill with data parallel attention
prefill_config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 2,  # 2 DP replicas for prefill
        "tensor_parallel_size": 1,
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        }
    },
    experimental_configs={
        "dp_size_per_node": 2,
    },
)

# Configure decode with data parallel attention
decode_config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 4,  # 4 DP replicas for decode
        "tensor_parallel_size": 1,
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        }
    },
    experimental_configs={
        "dp_size_per_node": 4,
    },
)

# Build prefill and decode deployments with DP
prefill_deployment = build_dp_deployment(prefill_config, name_prefix="Prefill:")
decode_deployment = build_dp_deployment(decode_config, name_prefix="Decode:")

# Create PDProxyServer to coordinate between prefill and decode
proxy_options = PDProxyServer.get_deployment_options(prefill_config, decode_config)
proxy_deployment = serve.deployment(PDProxyServer).options(**proxy_options).bind(
    prefill_server=prefill_deployment,
    decode_server=decode_deployment,
)

# Create OpenAI-compatible ingress
ingress_options = OpenAiIngress.get_deployment_options([prefill_config, decode_config])
ingress_cls = make_fastapi_ingress(OpenAiIngress)
ingress_deployment = serve.deployment(ingress_cls).options(**ingress_options).bind(
    llm_deployments=[proxy_deployment]
)

# Deploy the application
serve.run(ingress_deployment, blocking=True)
```

This configuration creates:
- **Prefill phase**: 2 data parallel replicas for processing input prompts
- **Decode phase**: 4 data parallel replicas for generating tokens
- **PDProxyServer**: Coordinates requests between prefill and decode phases
- **OpenAI ingress**: Provides OpenAI-compatible API endpoints

This allows you to:
- Optimize prefill and decode phases independently based on workload characteristics
- Use data parallel attention within each phase for increased throughput

:::{note}
For this example to work, you need to have NIXL installed. See the {doc}`prefill-decode` guide for prerequisites and installation instructions.
:::


## See also

- {doc}`../architecture/serving-patterns/data-parallel` - Data parallel attention architecture details
- {doc}`prefill-decode` - Prefill-decode disaggregation guide
- {doc}`../architecture/serving-patterns/index` - Overview of serving patterns
- {doc}`../quick-start` - Basic LLM deployment examples

