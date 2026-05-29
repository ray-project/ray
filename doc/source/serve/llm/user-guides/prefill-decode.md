(prefill-decode-guide)=
# Prefill/decode disaggregation

Deploy LLMs with separated prefill and decode phases for better resource utilization and cost optimization.

:::{warning}
This feature requires vLLM v1, which is the default engine. For legacy deployments using vLLM v0, upgrade to v1 first.
:::

Prefill/decode disaggregation separates the prefill phase (processing input prompts) from the decode phase (generating tokens). This separation provides:

- **Independent optimization**: You can optimize prefill separately from decode with different configurations.
- **Reduced interference**: Prefill operations can interfere with decode operations and vice versa, degrading performance during unpredictable traffic spikes. Disaggregation removes this contention.
- **Independent scaling**: You can scale each phase independently based on demand.
- **Cost optimization**: You can use different node types for different workloads, taking advantage of heterogeneous clusters.

vLLM provides several KV transfer backends for disaggregated serving:

1. **NIXLConnector**: Network-based KV cache transfer using NVIDIA Inference Xfer Library (NIXL) with support for various backends such as UCX, libfabric, and EFA. Simple setup with minimal configuration.
2. **LMCacheConnectorV1**: Advanced caching solution with support for various storage backends, including integration with NIXL. 

## When to use prefill/decode disaggregation

Consider this pattern when:

- You have variable workload patterns with different resource needs for prefill vs decode.
- You want to optimize costs by using different hardware for different phases.
- Your application has high throughput requirements that benefit from decoupling prefill and decode.

## Deploy with NIXLConnector

NIXLConnector provides network-based KV cache transfer between prefill and decode servers with minimal configuration.

### Prerequisites

If you use [ray-project/ray-llm](https://hub.docker.com/r/rayproject/ray-llm/tags) Docker images, NIXL is already installed. Otherwise, install it:

```bash
uv pip install nixl
```

The NIXL wheel comes bundled with its supported backends (UCX, libfabric, EFA, etc.). These shared binaries may not be the latest version for your hardware and network stack. If you need the latest versions, install NIXL from source against the target backend library. See the [NIXL installation guide](https://github.com/ai-dynamo/nixl?tab=readme-ov-file#prerequisites-for-source-build) for details.

### Basic deployment

The following example shows how to deploy with NIXLConnector:

```python
from ray.serve.llm import LLMConfig, build_pd_openai_app
import ray.serve as serve

# Configure prefill instance
prefill_config = LLMConfig(
    model_loading_config={
        "model_id": "meta-llama/Llama-3.1-8B-Instruct"
    },
    engine_kwargs={
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        }
    }
)

# Configure decode instance
decode_config = LLMConfig(
    model_loading_config={
        "model_id": "meta-llama/Llama-3.1-8B-Instruct"
    },
    engine_kwargs={
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        }
    }
)

pd_config = dict(
    prefill_config=prefill_config,
    decode_config=decode_config,
)

app = build_pd_openai_app(pd_config)
serve.run(app)
```

### Production YAML configuration

For production deployments, use a YAML configuration file:

```{literalinclude} ../../doc_code/pd_dissagregation/nixl_example.yaml
:language: yaml
```

Deploy with:

```bash
serve deploy nixl_config.yaml
```

### Configuration parameters

- `kv_connector`: Set to `"NixlConnector"` to use NIXL.
- `kv_role`: Set to `"kv_both"` for both prefill and decode instances.

## Deploy with LMCacheConnectorV1

LMCacheConnectorV1 provides advanced caching with support for multiple storage backends.

### Prerequisites

Install LMCache:

```bash
uv pip install lmcache
```

### Scenario 1: LMCache with NIXL backend

This configuration uses LMCache with a NIXL-based storage backend for network communication.

The following is an example Ray Serve configuration for LMCache with NIXL:

```{literalinclude} ../../doc_code/pd_dissagregation/lmcache_nixl_example.yaml
:language: yaml
```

Create the LMCache configuration for the prefill instance (`lmcache_prefiller.yaml`):

```{literalinclude} ../../doc_code/pd_dissagregation/lmcache/nixl/prefiller.yaml
:language: yaml
```

Create the LMCache configuration for the decode instance (`lmcache_decoder.yaml`):

```{literalinclude} ../../doc_code/pd_dissagregation/lmcache/nixl/decoder.yaml
:language: yaml
```

:::{note}
The `LMCACHE_CONFIG_FILE` environment variable must point to an existing configuration file that's accessible within the Ray Serve container or worker environment. Ensure these configuration files are properly mounted or available in your deployment environment.
:::

### Scenario 2: LMCache with Mooncake store backend

This configuration uses LMCache with Mooncake store, a high-performance distributed storage system.

The following is an example Ray Serve configuration for LMCache with Mooncake:

```{literalinclude} ../../doc_code/pd_dissagregation/lmcache_mooncake_example.yaml
:language: yaml
```

Create the LMCache configuration for Mooncake (`lmcache_mooncake.yaml`):

```{literalinclude} ../../doc_code/pd_dissagregation/lmcache/mooncake.yaml
:language: yaml
```

:::{warning}
For Mooncake deployments:
- Ensure the etcd metadata server is running and accessible at the specified address.
- Verify that you properly configured RDMA devices and storage servers and that they are accessible.
- In containerized deployments, mount configuration files with appropriate read permissions (for example, `chmod 644`).
- Ensure all referenced hostnames and IP addresses in configuration files are resolvable from the deployment environment.
:::

### Configuration parameters

- `kv_connector`: Set to `"LMCacheConnectorV1"`.
- `kv_role`: Set to `"kv_producer"` for prefill, `"kv_consumer"` for decode.
- `kv_buffer_size`: Size of the KV cache buffer.
- `LMCACHE_CONFIG_FILE`: Environment variable that specifies the configuration file path.

## Test your deployment

Before deploying with LMCacheConnectorV1, start the required services:

```bash
# Start etcd server if not already running
docker run -d --name etcd-server \
  -p 2379:2379 -p 2380:2380 \
  quay.io/coreos/etcd:latest \
  etcd --listen-client-urls http://0.0.0.0:2379 \
       --advertise-client-urls http://localhost:2379

# For Mooncake backend, start the Mooncake master
# See https://docs.lmcache.ai/kv_cache/mooncake.html for details
mooncake_master --port 49999
```

Test with a chat completion request:

```bash
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "meta-llama/Llama-3.1-8B-Instruct",
    "messages": [
      {"role": "user", "content": "Explain the benefits of prefill/decode disaggregation"}
    ],
    "max_tokens": 100,
    "temperature": 0.7
  }'
```

## Best practices

- **Choose the right backend**: Use NIXLConnector for simpler deployments. Use LMCacheConnectorV1 when you need advanced caching or multiple storage backends.
- **Monitor KV transfer overhead**: Ensure that the benefits of disaggregation outweigh the network transfer costs. Monitor latency and throughput.
- **Scale independently**: Take advantage of independent scaling by monitoring resource utilization for each phase separately.
- **Test with realistic workloads**: Validate performance improvements with your actual traffic patterns before production deployment.
- **Ensure network connectivity**: For NIXLConnector, verify that prefill and decode instances can communicate over the network.
- **Secure etcd access**: For LMCacheConnectorV1, ensure your etcd server is properly secured and accessible only to authorized services.

## Troubleshooting

### Prefill and decode instances can't communicate

- Verify network connectivity between instances with sufficient bandwidth for KV transfer.
- Check that your network supports the backend you're using (such as RDMA for high-performance deployments).
- For NIXLConnector, ensure NIXL is properly installed on all nodes.
- Verify firewall rules and security groups allow communication between prefill and decode instances.

### LMCache configuration not found

- Verify the `LMCACHE_CONFIG_FILE` environment variable points to an existing file.
- Ensure the configuration file is accessible from the Ray Serve worker environment.
- Check that the file has appropriate read permissions.


## See also

- [LMCache disaggregated serving documentation](https://docs.lmcache.ai/disaggregated_prefill/nixl/index.html)
- [NIXLConnector usage guide](https://docs.vllm.ai/en/stable/features/nixl_connector_usage.html)
- {doc}`Quickstart <../quick-start>` - Basic LLM deployment examples
