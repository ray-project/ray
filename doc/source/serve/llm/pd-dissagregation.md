(serve-pd-dissagregation)=
# Prefill/Decode Disaggregation with KV Transfer Backends

## Overview

Prefill/decode disaggregation is a technique that separates the prefill phase (processing input prompts) from the decode phase (generating tokens). This separation allows for:

- **Better resource utilization**: Prefill operations can use high-memory, high-compute nodes while decode operations can use optimized inference nodes
- **Improved scalability**: Each phase can be scaled independently based on demand
- **Cost optimization**: Different node types can be used for different workloads

vLLM v1 supports two main KV transfer backends:
1. **NIXLConnector**: Network-based KV cache transfer using NIXL (Network Interface for eXtended LLM). Simple setup with automatic network configuration.
2. **LMCacheConnectorV1**: Advanced caching solution with support for various storage backends. **Requires etcd server** for metadata coordination between prefill and decode instances.

## Prerequisites

Make sure that you are using vLLM v1 by setting `VLLM_USE_V1=1` environment variable.

For NixlConnector make sure nixl is installed. If you use [ray-project/ray-llm](https://hub.docker.com/r/rayproject/ray-llm/tags) images you automatically get the dependency installed. 

For LMCacheConnectorV1, also install LMCache:

```bash
pip install lmcache
```

## NIXLConnector Backend

The NIXLConnector provides network-based KV cache transfer between prefill and decode servers using a side channel communication mechanism.

### Basic Configuration

```python
from ray.serve.llm import LLMConfig, build_pd_openai_app

# Prefill configuration
prefill_config = LLMConfig(
    model_loading_config={
        "model_id": "meta-llama/Llama-3.1-8B-Instruct"
    },
    engine_kwargs={
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
            "engine_id": "engine1"
        }
    }
)

# Decode configuration
decode_config = LLMConfig(
    model_loading_config={
        "model_id": "meta-llama/Llama-3.1-8B-Instruct"
    },
    engine_kwargs={
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
            "engine_id": "engine2"
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

### Complete YAML Configuration Example

Here's a complete configuration file for NIXLConnector:

```{literalinclude} ../doc_code/pd_dissagregation/nixl_example.yaml
:language: yaml
```

## LMCacheConnectorV1 Backend

LMCacheConnectorV1 provides a more advanced caching solution with support for multiple storage backends and enhanced performance features.

### Scenario 1: LMCache with NIXL Backend

This configuration uses LMCache with a NIXL-based storage backend for network communication.

```{literalinclude} ../doc_code/pd_dissagregation/lmcache_nixl_example.yaml
:language: yaml
```

#### LMCache Configuration for NIXL Backend

Create `lmcache_prefiller.yaml`:

```{literalinclude} ../doc_code/pd_dissagregation/lmcache/nixl/prefiller.yaml
:language: yaml
```

Create `lmcache_decoder.yaml`:

```{literalinclude} ../doc_code/pd_dissagregation/lmcache/nixl/decoder.yaml
:language: yaml
```

**Important**: The `LMCACHE_CONFIG_FILE` environment variable must point to an existing configuration file that is accessible within the Ray Serve container or worker environment. Ensure these configuration files are properly mounted or available in your deployment environment.

### Scenario 2: LMCache with Mooncake Store Backend

This configuration uses LMCache with Mooncake store, a high-performance distributed storage system.

```{literalinclude} ../doc_code/pd_dissagregation/lmcache_mooncake_example.yaml
:language: yaml
```

#### LMCache Configuration for Mooncake Store

Create `lmcache_mooncake.yaml`:

```{literalinclude} ../doc_code/pd_dissagregation/lmcache/mooncake.yaml
:language: yaml
```

**Important Notes**:
- The `LMCACHE_CONFIG_FILE` environment variable must point to an existing configuration file that is accessible within the Ray Serve container or worker environment.
- For Mooncake store backend, ensure the etcd metadata server is running and accessible at the specified address.
- Verify that RDMA devices and storage servers are properly configured and accessible.
- In containerized deployments, mount configuration files with appropriate read permissions (e.g., `chmod 644`).
- Ensure all referenced hostnames and IP addresses in configuration files are resolvable from the deployment environment.

## Deployment and Testing

### Deploy the Application

1. **Start required services** (for LMCacheConnectorV1):
   
   ```bash
   # Start etcd server if not already running
   docker run -d --name etcd-server \
     -p 2379:2379 -p 2380:2380 \
     quay.io/coreos/etcd:latest \
     etcd --listen-client-urls http://0.0.0.0:2379 \
          --advertise-client-urls http://localhost:2379
   
   # See https://docs.lmcache.ai/kv_cache/mooncake.html for more details.
   mooncake_master --port 49999
   ```

2. **Save your configuration** to a YAML file (e.g., `mooncake.yaml`)

3. **Deploy using Ray Serve CLI**:
   ```bash
   serve deploy pd_config.yaml
   ```

### Test the Deployment

Test with a simple request:

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

## Related Resources

LMCache prefill/decode dissagregation official guide:

- [dissagregated serving](https://docs.lmcache.ai/disaggregated_prefill/nixl/index.html)
