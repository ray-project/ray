(kv-cache-offloading-guide)=
# KV cache offloading

Extend KV cache capacity by offloading to CPU memory or local disk for larger batch sizes and reduced GPU memory pressure.

:::{note}
Ray Serve doesn't provide KV cache offloading out of the box, but integrates seamlessly with vLLM solutions. This guide demonstrates one such integration: LMCache.
:::


Benefits of KV cache offloading:

- **Increased capacity**: Store more KV caches by using CPU RAM or local storage instead of relying solely on GPU memory
- **Cache reuse across requests**: Save and reuse previously computed KV caches for repeated or similar prompts, reducing prefill computation
- **Flexible storage backends**: Choose from multiple storage options including local CPU, disk, or distributed systems

Consider KV cache offloading when your application has repeated prompts or multi-turn conversations where you can reuse cached prefills. If consecutive conversation queries aren't sent immediately, the GPU evicts these caches to make room for other concurrent requests, causing cache misses. Offloading KV caches to CPU memory or other storage backends, which has much larger capacity, preserves them for longer periods.  

## Deploy with LMCache

LMCache provides KV cache offloading with support for multiple storage backends.

### Prerequisites

Install LMCache:

```bash
uv pip install lmcache
```

### Basic deployment

The following example shows how to deploy with LMCache for local CPU offloading:

::::{tab-set}
:::{tab-item} Python
```python
from ray.serve.llm import LLMConfig, build_openai_app
import ray.serve as serve

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct"
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "kv_transfer_config": {
            "kv_connector": "LMCacheConnectorV1",
            "kv_role": "kv_both",
        }
    },
    runtime_env={
        "env_vars": {
            "LMCACHE_LOCAL_CPU": "True",
            "LMCACHE_CHUNK_SIZE": "256",
            "LMCACHE_MAX_LOCAL_CPU_SIZE": "100",  # 100GB
        }
    }
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: llm-with-lmcache
    route_prefix: /
    import_path: ray.serve.llm:build_openai_app
    runtime_env:
      env_vars:
        LMCACHE_LOCAL_CPU: "True"
        LMCACHE_CHUNK_SIZE: "256"
        LMCACHE_MAX_LOCAL_CPU_SIZE: "100"
    args:
      llm_configs:
        - model_loading_config:
            model_id: qwen-0.5b
            model_source: Qwen/Qwen2-0.5B-Instruct
          engine_kwargs:
            tensor_parallel_size: 1
            kv_transfer_config:
              kv_connector: LMCacheConnectorV1
              kv_role: kv_both
```

Deploy with:

```bash
serve run config.yaml
```
:::
::::

## Compose multiple KV transfer backends with MultiConnector

You can combine multiple KV transfer backends using `MultiConnector`. This is useful when you want both local offloading and cross-instance transfer in disaggregated deployments.

### When to use MultiConnector

Use `MultiConnector` to combine multiple backends when you're using prefill/decode disaggregation and want both cross-instance transfer (NIXL) and local offloading.


The following example shows how to combine NIXL (for cross-instance transfer) with LMCache (for local offloading) in a prefill/decode deployment:

:::{note}
The order of connectors matters. Since you want to prioritize local KV cache lookup through LMCache, it appears first in the list before the NIXL connector.
:::

::::{tab-set}
:::{tab-item} Python
```python
from ray.serve.llm import LLMConfig, build_pd_openai_app
import ray.serve as serve

# Shared KV transfer config combining NIXL and LMCache
kv_config = {
    "kv_connector": "MultiConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "connectors": [
            {
                "kv_connector": "LMCacheConnectorV1",
                "kv_role": "kv_both",
            },
            {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
                "backends": ["UCX"],
            }
        ]
    }
}

prefill_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct"
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "kv_transfer_config": kv_config,
    },
    runtime_env={
        "env_vars": {
            "LMCACHE_LOCAL_CPU": "True",
            "LMCACHE_CHUNK_SIZE": "256",
            "UCX_TLS": "all",
        }
    }
)

decode_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct"
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "kv_transfer_config": kv_config,
    },
    runtime_env={
        "env_vars": {
            "LMCACHE_LOCAL_CPU": "True",
            "LMCACHE_CHUNK_SIZE": "256",
            "UCX_TLS": "all",
        }
    }
)

pd_config = {
    "prefill_config": prefill_config,
    "decode_config": decode_config,
}

app = build_pd_openai_app(pd_config)
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: pd-multiconnector
    route_prefix: /
    import_path: ray.serve.llm:build_pd_openai_app
    runtime_env:
      env_vars:
        LMCACHE_LOCAL_CPU: "True"
        LMCACHE_CHUNK_SIZE: "256"
        UCX_TLS: "all"
    args:
      prefill_config:
        model_loading_config:
          model_id: qwen-0.5b
          model_source: Qwen/Qwen2-0.5B-Instruct
        engine_kwargs:
          tensor_parallel_size: 1
          kv_transfer_config:
            kv_connector: MultiConnector
            kv_role: kv_both
            kv_connector_extra_config:
              connectors:
                - kv_connector: LMCacheConnectorV1
                  kv_role: kv_both
                - kv_connector: NixlConnector
                  kv_role: kv_both
                  backends: ["UCX"]
      decode_config:
        model_loading_config:
          model_id: qwen-0.5b
          model_source: Qwen/Qwen2-0.5B-Instruct
        engine_kwargs:
          tensor_parallel_size: 1
          kv_transfer_config:
            kv_connector: MultiConnector
            kv_role: kv_both
            kv_connector_extra_config:
              connectors:
                - kv_connector: LMCacheConnectorV1
                  kv_role: kv_both
                - kv_connector: NixlConnector
                  kv_role: kv_both
                  backends: ["UCX"]
```

Deploy with:

```bash
serve run config.yaml
```
:::
::::

## Configuration parameters

### LMCache environment variables

- `LMCACHE_LOCAL_CPU`: Set to `"True"` to enable local CPU offloading
- `LMCACHE_CHUNK_SIZE`: Size of KV cache chunks, in terms of tokens (default: 256)
- `LMCACHE_MAX_LOCAL_CPU_SIZE`: Maximum CPU storage size in GB
- `LMCACHE_PD_BUFFER_DEVICE`: Buffer device for prefill/decode scenarios (default: "cpu")

For the full list of LMCache configuration options, see the [LMCache configuration reference](https://docs.lmcache.ai/api_reference/configurations.html).

### MultiConnector configuration

- `kv_connector`: Set to `"MultiConnector"` to compose multiple backends
- `kv_connector_extra_config.connectors`: List of connector configurations to compose. Order matters—connectors earlier in the list take priority.
- Each connector in the list uses the same configuration format as standalone connectors

## Performance considerations

Extending KV cache beyond local GPU memory introduces overhead for managing and looking up caches across different memory hierarchies. This creates a tradeoff: you gain larger cache capacity but may experience increased latency. Consider these factors:

**Overhead in cache-miss scenarios**: When there are no cache hits, offloading adds modest overhead (~10-15%) compared to pure GPU caching, based on our internal experiments. This overhead comes from the additional hashing, data movement, and management operations.

**Benefits with cache hits**: When caches can be reused, offloading significantly reduces prefill computation. For example, in multi-turn conversations where users return after minutes of inactivity, LMCache retrieves the conversation history from CPU rather than recomputing it, significantly reducing time to first token for follow-up requests.

**Network transfer costs**: When combining MultiConnector with cross-instance transfer (such as NIXL), ensure that the benefits of disaggregation outweigh the network transfer costs.


## Deploy on Kubernetes with LMCache and Mooncake

For distributed KV cache sharing across multiple GPU workers, you can use LMCache with Mooncake as the storage backend. Mooncake creates a distributed memory pool by aggregating memory from multiple nodes, supporting high-bandwidth RDMA or TCP transfer.

### Install system packages

Mooncake requires system-level dependencies. Use the `args` field in your RayService worker spec to install them at container startup:

```yaml
# Reference for system packages required by Mooncake: https://kvcache-ai.github.io/Mooncake/getting_started/build.html
- name: ray-worker
  image: rayproject/ray-llm:2.53.0-py311-cu128
  args:
    - |
      sudo apt-get update && \
      sudo apt-get install -y --no-install-recommends \
        build-essential cmake libibverbs-dev libgoogle-glog-dev \
        libgtest-dev libjsoncpp-dev libnuma-dev libunwind-dev \
        libpython3-dev libboost-all-dev libssl-dev pybind11-dev \
        libcurl4-openssl-dev libhiredis-dev pkg-config patchelf && \
      sudo rm -rf /var/lib/apt/lists/* \
```

For general Kubernetes dependency patterns, also see [Add custom dependencies](kuberay-rayservice-custom-deps) in the RayService user guide.

### Install LMCache and Mooncake via runtime_env

Add the Python packages through `runtime_env` in your LLM configuration:

```yaml
runtime_env:
  pip:
    - lmcache
    - mooncake-transfer-engine
  env_vars:
    LMCACHE_CONFIG_FILE: "/mnt/configs/lmcache-config.yaml"
    PYTHONHASHSEED: "0"  # Required for consistent pre-caching keys
```

### Configure kv_transfer_config

Enable the LMCache connector in your engine configuration:

```yaml
engine_kwargs:
  kv_transfer_config:
    kv_connector: "LMCacheConnectorV1"
    kv_role: "kv_both"
```

### LMCache configuration for Mooncake

Create a ConfigMap with your LMCache configuration file and mount it on your worker containers. The configuration specifies the Mooncake master address, metadata server, and transfer protocol. For the full configuration reference, see the [LMCache Mooncake backend documentation](https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html).

Example `lmcache-config.yaml`:

```yaml
chunk_size: 256
remote_url: "mooncakestore://mooncake-master.default.svc.cluster.local:50051/" # Pre-setup mooncake server by creating custom deployment.
remote_serde: "naive"
local_cpu: true
max_local_cpu_size: 64  # GB

extra_config:
  # Using etcd instead of mooncake metadata server, since it is currently unstable and it is advised to use etcd. 
  # Refer here for setting up etcd in K8s environment: https://etcd.io/docs/v3.6/op-guide/kubernetes/
  metadata_server: "etcd://etcd.default.svc.cluster.local:2379"
  protocol: "tcp"  # Use "rdma" for RDMA-capable networks
  master_server_address: "mooncake-master.default.svc.cluster.local:50051"
  global_segment_size: 21474836480  # 20GB per worker
```

:::{note}
This setup requires a running Mooncake master service and metadata server (etcd or HTTP). See the [Mooncake deployment guide](https://kvcache-ai.github.io/Mooncake/getting_started/build.html) for infrastructure setup.
:::

## See also

- {doc}`Prefill/decode disaggregation <prefill-decode>` - Deploy LLMs with separated prefill and decode phases
- [LMCache documentation](https://docs.lmcache.ai/) - Comprehensive LMCache configuration and features
- [LMCache Mooncake backend](https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html) - Distributed KV cache storage setup
- [Mooncake build guide](https://kvcache-ai.github.io/Mooncake/getting_started/build.html) - System dependencies and installation
