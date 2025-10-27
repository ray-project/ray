(data-parallel-deployment-guide)=
# Data parallel deployment (WIP)

Deploy large sparse MoE models using data parallel attention for high-throughput serving.

:::{warning}
**Work in Progress**: This guide covers advanced deployment patterns for sparse MoE models with data parallel attention. The feature is under active development.
:::

Data parallel (DP) deployment creates multiple coordinated inference engine instances that work together to serve requests in parallel. This pattern is particularly effective for sparse Mixture-of-Experts (MoE) models like DeepSeek-V3, where experts are parallelized across machines while attention layers are replicated.

## Key benefits

- **Higher throughput**: Serve more concurrent requests by parallelizing attention across replicas
- **Better GPU utilization**: Saturate sparse MoE layers by increasing effective batch size
- **Reduced KV cache bottleneck**: Distribute KV cache across replicas for models with MLA (Multi-head Latent Attention)
- **Coordinated inference**: Replicas work in sync to handle large batches efficiently

## When to use data parallel deployment

Consider this pattern when:

- **Deploying sparse MoE models**: Models like DeepSeek-V3, Mixtral, or similar architectures
- **High throughput requirements**: You need to serve many concurrent requests
- **KV cache limited**: Your model benefits from distributing KV cache across replicas
- **Using MLA attention**: Multi-head Latent Attention models benefit most from this pattern

Don't use this pattern when:

- **Low throughput workloads**: You can't saturate the MoE layers
- **Dense models**: The coordination overhead isn't worth it for non-MoE models
- **Limited resources**: You need autoscaling or variable replica counts

## Deployment example

The following example deploys DeepSeek-V3 with 16-way data parallelism and custom ingress configuration:

```{literalinclude} ../../doc_code/data_parallel_deployment.py
:language: python
```

This creates:
- 16 `DPServer` replicas working in coordination
- 1 `DPRankAssigner` for rank coordination
- OpenAI-compatible ingress with 64 replicas for high-throughput request routing

The `ingress_deployment_config` parameter allows you to customize the ingress layer independently from the backend data parallel deployment.

## Configuration options

### Data parallel size

The `data_parallel_size` parameter controls how many coordinated replicas to create. The example uses 16 replicas, which works well for a 2-node cluster with 8 GPUs per node.

Choose based on:
- Available GPUs in your cluster (typically 1 GPU per replica)
- Throughput requirements
- Expert parallelism configuration
- KV cache capacity needs

### Per-node distribution

The `dp_size_per_node` parameter helps Ray's placement strategy distribute replicas efficiently. The example uses 8, matching the number of GPUs per node in a typical H100/H200 setup.

This parameter tells the system to expect 8 replicas per node, allowing for better resource allocation and placement decisions.

### Ingress scaling

Control ingress scaling independently from the backend by passing `ingress_deployment_config` to `build_openai_dp_app`:

```python
ingress_deployment_config = {
    "autoscaling_config": {
        "min_replicas": 64,
        "max_replicas": 64,
    },
}

app = build_openai_dp_app(
    llm_config, 
    ingress_deployment_config=ingress_deployment_config
)
```

The ingress layer routes requests to the DP replicas. For high-throughput deployments, you may need many ingress replicas to handle the routing load efficiently.

## Performance tuning

### Batch size optimization

For MoE models, tune the batch size to saturate experts. The example uses `max_num_seqs: 4096` to allow many concurrent sequences per replica. With 16 replicas, this provides significant concurrent request handling capacity.

Higher batch sizes improve throughput but increase latency. Monitor TTFT and TPOT metrics to find the right balance for your workload.

### Memory optimization

The example disables prefix caching and sets `max_model_len: 16384` to manage memory usage. For very large models or limited GPU memory:

- Reduce `max_model_len` to decrease KV cache allocation
- Disable `enable_prefix_caching` to save memory
- Adjust `gpu_memory_utilization` (default 0.9) if needed
- Enable `enable_dbo` (Dynamic Batch Optimization) for better memory management

### Environment variables

Common environment variables for data parallel deployments (shown in the example above):

- `VLLM_USE_DEEP_GEMM`: Enable DeepGEMM optimization for MoE models
- `VLLM_ALL2ALL_BACKEND`: Choose the expert parallel communication backend
- `VLLM_MOE_DP_CHUNK_SIZE`: Set the data parallel chunk size for MoE
- `VLLM_SKIP_P2P_CHECK`: Skip P2P connectivity checks (if needed)
- `NVIDIA_GDRCOPY`: Enable NVIDIA GPUDirect RDMA Copy
- `PYTORCH_CUDA_ALLOC_CONF`: Configure PyTorch CUDA memory allocator

## Monitoring

Monitor your data parallel deployment using Ray Dashboard and custom metrics:

### Key metrics to watch

- **Throughput**: Requests per second across all replicas
- **Latency**: TTFT (Time to First Token) and TPOT (Time Per Output Token)
- **GPU utilization**: Should be high across all replicas
- **Queue depth**: Monitor request queuing in ingress

### Using Ray Dashboard

Access the Ray Dashboard to view:
- Deployment status and replica health
- Resource utilization per replica
- Request throughput and latency

## Troubleshooting

### Replicas not starting

**Symptom**: Deployment stuck in "UPDATING" state

**Common causes**:
- Insufficient resources for `data_parallel_size` replicas
- Incompatible `dp_size_per_node` with available GPUs
- Network connectivity issues between nodes

**Solution**: Check Ray Dashboard resources and adjust configuration.

### Low throughput

**Symptom**: Lower than expected requests/second

**Common causes**:
- Batch size too small to saturate MoE layers
- Ingress bottleneck (too few ingress replicas)
- Inefficient request routing

**Solution**: Increase `max_num_seqs` and scale ingress replicas.

### High latency

**Symptom**: Increased TTFT or TPOT

**Common causes**:
- Batch size too large causing head-of-line blocking
- Memory pressure causing swapping
- Network latency between replicas

**Solution**: Reduce `max_num_seqs` or increase `data_parallel_size`.

## See also

- {doc}`../architecture/serving-patterns/data-parallel` - Architecture details
- {doc}`prefill-decode` - Combine with prefill/decode disaggregation
- {doc}`multi-lora` - Serve multiple LoRA adapters
- {doc}`observability` - Monitor and debug deployments

