(fractional-gpu-guide)=
# Fractional GPU serving

Serve multiple small models on the same GPU for cost-efficient deployments.

:::{note}
This feature hasn't been extensively tested in production. If you encounter any issues, report them on [GitHub](https://github.com/ray-project/ray/issues) with reproducible code.
:::

Fractional GPU allocation allows you to run multiple model replicas on a single GPU by customizing placement groups. This approach maximizes GPU utilization and reduces costs when serving small models that don't require a full GPU's resources.

## When to use fractional GPUs

Consider fractional GPU allocation when:

- You're serving small models with low concurrency that don't require a full GPU for model weights and KV cache.
- You have multiple models that fit this profile.

## Deploy with fractional GPU allocation

The following example shows how to serve 8 replicas of a small model on 4 L4 GPUs (2 replicas per GPU):

```python
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve.llm import build_openai_app
from ray import serve


llm_config = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id="HuggingFaceTB/SmolVLM-256M-Instruct",
    ),
    engine_kwargs=dict(
        gpu_memory_utilization=0.4,
        use_tqdm_on_load=False,
        enforce_eager=True,
        max_model_len=2048,
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=8, max_replicas=8,
        )
    ),
    accelerator_type="L4",
    placement_group_config=dict(bundles=[dict(GPU=0.49)]),
    runtime_env=dict(
        env_vars={
            "VLLM_DISABLE_COMPILE_CACHE": "1",
        },
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

## Configuration parameters

Use the following parameters to configure fractional GPU allocation. The placement group defines the GPU share, and Ray Serve infers the matching `VLLM_RAY_PER_WORKER_GPUS` value for you. The memory management and performance settings are vLLM-specific optimizations that you can adjust based on your model and workload requirements.

### Placement group configuration

- `placement_group_config`: Specifies the GPU fraction each replica uses. Set `GPU` to the fraction (for example, `0.49` for approximately half a GPU). Use slightly less than the theoretical fraction to account for system overhead—this headroom prevents out-of-memory errors.
- `VLLM_RAY_PER_WORKER_GPUS`: Ray Serve derives this from `placement_group_config` when GPU bundles are fractional. Setting it manually is allowed but not recommended.

### Memory management

- `gpu_memory_utilization`: Controls how much GPU memory vLLM pre-allocates. vLLM allocates memory based on this setting regardless of Ray's GPU scheduling. In the example, `0.4` means vLLM targets 40% of GPU memory for the model, KV cache, and CUDAGraph memory.

### Performance settings

- `enforce_eager`: Set to `True` to disable CUDA graphs and reduce memory overhead.
- `max_model_len`: Limits the maximum sequence length, reducing memory requirements.
- `use_tqdm_on_load`: Set to `False` to disable progress bars during model loading.

### Workarounds

- `VLLM_DISABLE_COMPILE_CACHE`: Set to `1` to avoid a [resource contention issue](https://github.com/vllm-project/vllm/issues/24601) among workers during torch compile caching.

## Best practices

### Calculate GPU allocation

- **Leave headroom**: Use slightly less than the theoretical fraction (for example, `0.49` instead of `0.5`) to account for system overhead.
- **Match memory to workload**: Ensure `gpu_memory_utilization` × GPU memory × number of replicas per GPU doesn't exceed total GPU memory.
- **Account for all memory**: Consider model weights, KV cache, CUDA graphs, and framework overhead.

### Optimize for your models

- **Test memory requirements**: Profile your model's actual memory usage before setting `gpu_memory_utilization`. This information often gets printed as part of the vLLM initialization.
- **Start conservative**: Begin with fewer replicas per GPU and increase gradually while monitoring memory usage.
- **Monitor OOM errors**: Watch for out-of-memory errors that indicate you need to reduce replicas or lower `gpu_memory_utilization`.

### Production considerations

- **Validate performance**: Test throughput and latency with your actual workload before production deployment.
- **Consider autoscaling carefully**: Fractional GPU deployments work best with fixed replica counts rather than autoscaling.

## Troubleshooting

### Out of memory errors

- Reduce `gpu_memory_utilization` (for example, from `0.4` to `0.3`)
- Decrease the number of replicas per GPU
- Lower `max_model_len` to reduce KV cache size
- Enable `enforce_eager=True` if not already set to ensure CUDA graph memory requirements don't cause issues

### Replicas fail to start

- Verify that your fractional allocation matches your replica count (for example, 2 replicas with `GPU=0.49` each)
- Confirm that `placement_group_config` matches the share you expect Ray to reserve
- If you override `VLLM_RAY_PER_WORKER_GPUS` (not recommended) ensure it matches the GPU share from the placement group
- Ensure your model size is appropriate for fractional GPU allocation

### Resource contention issues

- Ensure `VLLM_DISABLE_COMPILE_CACHE=1` is set to avoid torch compile caching conflicts
- Check Ray logs for resource allocation errors
- Verify placement group configuration is applied correctly

## See also

- {doc}`Quickstart <../quick-start>` - Basic LLM deployment examples
- [Ray placement groups](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) - Ray Core placement group documentation

