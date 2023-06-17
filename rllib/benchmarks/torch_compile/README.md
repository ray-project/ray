# Torch 2.0 Compile benchmarks on RLlib 

Torch 2.0 comes with the new `torch.compile()` [API](https://pytorch.org/docs/stable/generated/torch.compile.html#torch.compile), which leverages [torch dynamo](https://pytorch.org/docs/stable/dynamo/index.html#torchdynamo-overview) under the hood to JIT-compile wrapped code. We integrate `torch.compile()` with RLlib in the context of [RLModules](https://docs.ray.io/en/latest/rllib/rllib-rlmodule.html) and Learners. 

We have integrated this feature with RLModules. You can set the backend and mode via `framework()` API on `AlgorithmConfig` object. 

```
config.framework(
    "torch",
    torch_compile_worker=True,
    torch_compile_worker_dynamo_backend="ipex"
    torch_compile_worker_dynamo_mode="default",
)
```

# Benchmarks

We conducted a comperhensive benchmark with this feature. 

## Inference
Here is the benchmarking code: [./run_inference_bm.py](./run_inference_bm.py). You can run the benchmark yourself as well:

```bash
./run_all_inference_bms.sh -bs <batch_size> --backend <dynamo_backend> --mode <dynamo_mode>
```

For the benchmarking metric, we compute the inverse of the time it takes to run `forward_exploration()` of the RLModule. We have conducted this benchmark on the default implementation of PPO RLModule under different hardware settings, torch versions, dynamo backends and modes, as well as different batch sizes. Here is a high-level summary of our findings:

| Hardware | PyTorch Version | Speedup (%) | Backend + Mode           |
|----------|----------------|-------------|--------------------------|
| CPU      | 2.0.1          | 33.92       | ipex + default           |
| CPU      | 2.1.0 nightly  | x           | ipex + default           |
| T4       | 2.0.1          | 14.05       | inductor + reduce-overhead|
| T4       | 2.1.0 nightly  | 15.01       | inductor + reduce-overhead|
| V100     | 2.0.1          | 92.43       | inductor + reduce-overhead|
| V100     | 2.1.0 nightly  | 85.71       | inductor + reduce-overhead|
| A100     | 2.0.1          | x           | inductor + reduce-overhead|
| A100     | 2.1.0 nightly  | 156.66      | inductor + reduce-overhead|


For detailed benchmarks checkout this PR.
