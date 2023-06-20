# Torch 2.0 Compile benchmarks on RLlib 

Torch 2.0 comes with the new `torch.compile()` [API](https://pytorch.org/docs/stable/generated/torch.compile.html#torch.compile), which leverages [torch dynamo](https://pytorch.org/docs/stable/dynamo/index.html#torchdynamo-overview) under the hood to JIT-compile wrapped code. We integrate `torch.compile()` with RLlib in the context of [RLModules](https://docs.ray.io/en/latest/rllib/rllib-rlmodule.html) and Learners. 

We have integrated this feature with RLModules. You can set the backend and mode via `framework()` API on an `AlgorithmConfig` object. Alternatively, you can compile the `RLModule` directly during stand-alone usage such as inference.


# Benchmarks

We conducted a comperhensive benchmark with this feature. 

## Inference
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


For detailed benchmarks, checkout [this google doc](https://docs.google.com/spreadsheets/d/1O7_vfGRLV7JfsClXO6stTg8snxghDRRHYBrR3f47T94/edit#gid=0). Here is the benchmarking code: [./run_inference_bm.py](./run_inference_bm.py). You can run the benchmark yourself as well:

```bash
./run_all_inference_bms.sh -bs <batch_size> --backend <dynamo_backend> --mode <dynamo_mode>
```

### Some meta level comments
1. The performance improvement depends on many factors, including the neural network architecture used, the batch size during sampling, the backend, the mode, the torch version, and many other things. The best way to optimize this is to first get the non-compiled workload learning and then do a hyper-parameter tuning on torch compile parameters on different hardware.

2. For CPU inference use the recommended inference only backends: `ipex` and `onnxrt`.

3. The speedups are more significant on more modern architectures such as A100s compared to older ones like T4.

4. Torch compile is still evolving. We noticed significant differences between the 2.0.1 release and the 2.1 nightly release. Therefore, it is important to take this into account during benchmarking your own workloads.


## Exploration

In RLlib, you can now set the configuration so that the compiled module is used during sampling of an RL agent training process. By default, the rollout workers run on CPU, therefore it is recommended to use the `ipex` or `onnxrt` backend. Having said that, you can still choose to run the sampling part on GPUs as well by setting `num_gpus_per_worker` in which case other backends can be used as well.


```
config.framework(
    "torch",
    torch_compile_worker=True,
    torch_compile_worker_dynamo_backend="ipex"
    torch_compile_worker_dynamo_mode="default",
)
```

This benchmark script runs PPO algorithm with the default model architecture for Atari-Breakout game. It will run the training for `n` iterations for both compiled and non-compiled RLModules and reports the speedup. Note that negative speedup values mean a slowdown when you compile the module. 

To run the benchmark script, you need a ray cluster comprised of at least 129 CPUs (2x64 + 1) and 2 GPUs. If this is not accessible to you, you can change the number of sampling workers and batch size to make the requirements smaller.

```
python ./run_ppo_with_inference_bm.py --backend <backend> --mode <mode>
```

Here is a summary of results:

| Backend | Mode | Speedup (%) |
|---------|------|-------------|
| onnxrt | default | -72.34 |
| onnxrt | reduce-overhead | -72.72 |
| ipex | default | 11.71 |
| ipex | reduce-overhead | 11.31 |
| ipex | max-autotune | 12.88 |


As you can see, `onnxrt` does not gain any speed-ups in the setup we tested (in fact it slows the workload down by %70) while the `ipex` provides ~%10 speed-up. If we change the model architecture, these numbers may change. So it is very important to fix the architecture first and then search for the fastest training settings. 