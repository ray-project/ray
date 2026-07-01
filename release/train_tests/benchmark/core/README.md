# Ray Train Benchmark Harness

A config-driven harness for benchmarking Ray Train workloads. One **experiment**
(a YAML file under `experiments/`) defines a full benchmark run; adding a new
case is normally a single new YAML unless a new framework is involved.

## Layout

```
core/
  experiment_config.py   ExperimentConfig schema + YAML loader (with --set overrides)
  metrics.py             FLOPs/MFU tables, GPU utilization sampler, throughput collector
  train_context.py       launcher-agnostic worker context (Ray Train | torchrun)
  registry.py            adapter name -> FrameworkAdapter class
  runner.py              entrypoint: load YAML, dispatch to launcher
  sinks.py               write results (release-test json + local mirror)
  launchers/
    ray_launcher.py      TorchTrainer wiring
    torchrun_launcher.py parity baseline
frameworks/
  base_adapter.py        FrameworkAdapter ABC
  deepspeed/adapter.py   DeepSpeed ZeRO LLM adapter
data/
  text_dataset.py        shared causal-LM dataloader (HF datasets + synthetic)
experiments/
  qwen3_06b_deepspeed.yaml
  qwen3_06b_deepspeed_smoke.yaml
```

## Running

```bash
cd release/train_tests/benchmark

# Ray Train (default launcher in the YAML)
RAY_TRAIN_V2_ENABLED=1 python -m core.runner \
    --experiment experiments/qwen3_06b_deepspeed.yaml

# Smoke test on a single GPU with synthetic data (no dataset download)
RAY_TRAIN_V2_ENABLED=1 python -m core.runner \
    --experiment experiments/qwen3_06b_deepspeed_smoke.yaml

# Torchrun parity baseline (no Ray in the loop)
torchrun --nproc_per_node=2 -m core.runner \
    --experiment experiments/qwen3_06b_deepspeed.yaml --launcher torchrun

# Inline overrides for quick iteration
python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
    --set training.num_steps=20 data.dataset=synthetic num_workers=1
```

## Metrics collected

Beyond the legacy per-step/epoch timers and rows/sec, the harness adds the
items the proposal flagged as missing:

- `train/global_tokens_per_sec`, `train/tokens_per_sec_per_device`
- `train/model_tflops_per_sec_per_device` and `train/mfu` (vs the device's peak
  dense FLOP/s from `core/metrics.GPU_PEAK_FLOPS`)
- `gpu/utilization_mean_pct`, `gpu/utilization_max_pct`, `gpu/memory_used_gb_max`
  (NVML sampling, no-ops without a GPU)

Steady-state metrics exclude `training.warmup_steps` so model download,
compilation, and allocator warmup don't skew throughput.

## MFU accounting

`transformer_flops_per_token` uses the standard `6N + 12·L·d·T` approximation
(forward 2N + backward 4N, plus attention score/value matmuls), matching PaLM
Appendix B and llm-foundry's tables so numbers are comparable across reports.

## Adding a workload

1. New case, existing framework → add a YAML to `experiments/`.
2. New framework → add `frameworks/<name>/adapter.py` implementing
   `FrameworkAdapter`, register it in `core/registry.py`, then add the YAML.

## Tests

```bash
python -m pytest core/test_harness_logic.py -v   # torch-free: config + MFU math
```
Adapter/launcher integration runs on a GPU cluster (needs torch/ray/deepspeed).
