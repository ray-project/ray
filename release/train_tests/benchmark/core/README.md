# Ray Train Benchmark Harness

A config-driven harness for benchmarking Ray Train workloads. One **experiment**
(a YAML file under `experiments/`) defines a full benchmark run; adding a new
case is normally a single new YAML unless a new framework is involved.

## Layout

```
core/
  experiment_config.py   ExperimentConfig schema + YAML loader (with --set overrides)
  metrics.py             FLOPs/MFU + bandwidth tables, GPU/memory sampler, throughput collector
  train_context.py       launcher-agnostic worker context (Ray Train | torchrun)
  registry.py            adapter name -> FrameworkAdapter class
  runner.py              entrypoint: load YAML, dispatch to launcher
  sinks.py               write results (release-test json + local mirror)
  launchers/
    ray_launcher.py            Ray Train TorchTrainer wiring
    torchrun_launcher.py       runs inside a torchrun-spawned process (env:// rendezvous)
    torchrun_ray_launcher.py   torch.distributed placed by Ray actors (no ssh needed)
  prepare.py             prefetch model + dataset into a shared HF cache
frameworks/
  base_adapter.py        FrameworkAdapter ABC
  deepspeed/adapter.py   DeepSpeed ZeRO LLM adapter
data/
  text_dataset.py        shared causal-LM dataloader (HF datasets + synthetic)
experiments/
  qwen3_06b_deepspeed.yaml
  qwen3_06b_deepspeed_smoke.yaml
collect.py               render result JSONs into an llm-foundry-style table
```

## Running

```bash
cd release/train_tests/benchmark

# Ray Train (default launcher in the YAML). Run from the head node; Ray
# schedules num_workers across the cluster's GPU nodes (single controller).
python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml

# Smoke test on a single GPU with synthetic data (no dataset download)
python -m core.runner --experiment experiments/qwen3_06b_deepspeed_smoke.yaml

# Inline overrides for quick iteration
python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
    --set training.num_steps=20 data.dataset=synthetic num_workers=1
```

Ray Train v2 is the default; no `RAY_TRAIN_V2_ENABLED` env var is needed.

### Prepare data once (recommended)

Prefetch the model + dataset into a shared HF cache so the workers hit a warm
cache instead of each cold-fetching from the Hub (faster, and removes download
variance from the measured loop):

```bash
python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml --prepare-data
```

### Torchrun parity baseline

The point of the baseline is to run the **same adapter** under vanilla
`torch.distributed` (env:// rendezvous) so the Ray-vs-torch delta on one
experiment quantifies Ray Train's *orchestration* overhead (controller, health
checks, worker-group management, checkpoint reporting). There are two launchers
for this, since torchrun has no single controller — it runs the same script on
every process.

**`torchrun_ray` (recommended on a Ray cluster — no ssh):** Ray places one actor
per GPU (using its scheduler) and the harness sets the torch.distributed env
vars itself, then each actor runs the adapter via `init_process_group("env://")`.
You launch it the same way as the Ray run — a single submission from the head:

```bash
python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
    --set launcher=torchrun_ray
```

This holds the launch substrate (Ray) constant and isolates Ray Train's control
plane. For a *fully* Ray-free number, use real `torchrun`, launched on the GPU
node(s) — one process per GPU:

```bash
# Single GPU node: --standalone handles rendezvous automatically.
torchrun --standalone --nproc_per_node=8 -m core.runner \
    --experiment experiments/qwen3_06b_deepspeed.yaml --launcher torchrun

# Multi-node: start on EVERY node with a shared rendezvous endpoint.
torchrun --nnodes=2 --nproc_per_node=8 --node_rank=$NODE_RANK \
    --rdzv_backend=c10d --rdzv_endpoint=$HEAD_IP:29500 \
    -m core.runner --experiment <exp>.yaml --launcher torchrun
```

| Launcher | Control plane | Launch substrate | Needs node ssh? |
|---|---|---|---|
| `ray` | Ray Train controller | Ray | no |
| `torchrun_ray` | none (raw torch.distributed) | Ray actors | no |
| `torchrun` | none (raw torch.distributed) | torchrun/srun | yes (run on GPU node) |

All three collect metrics identically: rank 0 writes the results JSON to shared
storage (`/mnt/cluster_storage` when present), and `collect.py` reads it.

## Metrics collected

Beyond the legacy per-step/epoch timers and rows/sec, the harness adds the
items the proposal flagged as missing:

**Throughput / compute**
- `train/global_tokens_per_sec`, `train/tokens_per_sec_per_device`
- `train/model_tflops_per_sec_per_device` and `train/mfu` (vs the device's peak
  dense FLOP/s from `core/metrics.GPU_PEAK_FLOPS`)
- `train/step_time_{mean,p50,max}_s` (steady state)

**Memory** (torch allocator, rank-local — captures the true peak)
- `gpu/peak_memory_allocated_gb` — peak working set (catches the backward spike)
- `gpu/peak_memory_reserved_gb` — allocator footprint, closest to the OOM line
- `gpu/static_memory_gb` (model + optimizer) and `gpu/activation_memory_gb`
  (= peak − static)

**GPU / bandwidth** (NVML sampling, no-ops without a GPU)
- `gpu/utilization_mean_pct`, `gpu/utilization_max_pct`
- `gpu/memory_bw_util_{mean,max}_pct` — memory-controller active time, a coarse
  MBU proxy (time-active, *not* % of peak GB/s). High here + low MFU = the run
  is memory-bound. `gpu/peak_memory_bandwidth_gbps` records the denominator.
  True MBU (achieved GB/s ÷ peak) needs DCGM/CUPTI counters — a planned follow-up.

The NVML sampler maps the logical device to the correct *physical* GPU via
`CUDA_VISIBLE_DEVICES`, so metrics reflect the GPU the process actually uses.

Steady-state metrics exclude `training.warmup_steps` so model download,
compilation, and allocator warmup don't skew throughput. View any run with
`python collect.py`.

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
python -m pytest core/test_harness_logic.py -v   # torch-free: config, MFU/BW math, topology
```
Adapter/launcher integration runs on a GPU cluster (needs torch/ray/deepspeed).
