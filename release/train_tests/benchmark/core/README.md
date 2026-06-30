# Ray Train Benchmark Harness

A config-driven harness for benchmarking Ray Train workloads. One **experiment**
(a YAML file under `experiments/`) defines a full benchmark run; adding a new
case is normally a single new YAML unless a new framework is involved.

## Layout

```
core/
  experiment_config.py   ExperimentConfig schema + YAML loader (with --set overrides)
  metrics.py             FLOPs/MFU + bandwidth tables; TrainMetricsCollector (+GPU subclass)
  train_context.py       launcher-agnostic worker context (Ray Train | torchrun_ray)
  registry.py            adapter name -> FrameworkAdapter class
  runner.py              entrypoint: load YAML, dispatch to launcher
  launchers/
    ray_launcher.py            Ray Train TorchTrainer wiring
    torchrun_ray_launcher.py   vanilla torch.distributed placed by Ray actors (baseline)
    ray_actor_utils.py         placement / rendezvous helpers for torchrun_ray
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
    --set training.num_steps=20 data.dataset=synthetic scaling.num_workers=1
```

Ray Train v2 is the default; no `RAY_TRAIN_V2_ENABLED` env var is needed.
Model/dataset download happens before the timed loop (and warmup steps are
excluded from steady-state metrics), so it doesn't affect throughput/MFU. Set
`HF_TOKEN` as a cluster env var if you hit Hub rate limits.

### Torchrun parity baseline (`torchrun_ray`)

The baseline runs the **same adapter** under vanilla `torch.distributed`
(`init_process_group("env://")`), so the Ray-vs-torch delta on one experiment
quantifies Ray Train's *orchestration* overhead (controller, health checks,
worker-group management, checkpoint reporting).

Ray places one actor per GPU (using its scheduler), the harness sets the
torch.distributed env vars (rank/world_size/master) itself, and each actor runs
the adapter. This is exactly how the legacy `air_benchmarks` ran "vanilla
torch" — Ray actors stand up the process group, no ssh/srun needed — so it's the
single baseline we keep. Launch it like the Ray run, from the head:

```bash
python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
    --set launcher=torchrun_ray
```

| Launcher | Control plane | Launch substrate | Needs node ssh? |
|---|---|---|---|
| `ray` | Ray Train controller | Ray | no |
| `torchrun_ray` | none (raw torch.distributed) | Ray actors | no |

Both collect metrics identically: rank 0 writes the results JSON to shared
storage (`/mnt/cluster_storage` when present), and `collect.py` reads it. (A
*fully* Ray-free run would need real `torchrun` via ssh/srun on the GPU nodes;
we don't keep that variant — `torchrun_ray` already isolates the Train control
plane, which is the comparison that matters.)

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

**GPU / bandwidth** (NVML sampling, no-ops without a GPU). These live in
`GpuTrainMetricsCollector` (a subclass of the device-agnostic
`TrainMetricsCollector`), so a future `TpuTrainMetricsCollector` can parallel it.
- `gpu/utilization_mean_pct`, `gpu/utilization_max_pct` — nvidia-smi "GPU-Util":
  the % of time ≥1 kernel was executing. A *busy* signal, **not** compute
  efficiency (that's MFU) and not memory. So high util + low MFU = busy but
  inefficient (memory-bound).
- `gpu/memory_bw_util_{mean,max}_pct` — memory-controller active time, a coarse
  MBU proxy (time-active, *not* % of peak GB/s). High here + low MFU = the run
  is memory-bound. `gpu/peak_memory_bandwidth_gbps` records the denominator.
  True MBU (achieved GB/s ÷ peak) needs DCGM/CUPTI counters — a planned follow-up.

The NVML sampler maps the logical device to the correct *physical* GPU via
`CUDA_VISIBLE_DEVICES`, so metrics reflect the GPU the process actually uses.

Steady-state metrics exclude `training.warmup_steps` so model download,
compilation, and allocator warmup don't skew throughput. View any run with
`python collect.py`.

## MFU / FLOPs accounting (dense + MoE)

`core/metrics.flops_per_token(FlopsSpec)` computes train FLOPs/token as
`6·N_active + (attention term)`:

- **Param term** `6·N_active` — forward 2N + backward 4N. For **dense**,
  `N_active = total params`. For **MoE**, `N_active = non-expert params +
  (top_k / num_experts)·routed-expert params` (+ always-on shared experts).
  Total experts never enter the count. This matches Megatron-LM and llm-foundry;
  HF Trainer (total params, no attention) is deliberately *not* followed.
- **Attention term**, picked automatically from the HF config:
  - `quadratic` → `12·L·hidden·seq` (standard softmax attention).
  - `linear` → omitted (Gated DeltaNet / SSM / RWKV are O(seq); conservative
    underestimate, logged as such).

The adapter derives `N_active` by counting expert tensors on the loaded model
and the attention kind from `config.model_type` / layer types — no per-model
hardcoding. Results carry `config/model_kind` (dense|moe), `active_params`, and
`config/attention_flops` so the table can treat dense vs MoE as its own axis.
Peak FLOP/s uses dense (de-sparsified) values from `GPU_PEAK_FLOPS`, as Composer
does, keeping MFU comparable to llm-foundry and NeMo.

## Adding a workload

1. New case, existing framework → add a YAML to `experiments/`.
2. New framework → add `frameworks/<name>/adapter.py` implementing
   `FrameworkAdapter`, register it in `core/registry.py`, then add the YAML.

## Tests

```bash
python -m pytest core/test_harness_logic.py -v   # torch-free: config, MFU/BW math, topology
```
Adapter/launcher integration runs on a GPU cluster (needs torch/ray/deepspeed).
