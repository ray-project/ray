# How to actually inject these faults

The objection that matters: "nice detector, but can you test it?" There are
three independent layers to inject at, and most of what we need is at the top
one, which needs no special hardware at all.

## Layer 1 — the training process (no GPU needed for most of it)

[NVRx](https://github.com/NVIDIA/nvidia-resiliency-ext)'s
`shared_utils/inject_fault.py` is a ready-made, proven fault menu. Every entry
is software:

| `Fault` | What it does | What it looks like to us |
|---|---|---|
| `GPU_SLEEP` | `torch.cuda._sleep(1 << 62)` | wedges the stream: straggler, then hang |
| `GPU_ERROR` | out-of-bounds index → device assert | sticky CUDA error, rank dies |
| `LOCK_GIL` | catastrophic regex backtracking | **process alive, Python frozen** — `poll_status` stops answering while the node is perfectly healthy |
| `SIGSTOP` | freeze the process | same shape, no error anywhere |
| `SEGFAULT` / `OS_ABORT` / `SIGKILL` | hard death | actor loss |
| `WORKLOAD_EXC` / `ASYNC_EXC` | raise in the training loop | user-code failure |

`LOCK_GIL` and `SIGSTOP` are the two worth wiring first: they are the exact
case the `NodeMonitor` exists for. The worker cannot describe itself, the node
is fine, and nothing raises.

For everything the UDF reports — step time, phase split, grad norm, NaN — the
injection is one line in the training loop conditioned on rank. That is not a
shortcut: **a detector consumes the symptom, and the symptom is the number.**
Whether a real SDC produces that number is a hardware-research question
(NVBitFI, RTL simulation), not something a Ray Train test can or should answer.

## Layer 2 — device telemetry (needs a GPU, not a broken one)

DCGM injects synthetic field values, which is exactly what NVSentinel's GPU
health monitor reads:

```bash
dcgmi test --inject --gpuid 0 -f 230 -v 74     # DCGM_FI_DEV_XID_ERRORS
```

So XID, ECC, thermal and PCIe-replay faults **are** reproducible — you fake the
counter rather than the silicon. This is how the NVSentinel → node condition →
Ray Train path gets an end-to-end test on one real GPU.

## Layer 3 — the control plane (no GPU at all)

NVSentinel ships `demos/local-fault-injection-demo`, kind-based. It drives the
whole pipeline — health event → node condition → taint → cordon → drain — with
no GPU anywhere, which is what our inbound adapter actually consumes.

## The harness

`health/testing/` wraps all three, so a test names the fault rather than the
mechanism:

```python
from ray.train.v2._internal.execution.health.testing import nvrx, dcgm, symptoms

# layer 1 — real process fault, inside the training function
nvrx.inject(nvrx.GPU_SLEEP, delay_s=120, keep_alive=1, seed=7)

# layer 2 — real device telemetry, the call NVSentinel's demo makes
dcgm.DcgmInjector(node="nvsentinel-demo-worker").inject_fatal_gpu_fault()

# layer 3 — wait for NVSentinel to respond, and time it
from ray.train.v2._internal.execution.health.testing import nvsentinel
nvsentinel.wait_for_quarantine("nvsentinel-demo-worker", timeout_s=180)

# unit tests — the symptom, straight from the training loop
fault = symptoms.Straggler(rank=4, slowdown=2.4)
with fault.step(step):
    loss = train_step(batch)
```

`nvrx.inject` refuses loudly when NVRx is missing, and a CUDA-only fault on a
CPU runner names the GPU-free alternative (`LOCK_GIL`, `SIGSTOP`) rather than
silently doing nothing. The DCGM commands are asserted against the demo's own
call in `test_health_fault_injection.py`, so a drift on either side fails a
fast test rather than a ten-minute release test.

Run the fast half:

```bash
python -m ray.train.v2._internal.execution.health.examples.injected_faults_demo --verbose
```

Seven injected faults, seven verdicts, real numbers from real injected effects.

## What this means for the test plan

- **Unit and CI**: layer 1 plus faked probe output. Everything in
  `test_health_fault_discrimination.py` runs here, in milliseconds.
- **Single-GPU integration**: layer 2, for the DCGM → NVSentinel → probe path.
- **kind / no GPU**: layer 3, for the node-condition → `Evict` path.
- **Multi-node release test**: NVRx `GPU_SLEEP` on one rank of a real job, for
  the full hang → diagnose → evict loop. Record two numbers: injection → first
  decision, and injection → training resumed. Those are the goodput claim.

The one thing that stays genuinely un-mockable end to end is a real silent
corruption on real silicon. We test the detector against the symptom and say so.
