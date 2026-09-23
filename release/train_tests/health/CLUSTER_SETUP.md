# Minimal GPU cluster for testing the health loop

Short answer: **T4, L4 and A10G all work.** Nothing here depends on the GPU
architecture. What does constrain the setup is the NCCL version, the rank
count, and the node count — in that order.

## What actually constrains it

### 1. NCCL ≥ 2.28 on **both** sides

`ncclras -f json` needs the `-f` flag, which landed in NCCL 2.28. RAS itself is
older (2.24). Two separate things have to be new enough:

- the **`ncclras` client binary**, which only ships in the apt `libnccl2`
  package — the pip wheel contains just `libnccl.so.2` and
  `libnccl_device.bc`, no binary;
- the **library the training process actually loads**, which for a pip torch is
  the `nvidia-nccl-cu1x` wheel, not the system one.

A new client against an old in-process library is not enough. Ray's own GPU
test says so directly, and works around it with `LD_PRELOAD`:

> We need users to have both nccl 2.28.7 on the GPU process and the pytorch pip
> install. Until torch>=2.11 is installed then we need to force torch to use the
> apt install nccl version.
> — `test_nccl_ras_hang_detection.py`

That workaround has an expiry date, and it has passed: `torch==2.11.0+cu128`
pins `nvidia-nccl-cu12==2.28.9` natively. Use it and there is no `LD_PRELOAD`
to carry.

Check both:

```bash
ncclras --version                                             # the client
python -c "import torch; print(torch.cuda.nccl.version())"    # in-process
```

A client below 2.28 fails quietly: the detector runs `ncclras -f json`, gets
`invalid option -- 'f'`, raises `RASQueryError("unsupported_f_option",
fatal=True)`, logs one warning and disables itself for the rest of the run. No
RAS data, and nothing that stops the job.

### 2. Rank count: 4 is the floor

The peer-relative checks need at least three ranks to have a majority, and you
want a majority *plus* an outlier. Three ranks with one faulty leaves a 2:1
split, which is thin. **Four ranks is the working minimum**; six is more
comfortable.

A hang test needs only two ranks, but everything that attributes a fault to a
rank needs the peer group.

### 3. Node count: 2 is enough

- **1 node** — enough for the merged NCCL RAS detector and for stack dumps.
  Not enough for anything this work adds: there is no rank→host join to make.
- **2 nodes** — everything worth testing at this stage: the rank→host
  attribution, node-scoped vs worker-scoped diagnostics, and the `nvidia-smi`
  per-node dedup.

**Do not size for `EVICT` yet.** Carrying one out end to end needs the
worker-group restart path that is not built (Phases 5–6), so a spare node would
sit idle. Assert that the controller *emits* `Evict(cause=HARDWARE,
target_nodes=[...])` with the right node — that is the whole decision, and it
is checkable from a log line on two nodes. The scheduling half is already
verified separately on a 2-node CPU cluster
(`test_health_node_exclusion.py`), so a GPU adds nothing to it.

What is worth real GPU time instead is **DIAGNOSE**: the push path, the
worker-scoped stack dump, the node-scoped `nvidia-smi`, and the results coming
back as evidence. None of that can be faked convincingly.

## VM or Kubernetes?

**VM.** Nothing in the NCCL RAS path touches Kubernetes: RAS is a mesh of
monitoring threads inside the NCCL processes, `ncclras` runs as a subprocess
*inside the worker* that Ray already has an actor handle to, and the mesh
reaches across nodes over the same network the collectives already use. If NCCL
works, RAS works.

Kubernetes only becomes necessary for the NVSentinel half — node conditions,
taints, cordons — which is deferred. When that time comes, NVSentinel's own
kind-based demo on a laptop is a better first contact than the real cluster.

The one thing worth checking on whichever backend you pick is `SYS_PTRACE`,
because `py-spy` needs it and the two backends grant capabilities differently
(a pod `securityContext` versus whatever the VM runtime sets). Ubuntu defaults
to `ptrace_scope=1`, which lets a process trace only its descendants — and
`py-spy` is spawned as a *child* of the worker it then has to trace, which is
the wrong direction. Without the capability the stack dump degrades to a
Python-only traceback, losing exactly the native frames a NCCL hang lives in.

Check it on a worker before concluding the dumps are broken:

```bash
capsh --print | grep -i ptrace
cat /proc/sys/kernel/yama/ptrace_scope
sleep 300 & py-spy dump --pid $! --native   # the real test
```

## Recommended shape

**2 × g4dn.12xlarge** (4× T4 each) = 8 ranks on 2 nodes, ~$7.8/hr on-demand and
far less on spot.

One shape covers everything:

| what it exercises | why this shape |
|---|---|
| peer-relative checks | 8 ranks, well past the 3-rank floor |
| rank→host attribution | 2 distinct hosts |
| worker-scoped diagnostics | 8 stack dumps, one per rank |
| node-scoped `nvidia-smi` dedup | 4 ranks share each host, so the "one snapshot per node, not per rank" branch actually runs |

AWS only offers 1, 4 or 8 GPUs per node in the g-families, so there is no
cheaper 2-GPU-per-node option. If cost matters more than the dedup branch,
**4 × g4dn.xlarge** (1× T4, ~$0.53/hr each) gives 4 ranks on 4 hosts for ~$2/hr
— everything except the shared-host path.

`g5` (A10G) and `g6` (L4) work identically at roughly twice the price.

## GPU type: why it does not matter

| | T4 | A10G | L4 |
|---|---|---|---|
| arch | Turing sm_75 | Ampere sm_86 | Ada sm_89 |
| NVLink | no | no | no |
| works for this | yes | yes | yes |

Nothing under test is architecture-dependent: NCCL RAS is a mesh of monitoring
threads, `nvidia-smi -q` is universal, and NVRx's `GPU_SLEEP` is a spin kernel.
The absence of NVLink on all three only means you cannot reproduce a real
NVLink fault — which you would inject through DCGM anyway, not by breaking one.

One caveat on T4: sm_75 is the floor for CUDA 13, so it is supported but it is
the oldest thing that is. If the image moves to CUDA 13 and something is odd,
try A10G before assuming the code is wrong.

## What to run, in order

### Step 0 — no GPU needed, run this first

```bash
pytest python/ray/train/v2/tests/test_health*.py
python -m ray.train.v2._internal.execution.health.examples.injected_faults_demo
```

146 tests. If these fail, a GPU will not help.

### Step 1 — confirm RAS produces data at all (1 node, 2 GPUs)

Before testing any detector, confirm the substrate works:

```bash
# during a running multi-GPU job, on a worker:
ncclras -f json -t 5 | head -40
```

**Capture that output and keep it.** The port's parsing is reused from the
merged callback, but every fixture in our tests was hand-written. A real report
from your cluster, pinned as a fixture, is worth more than another unit test.
Do the same for `nvidia-smi -q` — `_parse_nvidia_smi` was written against
invented text and is the least trustworthy thing in the prototype.

### Step 2 — the merged detector, on an injected hang (2 nodes)

This needs no new code — it is shipped:

```bash
export NCCL_RAS_ENABLE=1                         # what Ray's GPU test sets
export RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1
export RAY_TRAIN_NCCL_RAS_ACTION=observe         # `fail` once you trust it
export RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S=2  # CI's values: confirms in ~6s
export RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S=4   # vs the 10 min default
```

`RAY_TRAIN_NCCLRAS_PATH` overrides where the client is found, if you end up
with more than one.

and in the training function:

```python
from ray.train.v2._internal.execution.health.testing import nvrx
nvrx.inject(nvrx.GPU_SLEEP, delay_s=120, keep_alive=1, seed=7)
```

`keep_alive=1` keeps rank 0 healthy so there is a peer to compare against.
Expect: op counts freeze on the wedged communicator, the detector warns at 60 s
and confirms at the configured duration.

Record **injection → first warning** and **injection → confirmation**. Those two
numbers are the whole goodput argument, and they are what the REP's release
test has to assert.

### Step 3 — the port, against the same job

Feed `RASPoller.next_result` into `NcclRasProbe` and check both reach the same
verdict on the same hang. That is the migration's acceptance criterion: same
detections, same timings.

### Step 4 — `DIAGNOSE`, the push path (2 nodes)

The part that needs real hardware. On a confirmed hang, check that:

- the stack dump runs on **every rank** and the native frames are present
  (needs `SYS_PTRACE`, see below);
- `nvidia-smi -q` runs **once per node, not once per rank** — with 4 ranks per
  host, a per-rank fan-out would produce four identical 40 KB reports;
- both land in run storage, and the paths come back in
  `ProbeResult.artifacts`;
- the results appear in the next `HealthState`, and the follow-up decision
  reads them.

Then assert the `Evict` decision is *emitted* with the right node. Stop there.

## Image dependencies

`ray[train]` + `torch` is very nearly all of it. The health package is stdlib
only; every third-party import in it is lazy and optional.

What an Anyscale base is missing is NCCL 2.28 on both sides:
`anyscale/ray:nightly-py312-cu128` ships `ncclras` **2.25.1**, and an
unpinned `pip install torch` pulls a CUDA 13 build whose stack will not match a
`cu128` driver.

### The cu128 Dockerfile

Verified against real Anyscale builds.

```dockerfile
FROM anyscale/ray:nightly-py312-cu128

# 1. Your Ray build.
RUN pip uninstall -y ray && \
    pip install <your-ray-wheel-url> && \
    python -c "import ray"

# 2. torch 2.11+cu128 pins nvidia-nccl-cu12==2.28.9 in-process, which is what
#    removes the LD_PRELOAD dance Ray's own GPU test still carries for 2.9/2.10.
RUN pip install "torch==2.11.0+cu128" \
      --index-url https://download.pytorch.org/whl/cu128

# 3. The ncclras client. It lives in libnccl-dev, is an 18 KB binary, and links
#    only libc -- it talks to the in-process RAS threads over a socket, so it
#    needs no NCCL library of its own. Lift just the binary out of the .deb
#    rather than installing 374 MB of packages we already have via the wheel.
ARG NCCL_VER=2.28.9-1+cuda12.9
ARG NCCL_REPO=https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64
RUN cd /tmp && \
    wget -q "${NCCL_REPO}/libnccl-dev_${NCCL_VER}_amd64.deb" && \
    dpkg-deb --fsys-tarfile "libnccl-dev_${NCCL_VER}_amd64.deb" \
      | sudo tar -xf - -C / ./usr/bin/ncclras && \
    rm -f /tmp/libnccl-dev_*.deb && \
    command -v ncclras

# 4. Fail the build, not the run. Two subtleties: `&&` not `;` (with `;` only
#    the last command's exit code counts, so the check silently passes), and
#    `2>&1` because `ncclras --version` writes to stderr, which a bare pipe
#    would not carry.
RUN ncclras --version 2>&1 | grep -E '2\.(2[89]|3[0-9])\.' && \
    python -c "import torch; v=torch.cuda.nccl.version(); assert v[:2]>=(2,28), v; print('in-process NCCL', v)"
```

Both sides land on **2.28.9**: the extracted client and the wheel torch loads.
No `LD_PRELOAD`, no version skew, no apt.

Swap `ubuntu2204` for `ubuntu2404` if `cat /etc/os-release` says 24.04 — both
carry the same versions.

#### Why not apt

Three apt attempts failed on an Anyscale base, each for a different reason, and
the fourth was not worth chasing:

1. **Permission denied** on `/var/lib/apt/lists` — Anyscale images run as the
   unprivileged `ray` user. Fixed with `sudo` (Ray's base creates `ray` with
   passwordless sudo and uses `sudo apt-get` itself, so this beats
   `USER root` / `USER ray`: it works whether or not the builder honors `USER`,
   and cannot leave the image running as root).
2. **`Version '2.28.9-1+cuda12.9' for 'libnccl2' was not found`** — the base has
   `ncclras` 2.25.1 installed but not NVIDIA's apt repo.
3. **Same error after installing `cuda-keyring_1.1-1_all.deb`**, which does ship
   `/etc/apt/sources.list.d/cuda-ubuntu2204-x86_64.list`. The `apt-get update`
   that followed fetched only `archive.ubuntu.com`. The Anyscale builder runs an
   apt cache (`buildcache:apt_…` in the build log), which is the likeliest
   culprit, but it is someone else's infrastructure.

Extracting the binary sidesteps all of it. If you do want the apt route later,
this is the diagnostic:

```bash
sudo apt-get update
grep -r . /etc/apt/sources.list.d/ | grep -i nvidia
apt-cache policy libnccl2
```

#### What is actually in those packages

Worth knowing, because the naming is misleading:

| package | contents | size |
|---|---|---|
| `libnccl2` | `libnccl.so.2.28.9` and its symlink — **no binaries** | 189 MB |
| `libnccl-dev` | `/usr/bin/ncclras`, headers, static lib | 185 MB |

So the client you need is in the **`-dev`** package, and the runtime package you
would instinctively reach for does not contain it. The pip wheel contains the
mirror image: `libnccl.so.2` and no binary.

`py-spy` is already in the Anyscale image (0.4.2), so it needs no line — but it
does need `SYS_PTRACE` at runtime (see below).


### If you must stay on torch 2.9/2.10

Those pin `nvidia-nccl-cu12==2.27.5`, whose in-process RAS predates the JSON
format. Force the workers onto the apt copy — NCCL 2.x is ABI-stable, so a
newer library under an older torch is fine:

```python
runtime_env = {
    "env_vars": {"LD_PRELOAD": "/usr/lib/x86_64-linux-gnu/libnccl.so.2"}
}
```

It has to be on the **workers**, not just the driver — they are the processes
running NCCL. This is exactly what `test_nccl_ras_hang_detection.py` does, and
what upgrading to 2.11 lets you delete.

### Not required, despite appearances

| package | why you can skip it |
|---|---|
| `nvidia-resiliency-ext` | see below — and you do not need it |
| `kubernetes` | only the NVSentinel inbound probe imports it |
| `grpcio` + NVSentinel protos | only the NVSentinel outbound sink |
| DCGM / `datacenter-gpu-manager` | `NvidiaSmiProbe` shells out to `nvidia-smi`, which is always there. DCGM is only for NVSentinel. |

### NVRx will not install on Ray's GPU image, and you do not need it

`nvidia-resiliency-ext` 0.7.0 ships wheels only for **cp312 / cp314** on
**manylinux_2_39** (glibc ≥ 2.39, i.e. Ubuntu 24.04). Python 3.12 is fine on the
Anyscale image, but a 22.04 base is not — and Ray's own GPU base
(`ubuntu22.04`, Python 3.10) misses on both counts. Either way pip finds no
wheel and falls back to building C extensions from source.

Not worth fighting. The two faults you actually need are three lines each, and
both ship in the harness with no dependency beyond torch:

```python
from ray.train.v2._internal.execution.health.testing import symptoms

# wedge the CUDA stream — what NVRx's GPU_SLEEP does
hang = symptoms.CudaHang(rank=3, start_step=50)
...
hang.fire(step)

# or: one rank skips a collective. The purest RAS input — its op count
# falls one behind while every peer blocks inside the all-reduce.
desync = symptoms.CollectiveDesync(rank=3, start_step=50)
...
if not desync.maybe_skip(step):
    dist.all_reduce(grads)
```

Prefer `CollectiveDesync` for testing the RAS detector: it produces the exact
shape `mismatched_comms` keys on — counts differ, every rank still `RUNNING`.

One tidy coincidence worth noting: NVRx's own support matrix requires
**NCCL < 2.28.3 or >= 2.28.9** (2.28.3–2.28.8 have an in-process bug). Both
recipes above satisfy it. If you later run NVRx on a 24.04 / py3.12 image,
nothing needs to change.

## Two things that will bite

**`py-spy` needs `SYS_PTRACE`.** Without the capability in the container, the
stack dump silently degrades to a Python-only traceback — which is handled, but
it means the native C++ frames where a NCCL hang actually lives are missing.
Check `capsh --print | grep ptrace` on a worker before concluding the dumps are
useless.

**DCGM is not needed yet.** `NvidiaSmiProbe` shells out to `nvidia-smi`, which
is always present. DCGM only matters for the NVSentinel path, and skipping
NVSentinel for now is the right call — it needs a Kubernetes install (MongoDB or
Postgres, the operator, the health monitors) that buys nothing until the
inbound adapter is wired. When you do want it, NVSentinel's own kind-based
`demos/local-fault-injection-demo` runs on a laptop with no GPU at all, which
is a better first contact than putting it on the real cluster.
