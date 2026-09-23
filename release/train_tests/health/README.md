# Ray Train health loop — GPU tests

Scripts for validating the Collect → Decide → Act work on a real cluster.
The library code lives in `python/ray/train/v2/_internal/execution/health/`;
only the things you *run* live here.

## Getting your changes onto the cluster

The image installs a Ray wheel, so `import ray.train` resolves to the wheel,
not your checkout. Rebuilding a wheel for every edit is not the loop you want.
Instead, symlink the installed `ray/train` to your working tree once:

```bash
git pull && git checkout <your-branch>
python python/ray/setup-dev.py -y --allow train
```

Now every edit under `python/ray/train/` takes effect on the next `ray.init()`,
with no rebuild. Ray Core changes still need a new wheel — this only covers
pure Python, which is all of the health package.

Verify it took:

```bash
python -c "import ray.train, os; print(os.path.realpath(ray.train.__file__))"
```

It should print a path inside your checkout.

> On a multi-node cluster the symlink exists only on the head node. Ray ships
> the worker code from the driver's runtime env, so this is enough for the
> controller and for anything the driver imports — but a change that has to run
> *inside a training worker* needs either a new wheel or
> `runtime_env={"py_modules": [...]}`.

## The steps

Run them in order. Each one fails fast if the previous was not really passing.

```bash
python release/train_tests/health/00_preflight.py        # ~20s, no GPU work
python release/train_tests/health/01_nccl_ras.py         # is RAS producing data?
python release/train_tests/health/02_injected_fault.py   # does anything react?
python release/train_tests/health/03_collective_join.py  # does it react for the right reason?
```

### 00_preflight.py

Checks, on every GPU node: `ncclras` ≥ 2.28, in-process NCCL ≥ 2.28, torch and
CUDA, and whether `py-spy` can actually attach (it tests a real non-descendant
dump, which is what `ptrace_scope=1` blocks). Exits non-zero on anything
blocking. Warns, rather than fails, on `py-spy` — stack dumps degrade to
Python-only, they do not stop working.

### 01_nccl_ras.py

Starts a real all-reduce job and queries `ncclras` on every GPU node while it
runs, saving `ncclras -f json`, `ncclras -f text` and `nvidia-smi -q` per node.

```bash
python release/train_tests/health/01_nccl_ras.py            # healthy job
python release/train_tests/health/01_nccl_ras.py --hang     # one rank wedged
```

**Keep the output.** Every RAS and `nvidia-smi` fixture in our unit tests is
hand-written; `_parse_nvidia_smi` in particular was written against invented
text and is the least trustworthy thing in the prototype. One real capture from
your hardware is worth more than another unit test. The `--hang` capture is the
more valuable of the two — that is the input the detector actually parses.

A bare `ncclras` on an idle node will always fail with *"Connection refused …
Failed to connect to the NCCL RAS service"*. That is correct: RAS is not a
daemon, its threads live inside the NCCL processes, so the service exists only
while a job is running and only on nodes hosting a rank.

### 02_injected_fault.py

Injects a real fault — one rank stops calling the collective while staying
alive, so its op count falls a step behind and every peer blocks inside the
all-reduce — and checks that something reacts.

```bash
python release/train_tests/health/02_injected_fault.py            # A
python release/train_tests/health/02_injected_fault.py --ported   # B
```

**Part A** exercises the merged detector (#64928) with no new code: it should
raise `NCCLHangError`, which the failure policy turns into a retry. Run this
first — if it does not fire, the problem is the cluster, not our change.

**Part B** feeds the same RAS reports through `NcclRasProbe` /
`NcclHangEvaluator` and prints the `HealthDecision`. This is the migration's
acceptance criterion: same detection, same timing, better action.

Both print the two numbers that matter: injection → detection, and detection →
decision. Those are the goodput claim, and the REP's release test has to assert
them.

Useful knobs: `--workers`, `--hang-rank` (keep it non-zero so a healthy peer
survives), `--hang-step`, `--confirm-s`.

### 03_collective_join.py

The part a RAS-only detector cannot do. The job builds real NCCL subgroups --
TP groups all-reduced every step, "PP" groups only every `--pp-every` steps --
so for most of the run the PP communicators genuinely have frozen op counts
while the job is healthy. That is the false positive a fixed-timeout detector
produces, reproduced rather than simulated.

```bash
python release/train_tests/health/03_collective_join.py --no-hang   # phase 1 only
python release/train_tests/health/03_collective_join.py             # both phases
```

Pass means: silence while the PP groups sit frozen, then a decision within
seconds of the TP wedge. A fire during phase 1 is a real false positive and the
script fails on it.

Needs an even worker count (tp=2) and at least 4 ranks.

## The no-GPU demos

These need no cluster and run in seconds. Worth a look before the GPU work, to
see what the decisions are supposed to look like.

```bash
python release/train_tests/health/injected_faults_demo.py --verbose
python release/train_tests/health/diagnose_flow_demo.py
python release/train_tests/health/why_in_ray_demo.py
```

And the unit tests, which need nothing at all:

```bash
pytest python/ray/train/v2/tests/test_health*.py
```

## Reference

- `CLUSTER_SETUP.md` — cluster shape, GPU types, and the Dockerfile, with what
  each non-obvious line is for and which failed build earned it.
- `INJECTION.md` — the three fault-injection layers and what each one is good
  for.
