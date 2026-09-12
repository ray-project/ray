"""End-to-end tests for the NCCL RAS hang detector on real GPUs.

Each test deliberately induces one class of NCCL desync inside a real
``TorchTrainer`` (``backend="nccl"``, ``use_gpu=True``) with the
:class:`NCCLRASCallback` registered, and asserts the callback's whole-job
behavior: query RAS on a worker -> parse -> classify per communicator -> capture
stacks + raise :class:`NCCLHangError`.
"""
import os
import shutil
from typing import Any, Dict, Iterator

import pytest
import torch
import torch.distributed as dist

import ray
import ray.train
from ray.train import ScalingConfig
from ray.train.torch import TorchConfig, TorchTrainer, get_device
from ray.train.v2.api.exceptions import NCCLHangError, WorkerGroupError

if not torch.cuda.is_available() or torch.cuda.device_count() < 2:
    pytest.skip(
        "NCCL RAS e2e tests require >= 2 visible GPUs.", allow_module_level=True
    )
if shutil.which(os.environ.get("RAY_TRAIN_NCCLRAS_PATH", "ncclras")) is None:
    pytest.skip(
        "`ncclras` client binary not found on PATH (NCCL >= 2.28.7).",
        allow_module_level=True,
    )


# Fast detection so a hang is confirmed in seconds instead of the 10 min default.
# The confirmation window is converted to consecutive polls
# (ceil(4s / 2s) = 2 frozen polls after the first diff), so this confirms in ~6s.
RAS_ENV = {
    "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR": "1",
    "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
    "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": "2",
    "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": "4",
}


# The apt (`libnccl2`) copy of NCCL, which ships the `ncclras` client. Debug
# shim: hardcoded to the x86_64 Debian/Ubuntu location used by the CI GPU image.
# remove when torch 2.11 is merged
SYSTEM_LIBNCCL_PATH = "/usr/lib/x86_64-linux-gnu/libnccl.so.2"


# Step at which each scenario diverges, and a short loop so the non-hanging
# cases finish quickly. The hanging cases block in NCCL well before STEPS.
HANG_STEP = 3
STEPS = 16
STEP_SLEEP_S = 1.0


@pytest.fixture(scope="module", autouse=True)
def nccl_ras_env() -> Iterator[Dict[str, Any]]:
    """Speed up hang detection for this module only, then restore.

    These knobs are read by ``NCCLRASCallback`` on the driver (the callback is
    registered by default), so they only need to be present in this process.
    Only keys not already set by the caller are added (and only those removed),
    preserving any explicit override.

    We need users to have both nccl 2.28.7 on the GPU process and the pytorch
    pip install. Until torch>=2.11 is installed then we need to force torch to
    use the apt install nccl version. The yielded ``runtime_env`` preloads the
    system NCCL in the workers instead (2.x is ABI-stable, so a newer library
    under an older ``torch`` is fine).

    TODO(torch>=2.11): drop the ``LD_PRELOAD`` handling. ``torch<=2.10`` pins
    ``nvidia-nccl-cu12==2.27.5``; ``torch==2.11.0+cu128`` pins
    ``nvidia-nccl-cu12==2.28.9``, which supports the JSON format natively.

    Yields:
        Dict[str, Any]: The ``runtime_env`` for this module's ``ray.init``
        calls. Empty when the system NCCL is absent, so the wheel's copy is
        used as-is.
    """
    added = {k: v for k, v in RAS_ENV.items() if k not in os.environ}
    os.environ.update(added)

    if not os.path.exists(SYSTEM_LIBNCCL_PATH):
        runtime_env = {}
    else:
        # Preserve any caller-supplied LD_PRELOAD; runtime_env env vars replace
        # rather than extend the worker's environment.
        preload = [
            entry
            for entry in (os.environ.get("LD_PRELOAD"), SYSTEM_LIBNCCL_PATH)
            if entry
        ]
        runtime_env = {"env_vars": {"LD_PRELOAD": " ".join(preload)}}

    try:
        yield runtime_env
    finally:
        for key in added:
            os.environ.pop(key, None)


def straggler_train_fn(config):
    """A rank stops calling collectives but stays alive -> survivors wedge."""
    import time

    rank = ray.train.get_context().get_world_rank()
    device = get_device()
    tensor_shape = config["tensor_shape"]
    for step in range(STEPS):
        if step == HANG_STEP and rank == 1:
            while True:
                time.sleep(30)  # alive but never collectives again
        dist.all_reduce(torch.ones(tensor_shape, device=device))
        ray.train.report({"step": step})
        time.sleep(STEP_SLEEP_S)


def op_count_skew_train_fn(config):
    """Rank 0 issues one EXTRA unmatched all_reduce -> op-count skew."""
    import time

    rank = ray.train.get_context().get_world_rank()
    device = get_device()
    tensor_shape = config["tensor_shape"]
    for step in range(STEPS):
        dist.all_reduce(torch.ones(tensor_shape, device=device))
        ray.train.report({"step": step})
        if step == HANG_STEP and rank == 0:
            # An extra all_reduce
            dist.all_reduce(torch.ones(tensor_shape, device=device))
        time.sleep(STEP_SLEEP_S)


def collective_mismatch_train_fn(config):
    """Ranks call different collectives at the same step -> op-type mismatch."""
    import time

    rank = ray.train.get_context().get_world_rank()
    world_size = ray.train.get_context().get_world_size()
    device = get_device()
    tensor_shape = config["tensor_shape"]
    for step in range(STEPS):
        if step < HANG_STEP or rank == 0:
            dist.all_reduce(torch.ones(tensor_shape, device=device))
        else:
            out = [torch.empty(tensor_shape, device=device) for _ in range(world_size)]
            dist.all_gather(out, torch.ones(tensor_shape, device=device))
        ray.train.report({"step": step})
        time.sleep(STEP_SLEEP_S)


def shape_mismatch_train_fn(config):
    """Same collective, different tensor size per rank -> shape mismatch.

    Undetectable by RAS (op type/count are identical), so this only checks the
    callback does not spuriously fail a run.
    """
    import time

    rank = ray.train.get_context().get_world_rank()
    device = get_device()
    tensor_shape = config["tensor_shape"]
    for step in range(STEPS):
        size = 2 * tensor_shape if (step >= HANG_STEP and rank == 0) else tensor_shape
        dist.all_reduce(torch.ones(size, device=device))
        ray.train.report({"step": step})
        time.sleep(STEP_SLEEP_S)


def healthy_train_fn(config):
    """Negative control: all ranks issue matched collectives and finish cleanly.

    A correct run must complete with no error, proving the callback raises no
    spurious hang (false positive) on a healthy job. Op-counts advance in
    lock-step every step, so no rank ever looks frozen.
    """
    import time

    device = get_device()
    tensor_shape = config["tensor_shape"]
    for step in range(STEPS):
        dist.all_reduce(torch.ones(tensor_shape, device=device))
        ray.train.report({"step": step})
        time.sleep(STEP_SLEEP_S)


def dead_rank_train(config):
    """A rank hard-exits mid-collective -> dead/unresponsive rank."""
    import time

    rank = ray.train.get_context().get_world_rank()
    device = get_device()
    tensor_shape = config["tensor_shape"]
    for step in range(STEPS):
        dist.all_reduce(torch.ones(tensor_shape, device=device))
        ray.train.report({"step": step})
        if step == HANG_STEP and rank == 1:
            os._exit(1)  # die without finalizing NCCL
        time.sleep(STEP_SLEEP_S)


def multicomm_subset_train_fn(config):
    """Freeze one of two disjoint sub-communicators while the other advances.

    A single world-wide collective is issued up front, before the subgroups
    diverge, so every rank shares one communicator and the RAS monitoring
    threads form a single connected mesh. Without it the two disjoint subgroups
    form two *separate* RAS networks: a query on rank 0 only ever sees its own
    (healthy) subgroup and never the frozen one, so the hang goes undetected. The
    world comm is used only once and then sits idle at an equal op-count on every
    rank, so it never itself looks like a mismatch.
    """
    import time

    ctx = ray.train.get_context()
    rank = ctx.get_world_rank()
    world_size = ctx.get_world_size()
    device = get_device()
    tensor_shape = config["tensor_shape"]

    half = world_size // 2
    group_a_ranks = list(range(half))
    group_b_ranks = list(range(half, world_size))
    # new_group is collective over the world: every rank must call it.
    group_a = dist.new_group(group_a_ranks)
    group_b = dist.new_group(group_b_ranks)

    # Warm up the world communicator once so all ranks join one RAS mesh.
    dist.all_reduce(torch.ones(tensor_shape, device=device))

    in_a = rank in group_a_ranks
    my_group = group_a if in_a else group_b
    straggler = group_b_ranks[-1]

    for step in range(STEPS):
        if step == HANG_STEP and rank == straggler:
            while True:
                time.sleep(30)
        dist.all_reduce(torch.ones(tensor_shape, device=device), group=my_group)
        time.sleep(STEP_SLEEP_S)


def run_train_fn(train_func, num_workers, tensor_shape=64 * 1024 * 1024):
    """Run ``train_func`` with the RAS callback; return the raised error or None."""
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"tensor_shape": tensor_shape},
        torch_config=TorchConfig(backend="nccl"),
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    try:
        trainer.fit()
        return None
    except BaseException as e:  # noqa: BLE001 - return whatever fit raised
        return e


@pytest.fixture
def ray_start_4_cpus_2_gpus(nccl_ras_env):
    ray.init(num_cpus=4, num_gpus=2, runtime_env=nccl_ras_env)
    yield
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_4_gpus(nccl_ras_env):
    ray.init(num_cpus=4, num_gpus=4, runtime_env=nccl_ras_env)
    yield
    ray.shutdown()


def test_healthy_run_does_not_fail(ray_start_4_cpus_2_gpus):
    err = run_train_fn(healthy_train_fn, num_workers=2)
    assert err is None, f"healthy run must not fail, got {err!r}"


HANG, FAIL, MAYBE = "hang", "fail", "maybe"
# "hang"  -> a communicator deadlocks with a frozen straggler rank; the
#            callback must raise NCCLHangError.
# "fail"  -> a dead rank; that Ray's own worker health check fails the run
#            with a WorkerGroupError. RAS can detect these missing ranks but
#            we don't act on it quickly enough by default.
# "maybe" -> size-dependent or RAS-undetectable. On small tensors these often
#            keep advancing; on large tensors they may truly deadlock and
#            raise NCCLHangError.

TWO_WORKER_SCENARIOS = [
    pytest.param(straggler_train_fn, HANG, id="straggler"),
    pytest.param(dead_rank_train, FAIL, id="dead_rank"),
    pytest.param(op_count_skew_train_fn, MAYBE, id="op_count_skew"),
    pytest.param(collective_mismatch_train_fn, MAYBE, id="collective_mismatch"),
    pytest.param(shape_mismatch_train_fn, MAYBE, id="shape_mismatch"),
]


def _assert_outcome(err, expectation):
    if expectation == HANG:
        assert isinstance(err, NCCLHangError), f"expected NCCLHangError, got {err!r}"
    elif expectation == FAIL:
        assert isinstance(err, WorkerGroupError), f"expected a failure, got {err!r}"
    else:  # MAYBE
        assert err is None or isinstance(err, NCCLHangError), f"unexpected: {err!r}"


@pytest.mark.parametrize("train_func, expectation", TWO_WORKER_SCENARIOS)
def test_hang_scenarios(train_func, expectation, ray_start_4_cpus_2_gpus):
    err = run_train_fn(train_func, num_workers=2)
    _assert_outcome(err, expectation)


@pytest.mark.skipif(
    torch.cuda.device_count() < 4, reason="multi-communicator case needs >= 4 GPUs"
)
def test_multicomm_subset_detected(ray_start_4_cpus_4_gpus):
    # Two communicators, in which only one hangs
    err = run_train_fn(multicomm_subset_train_fn, num_workers=4)
    _assert_outcome(err, HANG)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
