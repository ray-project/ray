import pytest

import ray._private.utils as utils


class _FakeRuntimeContext:
    def __init__(self, accelerator_ids=None, assigned_resources=None):
        self._accelerator_ids = accelerator_ids or {}
        self._assigned_resources = assigned_resources or {}

    def get_accelerator_ids(self):
        return self._accelerator_ids

    def get_assigned_resources(self):
        return self._assigned_resources


@pytest.fixture
def fake_affinity(monkeypatch):
    affinity = {"value": {0, 1, 2, 3}}
    set_calls = []

    monkeypatch.setattr(
        utils.os,
        "sched_getaffinity",
        lambda pid: set(affinity["value"]),
        raising=False,
    )

    def set_affinity(pid, cpu_set):
        affinity["value"] = set(cpu_set)
        set_calls.append(set(cpu_set))

    monkeypatch.setattr(utils.os, "sched_setaffinity", set_affinity, raising=False)
    return affinity, set_calls


def test_nvidia_gpu_numa_affinity_disabled_by_default(monkeypatch, fake_affinity):
    affinity, set_calls = fake_affinity
    monkeypatch.delenv(utils.RAY_EXPERIMENTAL_NVIDIA_GPU_NUMA_AFFINITY_ENV_VAR, False)

    original = utils.set_nvidia_gpu_numa_affinity_if_enabled()

    assert original is None
    assert affinity["value"] == {0, 1, 2, 3}
    assert set_calls == []


def test_nvidia_gpu_numa_affinity_intersects_current_affinity(
    monkeypatch, fake_affinity
):
    affinity, set_calls = fake_affinity
    monkeypatch.setenv(utils.RAY_EXPERIMENTAL_NVIDIA_GPU_NUMA_AFFINITY_ENV_VAR, "1")
    monkeypatch.setattr(
        utils.ray,
        "get_runtime_context",
        lambda: _FakeRuntimeContext(
            accelerator_ids={"GPU": ["0"]},
            assigned_resources={"GPU": 1.0},
        ),
    )
    monkeypatch.setattr(utils, "_get_nvidia_gpu_cpu_affinity", lambda gpu_id: {2, 3, 4})

    original = utils.set_nvidia_gpu_numa_affinity_if_enabled()

    assert original == {0, 1, 2, 3}
    assert affinity["value"] == {2, 3}
    assert set_calls == [{2, 3}]

    utils.reset_nvidia_gpu_numa_affinity(original)
    assert affinity["value"] == {0, 1, 2, 3}


def test_nvidia_gpu_numa_affinity_rejects_fractional_gpus(monkeypatch, fake_affinity):
    affinity, set_calls = fake_affinity
    monkeypatch.setenv(utils.RAY_EXPERIMENTAL_NVIDIA_GPU_NUMA_AFFINITY_ENV_VAR, "1")
    monkeypatch.setattr(
        utils.ray,
        "get_runtime_context",
        lambda: _FakeRuntimeContext(
            accelerator_ids={"GPU": ["0"]},
            assigned_resources={"GPU": 0.5},
        ),
    )

    original = utils.set_nvidia_gpu_numa_affinity_if_enabled()

    assert original is None
    assert affinity["value"] == {0, 1, 2, 3}
    assert set_calls == []


def test_nvidia_gpu_numa_affinity_rejects_multiple_gpu_cpu_sets(
    monkeypatch, fake_affinity
):
    affinity, set_calls = fake_affinity
    monkeypatch.setenv(utils.RAY_EXPERIMENTAL_NVIDIA_GPU_NUMA_AFFINITY_ENV_VAR, "1")
    monkeypatch.setattr(
        utils.ray,
        "get_runtime_context",
        lambda: _FakeRuntimeContext(
            accelerator_ids={"GPU": ["0", "1"]},
            assigned_resources={"GPU": 2.0},
        ),
    )
    gpu_cpu_sets = {0: {0, 1}, 1: {2, 3}}
    monkeypatch.setattr(
        utils, "_get_nvidia_gpu_cpu_affinity", lambda gpu_id: gpu_cpu_sets[gpu_id]
    )

    original = utils.set_nvidia_gpu_numa_affinity_if_enabled()

    assert original is None
    assert affinity["value"] == {0, 1, 2, 3}
    assert set_calls == []
