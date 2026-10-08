import json
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from pyarrow.fs import LocalFileSystem

import ray
from ray._common.test_utils import run_string_as_driver, wait_for_condition
from ray.data.checkpoint import _checkpoint_guard as guard_module
from ray.data.checkpoint._checkpoint_guard import (
    CheckpointPathGuard,
    _checkpoint_key,
    _CheckpointGuardCoordinator,
)


@pytest.fixture
def coordinator(monkeypatch):
    records = {}
    jobs = []

    def put(key, value, overwrite, *, namespace):
        exists = key in records
        if overwrite or not exists:
            records[key] = value
        return exists

    monkeypatch.setattr(
        guard_module, "_internal_kv_get", lambda key, **kwargs: records.get(key)
    )
    monkeypatch.setattr(guard_module, "_internal_kv_put", put)
    monkeypatch.setattr(
        guard_module, "_internal_kv_del", lambda key, **kwargs: records.pop(key, None)
    )
    monkeypatch.setattr(ray._private.state, "jobs", lambda: jobs)
    return _CheckpointGuardCoordinator(), records, jobs


def test_guard_rejects_live_and_unknown_jobs(coordinator):
    guard, records, jobs = coordinator
    assert guard.acquire(b"path", "first", "job-1")
    for job_info in ([], [{"JobID": "job-1", "IsDead": False}]):
        jobs[:] = job_info
        with pytest.raises(RuntimeError, match="already in use by Ray job job-1"):
            guard.acquire(b"path", "second", "job-2")
        assert json.loads(records[b"path"]) == ["first", "job-1"]


def test_guard_retry_and_stale_release(coordinator):
    guard, records, _ = coordinator
    assert guard.acquire(b"path", "first", "job-1")
    # A lost acquisition response can be retried, including after actor restart.
    guard = _CheckpointGuardCoordinator()
    assert guard.acquire(b"path", "first", "job-1")
    guard.release(b"path", "not-owner", "job-1")
    assert b"path" in records
    guard.release(b"path", "first", "job-1")
    assert guard.acquire(b"path", "second", "job-1")
    guard.release(b"path", "first", "job-1")
    assert json.loads(records[b"path"]) == ["second", "job-1"]
    assert guard.acquire(b"other", "third", "job-1")


def test_guard_recovers_only_finished_jobs(coordinator):
    guard, records, jobs = coordinator
    assert guard.acquire(b"path", "first", "job-1")
    jobs.append({"JobID": "job-1", "IsDead": True})
    assert guard.acquire(b"path", "second", "job-2")
    guard.release(b"path", "first", "job-1")
    assert json.loads(records[b"path"]) == ["second", "job-2"]


def test_guard_job_lookup_failure_preserves_owner(coordinator, monkeypatch):
    guard, records, _ = coordinator
    assert guard.acquire(b"path", "first", "job-1")

    def unavailable():
        raise RuntimeError("GCS unavailable")

    monkeypatch.setattr(ray._private.state, "jobs", unavailable)
    with pytest.raises(RuntimeError, match="GCS unavailable"):
        guard.acquire(b"path", "second", "job-2")
    assert json.loads(records[b"path"]) == ["first", "job-1"]


def test_checkpoint_key_normalization(tmp_path):
    fs = LocalFileSystem()
    assert _checkpoint_key(str(tmp_path), fs) == _checkpoint_key(
        str(tmp_path / "child" / ".."), fs
    )
    assert _checkpoint_key(str(tmp_path), fs) == _checkpoint_key(tmp_path.as_uri(), fs)
    storage = SimpleNamespace(type_name="s3", normalize_path=lambda path: path)
    assert _checkpoint_key("s3://bucket/prefix/", storage) == _checkpoint_key(
        "bucket/prefix", storage
    )
    assert _checkpoint_key("bucket/a/../b", storage) != _checkpoint_key(
        "bucket/b", storage
    )


def test_concurrent_guards_have_one_winner(ray_start_regular, tmp_path):
    guards = [CheckpointPathGuard(str(tmp_path), LocalFileSystem()) for _ in range(8)]

    def acquire(guard):
        try:
            guard.acquire()
            return True
        except RuntimeError as exc:
            assert "already in use" in str(exc)
            return False

    try:
        with ThreadPoolExecutor(max_workers=len(guards)) as pool:
            results = list(pool.map(acquire, guards))
        assert sum(results) == 1
        winner = guards[results.index(True)]
        for guard, acquired in zip(guards, results):
            if not acquired:
                guard.release()
        contender = CheckpointPathGuard(str(tmp_path), LocalFileSystem())
        with pytest.raises(RuntimeError, match="already in use"):
            contender.acquire()
        winner.release()
        contender.acquire()
        contender.release()
    finally:
        for guard in guards:
            guard.release()


def test_coordinator_restart_preserves_guard(ray_start_regular, tmp_path):
    owner = CheckpointPathGuard(str(tmp_path), LocalFileSystem())
    contender = CheckpointPathGuard(str(tmp_path), LocalFileSystem())
    try:
        owner.acquire()
        ray.kill(owner._coordinator, no_restart=False)
        with pytest.raises(RuntimeError, match="already in use"):
            contender.acquire()
        owner.release()
        contender.acquire()
    finally:
        owner.release()
        contender.release()


def test_guard_across_driver_namespaces(ray_start_regular, tmp_path):
    owner = CheckpointPathGuard(str(tmp_path), LocalFileSystem())
    owner.acquire()
    script = f"""
import ray
from pyarrow.fs import LocalFileSystem
from ray.data.checkpoint._checkpoint_guard import CheckpointPathGuard
ray.init(address={ray_start_regular['address']!r}, namespace='another-namespace')
guard = CheckpointPathGuard({str(tmp_path)!r}, LocalFileSystem())
try:
    guard.acquire()
except RuntimeError as exc:
    assert 'already in use' in str(exc)
else:
    raise AssertionError('A second job acquired an active checkpoint path')
finally:
    guard.release()
    ray.shutdown()
"""
    try:
        run_string_as_driver(script, timeout=60)
        # The rejected job's cleanup must not release the first job's guard.
        contender = CheckpointPathGuard(str(tmp_path), LocalFileSystem())
        with pytest.raises(RuntimeError, match="already in use"):
            contender.acquire()
    finally:
        owner.release()


def test_finished_driver_guard_can_be_recovered(ray_start_regular, tmp_path):
    script = f"""
import ray
from pyarrow.fs import LocalFileSystem
from ray.data.checkpoint._checkpoint_guard import CheckpointPathGuard
ray.init(address={ray_start_regular['address']!r}, namespace='previous-driver')
guard = CheckpointPathGuard({str(tmp_path)!r}, LocalFileSystem())
guard.acquire()
# Simulate an abandoned guard by exiting without releasing it.
ray.shutdown()
"""
    run_string_as_driver(script, timeout=60)
    guard = CheckpointPathGuard(str(tmp_path), LocalFileSystem())

    def acquired():
        try:
            guard.acquire()
            return True
        except RuntimeError as exc:
            assert "already in use" in str(exc)
            return False

    try:
        wait_for_condition(acquired, timeout=30)
    finally:
        guard.release()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
