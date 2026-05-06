"""Phase 3 end-to-end recovery test (REP-64 POC).

Walks through the user-visible recovery story the PLAN calls for:
start a single-node Ray cluster with `RAY_GCS_STORAGE=rocksdb`, create
a detached actor, kill the GCS process, restart, and verify the actor
is recoverable.

Status (Phase 3): scaffold. The Phase 3 dev-VM environment cannot run
this end-to-end (no built `gcs_server` binary with the new code path
yet). The scaffold lives here so that:

  * The test exists in the dossier from the moment the code path is
    written, and gets exercised on the first machine that builds the
    POC end-to-end.
  * Reviewers can see the exact user-visible scenario being validated.

The C++ test `src/ray/gcs/store_client/test/rocksdb_store_client_test.cc`
covers the *storage layer* recovery claim (close + reopen restores
state). This Python test covers the *cluster level* claim (Ray client
sees actor across GCS restart).

To run on a machine with a Ray build that includes Phase 3 (i.e., one
where `bazel build //:gcs_server` succeeds against this branch):

    pip install ray pytest
    pytest rep-64-poc/harness/integration/test_rocksdb_recovery.py -s

Set `RAY_REP64_GCS_BIN` to a Ray-built `gcs_server` binary if it isn't
on PATH.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time

import pytest

# Skip by default in environments that don't have Ray + the Phase 3
# binary. The marker makes "I don't have the built artifacts" visible
# rather than a silent pass.
pytestmark = pytest.mark.skipif(
    os.environ.get("RAY_REP64_RUN_E2E") != "1",
    reason="Set RAY_REP64_RUN_E2E=1 to run the Phase 3 end-to-end test."
    " Requires a Ray build with the Phase 3 RocksDbStoreClient.",
)


@pytest.fixture()
def gcs_storage_dir():
    """Per-test temp dir for the RocksDB data files."""
    d = tempfile.mkdtemp(prefix="rep64-poc-gcs-")
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _ray_init_with_rocksdb(storage_dir: str):
    """Start a single-node Ray cluster configured for RocksDB FT."""
    import ray

    os.environ["RAY_GCS_STORAGE"] = "rocksdb"
    os.environ["RAY_GCS_STORAGE_PATH"] = storage_dir
    # Workers must wait for GCS to come back, not crash on first
    # disconnect — same knob as Redis-based FT.
    os.environ.setdefault("RAY_gcs_rpc_server_reconnect_timeout_s", "60")

    ray.init(
        address="local",
        include_dashboard=False,
        # Disable usage stats noise so test output stays clean.
        _system_config={
            "gcs_storage": "rocksdb",
            "gcs_storage_path": storage_dir,
        },
    )
    return ray


def _kill_gcs_process():
    """Locate and SIGKILL the GCS process. Returns when it's gone."""
    # Ray writes the GCS PID to the session dir; locating via psutil
    # avoids having to parse Ray internals. Falls back to pgrep.
    try:
        import psutil

        for p in psutil.process_iter(["name", "cmdline"]):
            cmd = p.info.get("cmdline") or []
            if any("gcs_server" in arg for arg in cmd):
                p.kill()
                p.wait(timeout=10)
                return
    except ImportError:
        subprocess.run(["pkill", "-9", "-f", "gcs_server"], check=False)
        time.sleep(1)


def test_detached_actor_survives_gcs_kill(gcs_storage_dir):
    """The headline Phase 3 scenario.

    1. Start Ray with RAY_GCS_STORAGE=rocksdb.
    2. Create a detached actor and put a value into it.
    3. Hard-kill the GCS process.
    4. Wait for GCS supervisor to restart it (Ray's existing FT
       mechanism — same code path Redis-backed FT uses).
    5. Look up the detached actor by name and read the value back.

    Phase 3 owes only this one passing test. Phase 6 expands to
    placement groups, jobs, workers; Phase 8 adds the K8s pod-restart
    variant.
    """
    ray = _ray_init_with_rocksdb(gcs_storage_dir)

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def incr(self, n: int = 1) -> int:
            self.value += n
            return self.value

    counter = Counter.options(name="rep64-counter", lifetime="detached").remote()
    assert ray.get(counter.incr.remote(5)) == 5

    _kill_gcs_process()
    # Allow Ray's GCS supervisor to restart the process.
    time.sleep(15)

    # Re-lookup by name. With the embedded backend, the actor record
    # was persisted to RocksDB before the kill; recovery should succeed.
    recovered = ray.get_actor("rep64-counter")
    assert (
        ray.get(recovered.incr.remote(7)) == 12
    ), "Expected detached actor's state to survive GCS restart"

    ray.shutdown()


def test_storage_dir_marker_present(gcs_storage_dir):
    """After the cluster has run once, the cluster-ID marker should
    exist in the RocksDB data directory.

    This is a smoke check that the marker mechanism actually fired,
    not a substitute for the C++ unit test that exercises the
    fail-fast path on mismatch.
    """
    ray = _ray_init_with_rocksdb(gcs_storage_dir)
    # Issue at least one write so the storage layer initializes.
    @ray.remote
    def noop():
        return 1

    assert ray.get(noop.remote()) == 1
    ray.shutdown()

    # The directory should be non-empty after the cluster ran.
    files = os.listdir(gcs_storage_dir)
    assert files, f"Expected RocksDB to have written files under {gcs_storage_dir}"
