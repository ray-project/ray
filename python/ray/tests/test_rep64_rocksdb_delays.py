"""REP-64 provenance, expanded: the RocksDB backend's full delay surface.

WHY THIS EXISTS
---------------
The original REP-64 harness (test_rep64_node_death_hang.py, PR #64187) injected
delay into **one logical path at a time** (e.g. only the node-death publish) and
ran every arm on the **in-memory GCS**. It concluded that publishing node death
before persist ("F3", node channel only) was sufficient, and that the actor
channel "self-heals" so needed no equivalent fix.

CI disproved that conclusion: with soft-durability ("F4") removed and F3 applied
(PR #64702), `test_dynamic_generator_reconstruction_nondeterministic[None-False]`
and `[None-True]` hang deterministically under the real RocksDB backend
(TIMEOUT 2/2 @ 915.1s, dev=0.0s).

The original design had a structural blind spot, not merely a wrong answer.
RocksDB introduces FOUR distinct delay mechanisms; the old harness modelled one:

  1. per-write WAL fsync latency, on *every* table      (old harness: NODE only)
  2. read latency                                        (old harness: never)
  3. bounded shared I/O pool contention -- reads queue
     behind fsyncing writes; pool defaults to 4 threads
     shared by AsyncGet and AsyncPut alike                (old harness: never)
  4. the combination actually shipped: F4 removed +
     F3 node-only, on the real backend                    (old harness: never --
     all arms were in-memory, where F4 is a no-op)

Mechanism 3 matters specifically because the actor channel's "self-heal" is a
fetch-on-subscribe **read** of the synchronously-set in-memory DEAD state. That
self-heal has a hidden precondition -- the read must be fast -- which single-path
injection could never violate, and which a saturated RocksDB I/O pool does.

This harness models all four, in two tiers:

  Tiers A-D  mechanistic, in-memory GCS + DelayInjectingStoreClient.
             Deterministic, fast, isolates cause.
  Tier  E    ground truth, real RocksDB backend, toggling the F4 set, F3 and
             gcs_rocksdb_io_pool_size. Reproduces the CI failure and attributes it.

This is an EXPERIMENT, not a pass/fail gate: every arm prints a verdict and the
test always passes. Run one arm per process via run_rep64_rocksdb.sh.

Both `too_many_returns` values are parametrized -- the original harness only ever
modelled `num_returns -= 1`, i.e. `[None-False]`.
"""

import os
import subprocess
import sys
import threading
import time

import numpy as np
import pytest

import ray

# A delay >> the per-arm timeout deterministically models "this never arrives".
_DELAY_MS = 300000
_HANG_TIMEOUT_S = 90.0

# Realistic-fsync delay: large enough to matter, small enough that the scenario
# still completes if the design is actually sound. Models a slow disk rather
# than an infinite stall, so arms using it answer "is this merely slow?" instead
# of "is this wedged?".
_FSYNC_MS = 50

_SOFT_DURABLE = "RAY_TESTING_GCS_SOFT_DURABLE_TABLES"
_STORE_WRITE = "RAY_TESTING_GCS_STORE_WRITE_DELAY_MS"
_STORE_READ = "RAY_TESTING_GCS_STORE_READ_DELAY_MS"
_STORE_TABLES = "RAY_TESTING_GCS_STORE_DELAY_TABLES"
_STORE_CONCURRENCY = "RAY_TESTING_GCS_STORE_IO_CONCURRENCY"
_STORE_TRIGGER = "RAY_TESTING_GCS_STORE_DELAY_TRIGGER_FILE"
_F3_NODE = "RAY_TESTING_GCS_NODE_PUBLISH_BEFORE_PERSIST"
_F3_ACTOR = "RAY_TESTING_GCS_ACTOR_PUBLISH_BEFORE_PERSIST"
_F5 = "RAY_TESTING_ENABLE_NODE_DEATH_FALLBACK"

# Each arm: env knobs -> what it isolates. Tiers A-D run on the in-memory GCS
# with storage-layer injection; tier E uses the real RocksDB backend.
_ARMS = {
    # -- Tier A: reproduce the old findings at the storage layer ------------
    # Confirms the new injection point reproduces the known-good baseline and
    # the known node-channel result, so tiers B-D can be trusted.
    "A1_control": {},
    "A2_node_writes_stalled": {
        _STORE_WRITE: str(_DELAY_MS),
        _STORE_TABLES: "NODE",
    },
    "A3_node_writes_stalled_f3": {
        _STORE_WRITE: str(_DELAY_MS),
        _STORE_TABLES: "NODE",
        _F3_NODE: "1",
    },
    # -- Tier B: the actor channel, which the old harness declared benign ---
    # B1 is the old `actor_publish_delayed` equivalent at the storage layer.
    # B2 is the arm that never existed: stall the actor *read* path, i.e.
    # remove the precondition the self-heal silently depends on.
    "B1_actor_writes_stalled": {
        _STORE_WRITE: str(_DELAY_MS),
        _STORE_TABLES: "ACTOR",
    },
    "B2_actor_reads_stalled": {
        _STORE_READ: str(_DELAY_MS),
        _STORE_TABLES: "ACTOR",
    },
    "B3_actor_rw_stalled": {
        _STORE_WRITE: str(_DELAY_MS),
        _STORE_READ: str(_DELAY_MS),
        _STORE_TABLES: "ACTOR",
    },
    "B4_actor_rw_stalled_f3actor": {
        _STORE_WRITE: str(_DELAY_MS),
        _STORE_READ: str(_DELAY_MS),
        _STORE_TABLES: "ACTOR",
        _F3_ACTOR: "1",
    },
    # -- Tier C: global write latency, i.e. what fsync-on-every-table means --
    # The old harness never slowed more than one table at a time.
    "C1_all_writes_slow": {_STORE_WRITE: str(_FSYNC_MS)},
    "C2_all_writes_slow_f3node": {
        _STORE_WRITE: str(_FSYNC_MS),
        _F3_NODE: "1",
    },
    "C3_all_writes_slow_f3both": {
        _STORE_WRITE: str(_FSYNC_MS),
        _F3_NODE: "1",
        _F3_ACTOR: "1",
    },
    "C4_all_writes_slow_f3node_f5": {
        _STORE_WRITE: str(_FSYNC_MS),
        _F3_NODE: "1",
        _F5: "1",
    },
    # -- Tier D: bounded-pool contention (mechanism 3) ----------------------
    # Route every op through a 4-thread pool, matching gcs_rocksdb_io_pool_size,
    # with writes *occupying* a thread for the fsync. Reads then queue behind
    # writes -- unreachable by any single-path delay injection.
    "D1_pool4_writes_slow": {
        _STORE_WRITE: str(_FSYNC_MS),
        _STORE_CONCURRENCY: "4",
    },
    "D2_pool4_writes_slow_f3node": {
        _STORE_WRITE: str(_FSYNC_MS),
        _STORE_CONCURRENCY: "4",
        _F3_NODE: "1",
    },
    "D3_pool4_writes_slow_f3both": {
        _STORE_WRITE: str(_FSYNC_MS),
        _STORE_CONCURRENCY: "4",
        _F3_NODE: "1",
        _F3_ACTOR: "1",
    },
    "D4_pool1_writes_slow_f3node": {
        # Pathological pool of 1: maximum queueing. If F3-node alone survives
        # D2 but not D4, contention depth -- not the node channel -- is the
        # governing variable.
        _STORE_WRITE: str(_FSYNC_MS),
        _STORE_CONCURRENCY: "1",
        _F3_NODE: "1",
    },
    "D5_pool4_writes_slow_f3node_f5": {
        _STORE_WRITE: str(_FSYNC_MS),
        _STORE_CONCURRENCY: "4",
        _F3_NODE: "1",
        _F5: "1",
    },
    # -- Tier E: ground truth on the real RocksDB backend --------------------
    # These use the actual store client (real fsync, real pool, real strands).
    # E2 is master's shipped behavior; E3 is exactly what PR #64702 ships.
    "E1_rocksdb_f4_none": {_SOFT_DURABLE: ""},
    "E2_rocksdb_f4_node_actor": {_SOFT_DURABLE: "NODE,ACTOR"},
    "E3_rocksdb_f4_none_f3node": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "E4_rocksdb_f4_none_f3both": {
        _SOFT_DURABLE: "",
        _F3_NODE: "1",
        _F3_ACTOR: "1",
    },
    "E5_rocksdb_f4_none_f3node_f5": {
        _SOFT_DURABLE: "",
        _F3_NODE: "1",
        _F5: "1",
    },
    "E6_rocksdb_f4_none_f3both_f5": {
        _SOFT_DURABLE: "",
        _F3_NODE: "1",
        _F3_ACTOR: "1",
        _F5: "1",
    },
    # -- Tier F: execution-environment pressure, not GCS timing --------------
    # Tiers A-E showed the RocksDB *delay surface* does not reproduce the CI
    # hang -- not even E3, which is exactly what PR #64702 ships. The untested
    # difference is how CI *runs* the job: `--parallelism-per-worker 3` sharing
    # one container's CPU and disk, on top of a ~16.5 GB spill already emitted
    # by test_generator_oom earlier in the same file. These arms keep the
    # shipped GCS configuration fixed and vary only that pressure, so any hang
    # is attributable to contention rather than to notification ordering.
    #
    # Disk load fsyncs into the same directory as the GCS RocksDB WAL, so it
    # contends for exactly the resource the durability contract depends on.
    "F1_shipped_cpu_load": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "F2_shipped_disk_load": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "F3_shipped_cpu_disk_load": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "F4_shipped_disk_load_heavy": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    # Control: identical pressure, but with the F4 workaround still in place.
    # If F4-on survives what F4-off cannot, the workaround was load-masking.
    "F5_f4on_cpu_disk_load": {_SOFT_DURABLE: "NODE,ACTOR", _F3_NODE: "1"},
    # Does the owner-side fallback survive pressure that F3 alone does not?
    "F6_shipped_cpu_disk_load_f5": {
        _SOFT_DURABLE: "",
        _F3_NODE: "1",
        _F5: "1",
    },
    # -- Tier G: memory pressure --------------------------------------------
    # rocksdb_store_client.cc itself documents that the embedded DB's block
    # cache + memtables live *inside* the GCS process and can get it OOM-killed
    # on a memory-constrained node -- and that this is why a broad core test
    # (test_channel::test_payload_large) fails only under the RocksDB backend.
    # The CI job reached these tests right after test_generator_oom spilled
    # ~16.5 GB, i.e. in exactly that regime. Memory was never varied in tiers
    # A-F.
    "G1_shipped_mem_load": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "G2_shipped_mem_cpu_load": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "G3_shipped_mem_cpu_disk_load": {_SOFT_DURABLE: "", _F3_NODE: "1"},
    "G4_f4on_mem_cpu_disk_load": {_SOFT_DURABLE: "NODE,ACTOR", _F3_NODE: "1"},
}

# arm -> background pressure applied during the death -> recovery phase only.
_ARM_LOAD = {
    "G1_shipped_mem_load": {"mem_procs": 6, "mem_mb": 2048},
    "G2_shipped_mem_cpu_load": {"mem_procs": 6, "mem_mb": 2048, "cpu": 8},
    "G3_shipped_mem_cpu_disk_load": {
        "mem_procs": 6,
        "mem_mb": 2048,
        "cpu": 8,
        "disk": 3,
    },
    "G4_f4on_mem_cpu_disk_load": {
        "mem_procs": 6,
        "mem_mb": 2048,
        "cpu": 8,
        "disk": 3,
    },
    "F1_shipped_cpu_load": {"cpu": 8},
    "F2_shipped_disk_load": {"disk": 3},
    "F3_shipped_cpu_disk_load": {"cpu": 8, "disk": 3},
    "F4_shipped_disk_load_heavy": {"disk": 8},
    "F5_f4on_cpu_disk_load": {"cpu": 8, "disk": 3},
    "F6_shipped_cpu_disk_load_f5": {"cpu": 8, "disk": 3},
}

# Busy-loop; saturates a core so GCS/raylet callbacks compete for CPU.
_CPU_BURN = "x = 0\nwhile True:\n    x += 1\n"

# Repeated write+fsync; contends for the same IOPS/fsync path as the WAL.
_DISK_BURN = (
    "import os, sys\n"
    "path = sys.argv[1]\n"
    "buf = b'x' * (4 * 1024 * 1024)\n"
    "while True:\n"
    "    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)\n"
    "    for _ in range(8):\n"
    "        os.write(fd, buf)\n"
    "        os.fsync(fd)\n"
    "    os.close(fd)\n"
)


# Anonymous memory hog: pins RSS so the GCS/raylet compete for RAM and page
# cache, mirroring a CI container that has already spilled multiple GB.
_MEM_BURN = (
    "import sys, time\n"
    "mb = int(sys.argv[1])\n"
    "buf = bytearray(mb * 1024 * 1024)\n"
    "for i in range(0, len(buf), 4096):\n"
    "    buf[i] = 1\n"
    "while True:\n"
    "    time.sleep(3600)\n"
)


class _Pressure:
    """Spawn CPU/disk contention for the duration of a `with` block."""

    def __init__(self, spec, scratch_dir):
        self._spec = spec or {}
        self._scratch = scratch_dir
        self._procs = []

    def __enter__(self):
        devnull = subprocess.DEVNULL
        for _ in range(self._spec.get("cpu", 0)):
            self._procs.append(
                subprocess.Popen(
                    [sys.executable, "-c", _CPU_BURN], stdout=devnull, stderr=devnull
                )
            )
        for _ in range(self._spec.get("mem_procs", 0)):
            self._procs.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        _MEM_BURN,
                        str(self._spec.get("mem_mb", 512)),
                    ],
                    stdout=devnull,
                    stderr=devnull,
                )
            )
        for i in range(self._spec.get("disk", 0)):
            target = os.path.join(self._scratch, f"diskburn_{i}.bin")
            self._procs.append(
                subprocess.Popen(
                    [sys.executable, "-c", _DISK_BURN, target],
                    stdout=devnull,
                    stderr=devnull,
                )
            )
        return self

    def __exit__(self, *exc):
        for proc in self._procs:
            proc.kill()
        for proc in self._procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        return False


# Arms whose name starts with "E" exercise the real RocksDB storage backend.
_ROCKSDB_ARMS = {
    name
    for name in _ARMS
    if name.startswith("E") or name.startswith("F") or name.startswith("G")
}


@pytest.mark.parametrize("too_many_returns", [False, True])
@pytest.mark.parametrize("arm", list(_ARMS.keys()))
def test_rocksdb_delay_surface(
    ray_start_cluster, monkeypatch, tmp_path, arm, too_many_returns
):
    for env_name, env_val in _ARMS[arm].items():
        monkeypatch.setenv(env_name, env_val)

    # Storage-layer injection is armed only for the death -> notification ->
    # reconstruction phase. Without this gate, an arm that stalls a table
    # "forever" also stalls node *registration*, so the cluster never boots and
    # the arm proves nothing about the path under study.
    trigger_file = tmp_path / "delay_armed"
    monkeypatch.setenv(_STORE_TRIGGER, str(trigger_file))

    config = {
        "health_check_failure_threshold": 10,
        "health_check_period_ms": 100,
        "health_check_initial_delay_ms": 0,
        "max_direct_call_object_size": 100,
        "task_retry_delay_ms": 100,
        "object_timeout_milliseconds": 200,
        "fetch_warn_timeout_milliseconds": 1000,
        "local_gc_min_interval_s": 1,
    }
    if arm in _ROCKSDB_ARMS:
        config["gcs_storage"] = "rocksdb"
        config["gcs_storage_path"] = str(tmp_path / "gcs")
        os.makedirs(config["gcs_storage_path"], exist_ok=True)

    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=1,
        _system_config=config,
        enable_object_reconstruction=True,
        resources={"head": 1},
    )
    ray.init(address=cluster.address)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, resources={"head": 1})
    class FailureSignal:
        def __init__(self):
            return

        def ping(self):
            return

    # num_returns=None is the variant that hangs; "dynamic" is unaffected.
    @ray.remote(num_returns=None)
    def dynamic_generator(failure_signal):
        num_returns = 10
        try:
            ray.get(failure_signal.ping.remote())
        except ray.exceptions.RayActorError:
            if too_many_returns:
                num_returns += 1
            else:
                num_returns -= 1
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    failure_signal = FailureSignal.remote()
    gen = ray.get(dynamic_generator.remote(failure_signal))

    result = {"done": False, "n": None, "err": None, "elapsed": None}

    def drive():
        started = time.perf_counter()
        try:
            result["n"] = len(list(gen))
        except Exception as e:  # noqa: BLE001
            result["err"] = repr(e)
        finally:
            result["elapsed"] = time.perf_counter() - started
            result["done"] = True

    # Everything above ran unimpeded. Pressure and storage-layer injection are
    # applied only from here, so a verdict is attributable to the
    # death -> notification -> reconstruction phase rather than to setup.
    with _Pressure(_ARM_LOAD.get(arm), str(tmp_path)):
        trigger_file.touch()

        cluster.remove_node(node_to_kill, allow_graceful=False)
        ray.kill(failure_signal)

        # Drive list(gen) on a daemon thread so a permanent hang is observable
        # rather than wedging the test process.
        driver = threading.Thread(target=drive, daemon=True)
        driver.start()
        driver.join(timeout=_HANG_TIMEOUT_S)

    # Disarm before teardown so fixture cleanup is not itself stalled.
    trigger_file.unlink(missing_ok=True)

    verdict = "PERMANENT-HANG" if not result["done"] else "FAST"
    print(
        f"\nREP64_RESULT arm={arm} too_many_returns={too_many_returns} "
        f"verdict={verdict} elapsed={result['elapsed']} "
        f"n={result['n']} err={result['err']}"
    )
    # Experiment, not a gate: always surface the verdict.
    assert driver is not None
