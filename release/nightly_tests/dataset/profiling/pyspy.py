# ABOUTME: Attaches py-spy CPU profiling to the driver and to Ray UDF worker processes.
# ABOUTME: Driver profiling runs on head; worker profiling runs via Ray actors on sampled nodes.

import os
import re
import signal
import subprocess
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


# --- Configuration (override via environment) ---
PYSPY_FORMAT = os.environ.get("PYSPY_FORMAT", "speedscope")
PYSPY_RATE = int(os.environ.get("PYSPY_RATE", "100"))

_FORMAT_EXTENSIONS = {
    "speedscope": ".speedscope.json",
    "flamegraph": ".svg",
    "raw": ".raw",
}

# Module-level handle so stop() can reach it.
_pyspy_proc = None
_log_file = None


def _ensure_pyspy_permissions():
    """Sets ptrace_scope to 0 or applies setuid to py-spy binary."""
    import shutil

    try:
        subprocess.run(
            ["sudo", "sysctl", "-w", "kernel.yama.ptrace_scope=0"],
            check=True,
            capture_output=True,
        )
        return
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    pyspy_path = shutil.which("py-spy")
    if pyspy_path:
        try:
            subprocess.run(
                ["sudo", "chmod", "u+s", pyspy_path],
                check=True,
                capture_output=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass


def start(outdir):
    """Launches py-spy on the current (driver) process with --subprocesses.

    Args:
        outdir: Shared storage directory for profile output.
    """
    global _pyspy_proc, _log_file

    _ensure_pyspy_permissions()

    pid = os.getpid()
    os.makedirs(outdir, exist_ok=True)

    ext = _FORMAT_EXTENSIONS.get(PYSPY_FORMAT, ".raw")
    output_path = f"{outdir}/pyspy_driver{ext}"
    log_path = f"{outdir}/pyspy_driver.log"

    cmd = [
        "py-spy",
        "record",
        "-p",
        str(pid),
        "-o",
        output_path,
        "-f",
        PYSPY_FORMAT,
        "-r",
        str(PYSPY_RATE),
        "--nonblocking",
        "--subprocesses",
        # Include native (C / C++ / Cython) stack frames so the speedscope
        # surfaces raylet RPC / cloudpickle / arrow leaves alongside Python.
        "--native",
    ]

    _log_file = open(log_path, "w")
    _log_file.write(f"cmd: {' '.join(cmd)}\n")
    _log_file.write(f"driver pid: {pid}\n")
    _log_file.write(f"output_path: {output_path}\n")
    _log_file.flush()

    _pyspy_proc = subprocess.Popen(cmd, stdout=_log_file, stderr=_log_file)
    _log_file.write(f"py-spy pid: {_pyspy_proc.pid}\n")
    _log_file.flush()

    print(f"py-spy profiling started on driver (pid {pid}) -> {output_path}")


def stop(timeout=15):
    """Sends SIGINT to py-spy and waits for it to write output.

    Args:
        timeout: Seconds to wait for py-spy to flush output.
    """
    global _pyspy_proc, _log_file

    if _pyspy_proc is None:
        print("No py-spy process to stop.")
        return

    print("Stopping py-spy...")
    try:
        _pyspy_proc.send_signal(signal.SIGINT)
        _pyspy_proc.wait(timeout=timeout)
        print(f"py-spy exited with code {_pyspy_proc.returncode}")
        if _log_file:
            _log_file.write(f"py-spy exited with code {_pyspy_proc.returncode}\n")
    except subprocess.TimeoutExpired:
        print(f"py-spy did not exit in {timeout}s, killing")
        _pyspy_proc.kill()
        if _log_file:
            _log_file.write(f"py-spy killed after {timeout}s timeout\n")
    except ProcessLookupError:
        print("py-spy already exited")
        if _log_file:
            _log_file.write("py-spy already exited\n")
    finally:
        if _log_file:
            _log_file.flush()
            _log_file.close()
            _log_file = None
        _pyspy_proc = None


# ---------------------------------------------------------------------------
# Worker-node UDF profiling via Ray actors
# ---------------------------------------------------------------------------


def _sanitize_proc_name(name):
    """Strip ray:: prefix and replace filename-unsafe chars."""
    if name.startswith("ray::"):
        name = name[len("ray::") :]
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name) or "unknown"


# Linux comm is capped at 15 chars; these are prefixes of the sanitized
# (ray::-stripped) name that identify non-UDF processes we must skip.
_INFRA_NAME_PREFIXES = (
    "DashboardA",  # ray::DashboardAgent
    "RuntimeEnv",  # ray::RuntimeEnvAgent
    "APILogAge",  # ray::APILogAgent
    "AgentBase",  # ray::AgentBase* (older Ray)
    "_start_net",  # our own net_monitor task (_start_net_io_monitor)
    "_UDFPySpy",  # our own py-spy actor (_UDFPySpyProfiler)
    "_RayletPe",  # our own perf actor (_RayletPerfProfiler)
)


def _is_infra_worker(name):
    return any(name.startswith(p) for p in _INFRA_NAME_PREFIXES)


def _find_ray_workers(max_targets, retries=150, interval=2):
    """Find ray:: UDF worker PIDs on the current node.

    Polls up to retries*interval seconds. Returns on the first attempt that
    finds at least one non-infrastructure ray:: worker — infra processes
    (DashboardAgent, RuntimeEnvAgent, our own profiler actors, etc.) exist
    from node boot and would otherwise be picked before the UDF workers
    spawn.

    Returns:
        List of (pid, sanitized_name) tuples, up to max_targets.
    """
    own_pid = os.getpid()
    for attempt in range(retries):
        try:
            result = subprocess.run(
                ["pgrep", "-f", "ray::"],
                capture_output=True,
                text=True,
            )
            pids = []
            if result.returncode == 0:
                pids = [int(p) for p in result.stdout.strip().split("\n") if p]
                pids = [p for p in pids if p != own_pid]
            candidates = []
            for pid in pids:
                try:
                    raw = subprocess.check_output(
                        ["ps", "-p", str(pid), "-o", "comm="],
                        text=True,
                    ).strip()
                except subprocess.CalledProcessError:
                    continue
                name = _sanitize_proc_name(raw)
                if _is_infra_worker(name):
                    continue
                candidates.append((pid, name))
            if candidates:
                return candidates[:max_targets]
        except (ValueError, IndexError):
            pass
        if attempt < retries - 1:
            time.sleep(interval)
    print(
        f"WARNING: no UDF ray:: workers found after {retries * interval}s "
        f"(infra-only PIDs skipped)"
    )
    return []


def _attach_worker_pyspy(pid, name, outdir, name_counts):
    """Attach py-spy to one Ray worker PID.

    Output filename: pyspy_worker_<nodeip>_<name>.speedscope.json. If the same
    sanitized name has already been used on this node, append _pid<pid> to
    disambiguate.

    Returns:
        (proc, log_file, output_path) tuple, or None on failure.
    """
    node_ip = ray.util.get_node_ip_address().replace(".", "_")
    ext = _FORMAT_EXTENSIONS.get(PYSPY_FORMAT, ".raw")

    if name_counts.get(name, 0) > 0:
        label = f"{name}_pid{pid}"
    else:
        label = name
    name_counts[name] = name_counts.get(name, 0) + 1

    output_path = f"{outdir}/pyspy_worker_{node_ip}_{label}{ext}"
    log_path = f"{outdir}/pyspy_worker_{node_ip}_{label}.log"

    # No --subprocesses here: UDF worker processes don't spawn Python
    # children, and including the flag complicates py-spy's SIGINT shutdown.
    cmd = [
        "py-spy",
        "record",
        "-p",
        str(pid),
        "-o",
        output_path,
        "-f",
        PYSPY_FORMAT,
        "-r",
        str(PYSPY_RATE),
        "--nonblocking",
        # Include native (C / C++ / Cython) stack frames so worker UDF
        # speedscopes surface arrow / serialization leaves alongside Python.
        "--native",
    ]

    log_file = open(log_path, "w")
    log_file.write(f"cmd: {' '.join(cmd)}\n")
    log_file.write(f"target pid: {pid}\n")
    log_file.write(f"target name: {name}\n")
    log_file.flush()

    def _reset_signals():
        # Ray actor workers run with SIGINT blocked in the process signal
        # mask (so the actor survives shutdown signals that target the
        # raylet's group). A blocked signal mask survives both fork AND
        # exec, so py-spy installs a SIGINT handler that never fires — the
        # signal is stuck in the pending set. Unblock it here, then reset
        # the dispositions so py-spy gets a clean slate.
        signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGINT, signal.SIGTERM])
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=log_file,
            preexec_fn=_reset_signals,
            start_new_session=True,
        )
    except OSError as e:
        log_file.write(f"Failed to start py-spy: {e}\n")
        log_file.close()
        print(f"WARNING: failed to start py-spy for pid {pid}: {e}")
        return None

    # Give py-spy a moment to fail on startup (missing permissions etc).
    time.sleep(1)
    if proc.poll() is not None:
        log_file.write(f"py-spy exited early with code {proc.returncode}\n")
        log_file.close()
        print(
            f"WARNING: py-spy failed to start for pid {pid} "
            f"(code {proc.returncode})"
        )
        return None

    log_file.write(f"py-spy pid: {proc.pid}\n")
    log_file.flush()
    print(f"py-spy started on {node_ip} for pid {pid} ({name}) -> {output_path}")
    return (proc, log_file, output_path)


def _stop_worker_pyspy(proc, log_file, timeout=60):
    """SIGINT a single py-spy process and wait for it to flush output.

    py-spy's ctrlc handler handles SIGINT only (SIGTERM hits the default
    disposition and kills py-spy without flushing). See _reset_signals in
    _attach_worker_pyspy for why SIGINT works here despite being inherited
    as SIG_IGN from the Ray actor worker.
    """
    if proc is None:
        return
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=timeout)
        msg = f"py-spy exited with code {proc.returncode}"
        print(msg)
        if log_file:
            log_file.write(msg + "\n")
    except subprocess.TimeoutExpired:
        msg = f"py-spy did not exit in {timeout}s, killing"
        print(msg)
        proc.kill()
        if log_file:
            log_file.write(msg + "\n")
    except ProcessLookupError:
        msg = "py-spy already exited"
        print(msg)
        if log_file:
            log_file.write(msg + "\n")
    finally:
        if log_file:
            log_file.flush()
            log_file.close()


@ray.remote(num_cpus=0, num_gpus=0)
class _UDFPySpyProfiler:
    """Profiles Ray UDF Python workers on its node.

    Actor is pinned to one node via NodeAffinitySchedulingStrategy. On
    start() it polls for ray:: worker processes (which are spawned lazily
    by the raylet once the pipeline begins) and attaches py-spy to each,
    up to max_targets. Ray actors execute methods serially by default, so
    stop() blocks until start() completes.
    """

    def __init__(self, outdir, max_targets=3):
        self._outdir = outdir
        self._max_targets = max_targets
        self._profilers = []

    def ping(self):
        """Cheap method used by the driver to confirm scheduling.

        Returns once the actor is scheduled and constructed, so the driver
        can safely fire-and-forget start.remote() after this.
        """
        return ray.util.get_node_ip_address()

    def start(self):
        _ensure_pyspy_permissions()
        workers = _find_ray_workers(self._max_targets)
        name_counts = {}
        for pid, name in workers:
            handle = _attach_worker_pyspy(pid, name, self._outdir, name_counts)
            if handle:
                self._profilers.append(handle)
        return len(self._profilers)

    def stop(self):
        for proc, log_file, _ in self._profilers:
            _stop_worker_pyspy(proc, log_file)
        self._profilers = []


def start_worker_nodes(
    outdir, num_cpu_workers=5, num_gpu_workers=5, max_targets_per_node=3
):
    """Launch py-spy profiling actors on a sample of worker nodes.

    Args:
        outdir: Shared storage directory for profile output.
        num_cpu_workers: Number of CPU-only worker nodes to profile.
        num_gpu_workers: Number of GPU worker nodes to profile.
        max_targets_per_node: Max ray:: workers to attach py-spy to per node.

    Returns:
        List of actor handles for stop_workers.
    """
    head_node_id = ray.get_runtime_context().get_node_id()
    monitored_node_ids = set()
    actors = []
    cpu_count = 0
    gpu_count = 0
    target_count = num_cpu_workers + num_gpu_workers

    stale_polls = 0
    max_stale_polls = 30  # 30 * 2s = 60s with no new nodes

    while (cpu_count + gpu_count) < target_count:
        found_new = False
        for node in ray.nodes():
            if not node["Alive"] or node["NodeID"] in monitored_node_ids:
                continue
            if node["NodeID"] == head_node_id:
                continue
            has_gpu = node["Resources"].get("GPU", 0) > 0
            if has_gpu and gpu_count >= num_gpu_workers:
                continue
            if not has_gpu and cpu_count >= num_cpu_workers:
                continue
            try:
                actor = _UDFPySpyProfiler.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"], soft=False
                    )
                ).remote(outdir, max_targets=max_targets_per_node)
                # Confirm scheduling with a cheap ping so stop() isn't left
                # holding a dead handle later. GPU node actor startup can
                # take > 30s on a freshly-provisioned cluster.
                ray.get(actor.ping.remote(), timeout=90)
                # Fire-and-forget start: the actor polls for ray:: workers to
                # appear once the pipeline begins. ray.get()ing start here
                # would block the driver before the pipeline has had a chance
                # to spawn any workers.
                actor.start.remote()
                actors.append(actor)
                monitored_node_ids.add(node["NodeID"])
                if has_gpu:
                    gpu_count += 1
                else:
                    cpu_count += 1
                found_new = True
                print(
                    f"py-spy worker actor scheduled on {node['NodeManagerAddress']} "
                    f"(cpu={cpu_count}/{num_cpu_workers}, "
                    f"gpu={gpu_count}/{num_gpu_workers})"
                )
            except Exception as e:
                monitored_node_ids.add(node["NodeID"])
                print(
                    f"Failed to schedule py-spy actor on "
                    f"{node['NodeManagerAddress']}: {e}"
                )
        if not found_new:
            stale_polls += 1
            if stale_polls >= max_stale_polls:
                print(
                    f"py-spy: no new worker nodes for {max_stale_polls * 2}s, "
                    f"proceeding with {cpu_count} CPU + {gpu_count} GPU "
                    f"({len(actors)} actors attached)"
                )
                break
        else:
            stale_polls = 0
        time.sleep(2)
    if (cpu_count + gpu_count) >= target_count:
        print(
            f"py-spy worker actors scheduled on {cpu_count} CPU + "
            f"{gpu_count} GPU worker nodes"
        )
    return actors


def stop_workers(actors):
    """Stop worker-node py-spy profilers.

    Each actor's stop() runs after its start() has completed (Ray actors are
    serial by default), so this finalizes any py-spy subprocesses that were
    attached during the run.

    Args:
        actors: List of actor handles from start_worker_nodes.
    """
    if not actors:
        return
    print(f"Stopping {len(actors)} worker py-spy profilers...")
    ray.get([a.stop.remote() for a in actors])
