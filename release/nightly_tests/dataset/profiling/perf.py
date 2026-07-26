# ABOUTME: Attaches perf record CPU profiling to GCS and raylet C++ processes.
# ABOUTME: Provides head-node profiling, worker-node actor-based profiling, and collapsed stack generation.

import os
import re
import shutil
import signal
import subprocess
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def _ensure_perf_available():
    """Check that perf is installed. Returns True if available."""
    if shutil.which("perf"):
        return True
    print("WARNING: perf not found on PATH. Skipping CPU profiling.")
    return False


def _ensure_perf_permissions():
    """Lower perf_event_paranoid so non-root users can record."""
    try:
        subprocess.run(
            ["sudo", "sysctl", "-w", "kernel.perf_event_paranoid=-1"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["sudo", "sysctl", "-w", "kernel.kptr_restrict=0"],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"WARNING: Failed to set perf permissions: {e}")


def find_pid(process_name, use_full=False, retries=15, interval=2):
    """Find a process PID by name with retries for startup races.

    Args:
        process_name: Process name to match.
        use_full: If True, match against full command line (pgrep -f).
                  If False, match exact process name (pgrep -x).
        retries: Number of retry attempts.
        interval: Seconds between retries.

    Returns:
        PID as int, or None if not found after all retries.
    """
    flag = "-f" if use_full else "-x"
    for attempt in range(retries):
        try:
            result = subprocess.run(
                ["pgrep", flag, process_name],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                pid = int(result.stdout.strip().split("\n")[0])
                return pid
        except (ValueError, IndexError):
            pass
        if attempt < retries - 1:
            time.sleep(interval)
    print(f"WARNING: Process '{process_name}' not found after {retries * interval}s")
    return None


def start_profiling(pid, label, outdir):
    """Start perf record on a process.

    Args:
        pid: Target process ID.
        label: Label for output files (e.g. "gcs", "raylet").
        outdir: Directory for output files.

    Returns:
        (subprocess.Popen, IO, data_path) tuple, or (None, None, None) if perf
        fails to start.
    """
    node_ip = ray.util.get_node_ip_address().replace(".", "_")

    os.makedirs(outdir, exist_ok=True)
    data_path = f"{outdir}/perf_{label}_{node_ip}.data"
    log_path = f"{outdir}/perf_{label}_{node_ip}.log"

    cmd = [
        "perf",
        "record",
        "-g",
        "--call-graph",
        "fp",
        "-F",
        "99",
        "-p",
        str(pid),
        "-o",
        data_path,
    ]

    log_file = open(log_path, "w")
    log_file.write(f"cmd: {' '.join(cmd)}\n")
    log_file.write(f"target pid: {pid}\n")
    log_file.flush()

    try:
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
    except OSError as e:
        log_file.write(f"Failed to start perf: {e}\n")
        log_file.close()
        print(f"WARNING: Failed to start perf for {label} (pid {pid}): {e}")
        return None, None, None

    # Give perf a moment to fail on startup.
    time.sleep(1)
    if proc.poll() is not None:
        log_file.write(f"perf exited early with code {proc.returncode}\n")
        log_file.close()
        print(f"WARNING: perf failed to start for {label} (code {proc.returncode})")
        return None, None, None

    log_file.write(f"perf pid: {proc.pid}\n")
    log_file.flush()
    print(f"perf profiling started for {label} (target pid {pid}) -> {data_path}")
    return proc, log_file, data_path


def stop_profiler(proc, log_file, timeout=15):
    """Stop a perf process gracefully.

    Args:
        proc: subprocess.Popen handle from start_profiling.
        log_file: Log file handle from start_profiling.
        timeout: Seconds to wait for perf to flush output.
    """
    if proc is None:
        return

    print(f"Stopping perf (pid {proc.pid})...")
    try:
        # Use SIGTERM rather than SIGINT. perf record handles both for clean
        # shutdown, but SIGINT can be ignored when perf lacks a controlling
        # terminal (common inside Ray actor worker processes).
        os.kill(proc.pid, signal.SIGTERM)
        proc.wait(timeout=timeout)
        msg = f"perf exited with code {proc.returncode}"
        print(msg)
        if log_file:
            log_file.write(msg + "\n")
    except subprocess.TimeoutExpired:
        msg = f"perf did not exit in {timeout}s, killing"
        print(msg)
        proc.kill()
        if log_file:
            log_file.write(msg + "\n")
    except ProcessLookupError:
        msg = "perf already exited"
        print(msg)
        if log_file:
            log_file.write(msg + "\n")
    finally:
        if log_file:
            log_file.flush()
            log_file.close()


def _clean_func_name(name):
    """Strip hex offsets and clean up a function name from perf script output.

    Removes trailing +0x... offsets and replaces semicolons (which conflict
    with the collapsed stack delimiter) with colons.
    """
    # Strip trailing +0xHEX offset
    name = re.sub(r"\+0x[0-9a-f]+$", "", name)
    # Replace semicolons in C++ names (template args, etc.)
    name = name.replace(";", ":")
    return name


def generate_collapsed_stacks(outdir, data_path=None):
    """Convert perf.data files to collapsed stack format.

    Produces *_collapsed.txt next to each *.data. Must be run on a machine
    whose kernel and runtime libraries match the one that recorded the data —
    for Ray worker profiles this means the worker node itself, before its
    /tmp/ray/session_* directory is torn down.

    Args:
        outdir: Directory to scan (ignored if data_path is given).
        data_path: If set, convert only this single .data file. Otherwise
                   convert every perf_*.data under outdir that does not
                   already have a matching _collapsed.txt.

    perf script output format:
        command  pid/tid  [cpu] timestamp: event:
            hex_addr func_name+0xoff (dso)
            hex_addr func_name+0xoff (dso)
            ...
        <blank line>
    """
    import glob as globmod

    if data_path is not None:
        data_files = [data_path]
    else:
        data_files = globmod.glob(os.path.join(outdir, "perf_*.data"))
    if not data_files:
        print("No perf data files found to convert.")
        return

    # Regex for the header line: "command pid/tid [cpu] timestamp: event:"
    header_re = re.compile(r"^\s*(\S+)\s+\d+")

    for data_path in data_files:
        name = os.path.basename(data_path).replace(".data", "")
        collapsed_path = os.path.join(
            os.path.dirname(data_path) or outdir, f"{name}_collapsed.txt"
        )
        if os.path.exists(collapsed_path):
            print(f"Skipping {data_path}: {collapsed_path} already exists")
            continue
        print(f"Converting {data_path} -> {collapsed_path}")
        try:
            perf_script = subprocess.Popen(
                ["perf", "script", "-i", data_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            with open(collapsed_path, "w") as out:
                comm = ""
                stack = []
                for line in perf_script.stdout:
                    line = line.decode("utf-8", errors="replace").rstrip()
                    if line == "":
                        if stack:
                            prefix = comm + ";" if comm else ""
                            out.write(prefix + ";".join(reversed(stack)) + " 1\n")
                            stack = []
                            comm = ""
                    elif line and line[0] in (" ", "\t"):
                        # Stack frame: leading whitespace + hex_addr + func_name+0xoff (dso)
                        parts = line.strip().split(" ", 1)
                        if len(parts) >= 2:
                            addr, rest = parts
                            # Split trailing "(dso)" off the end if present.
                            # Stripped libs (libcudart, libcublas, libtorch
                            # extensions, etc.) yield "[unknown] (dso)" — keep
                            # the DSO basename so per-library time is visible.
                            dso = ""
                            if rest.endswith(")"):
                                paren = rest.rfind(" (")
                                if paren != -1:
                                    dso = rest[paren + 2 : -1].strip()
                                    rest = rest[:paren]
                            func = _clean_func_name(rest)
                            if func == "[unknown]":
                                if dso and dso not in ("unknown", "[unknown]"):
                                    dso_base = os.path.basename(dso).replace(";", ":")
                                    # "@0x..." rather than "+0x..." so the
                                    # address survives collapsed_to_speedscope
                                    # (which strips trailing +0xHEX offsets).
                                    # analyze_pyspy_profile groups by DSO by
                                    # default and shows the @addr with
                                    # --detail-unknowns.
                                    func = f"unk_{dso_base}@0x{addr}"
                                else:
                                    func = f"unk_0x{addr}"
                            stack.append(func)
                    else:
                        # Header line: extract command name (thread)
                        m = header_re.match(line)
                        if m:
                            comm = m.group(1)
                if stack:
                    prefix = comm + ";" if comm else ""
                    out.write(prefix + ";".join(reversed(stack)) + " 1\n")
            perf_script.wait()
            size = os.path.getsize(collapsed_path)
            print(f"  -> {collapsed_path} ({size} bytes)")
        except Exception as e:
            print(f"  WARNING: Failed to convert {data_path}: {e}")


# ---------------------------------------------------------------------------
# Head-node profiling: GCS + local raylet
# ---------------------------------------------------------------------------


def start_head_node(outdir):
    """Start perf profiling on GCS and raylet processes on the head node.

    Returns:
        List of (proc, log_file, data_path) tuples for each started profiler.
    """
    handles = []
    if not _ensure_perf_available():
        return handles
    _ensure_perf_permissions()

    gcs_pid = find_pid("gcs_server", use_full=True)
    if gcs_pid:
        handles.append(start_profiling(gcs_pid, "gcs", outdir))

    raylet_pid = find_pid("raylet")
    if raylet_pid:
        handles.append(start_profiling(raylet_pid, "raylet", outdir))

    return handles


def stop_head(handles):
    """Stop head-node perf profilers and convert their data to collapsed stacks.

    Symbolization happens here (on the head) because the head's filesystem
    is what produced the .data files.

    Args:
        handles: List of (proc, log_file, data_path) tuples from start_head_node.
    """
    for proc, log_file, data_path in handles:
        stop_profiler(proc, log_file)
        if data_path and os.path.exists(data_path):
            try:
                generate_collapsed_stacks(
                    os.path.dirname(data_path), data_path=data_path
                )
            except Exception as e:
                print(f"WARNING: head collapse failed for {data_path}: {e}")


# ---------------------------------------------------------------------------
# Worker-node profiling via Ray actors
# ---------------------------------------------------------------------------


@ray.remote(num_cpus=0, num_gpus=0)
class _RayletPerfProfiler:
    """Actor that profiles the local raylet with perf record.

    Using an actor (not a task) so the head node can call stop() explicitly
    before the job exits, giving perf a clean SIGTERM to finalize the data file.
    """

    def __init__(self, outdir):
        self._outdir = outdir
        self._proc = None
        self._log_file = None
        self._data_path = None

    def start(self):
        if not _ensure_perf_available():
            return False
        _ensure_perf_permissions()
        pid = find_pid("raylet")
        if pid is None:
            return False
        self._proc, self._log_file, self._data_path = start_profiling(
            pid, "raylet", self._outdir
        )
        return self._proc is not None

    def stop(self):
        if self._proc is not None:
            print(
                f"Actor stop() called, perf pid={self._proc.pid}, "
                f"poll={self._proc.poll()}"
            )
        stop_profiler(self._proc, self._log_file, timeout=30)
        # Convert to collapsed stacks on this worker node, while the local
        # /tmp/ray/session_* runtime_env libs (libtorch, libcuda, etc.) are
        # still on disk. If we defer this to the head, those DSOs are gone
        # and ~every user-space frame resolves to [unknown].
        if self._data_path and os.path.exists(self._data_path):
            try:
                generate_collapsed_stacks(self._outdir, data_path=self._data_path)
            except Exception as e:
                print(f"WARNING: worker collapse failed for {self._data_path}: {e}")
        self._proc = None
        self._log_file = None
        self._data_path = None


def start_worker_nodes(outdir, num_cpu_workers=5, num_gpu_workers=5):
    """Launch perf profiling actors on a sample of worker nodes.

    Args:
        outdir: Shared storage directory for profile output.
        num_cpu_workers: Number of CPU-only worker nodes to profile.
        num_gpu_workers: Number of GPU worker nodes to profile.

    Returns:
        List of actor handles (for passing to stop_workers later).
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
                actor = _RayletPerfProfiler.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"], soft=False
                    ),
                ).remote(outdir)
                started = ray.get(actor.start.remote())
                found_new = True
                if started:
                    actors.append(actor)
                    monitored_node_ids.add(node["NodeID"])
                    if has_gpu:
                        gpu_count += 1
                    else:
                        cpu_count += 1
                    print(
                        f"Perf profiling raylet on {node['NodeManagerAddress']} "
                        f"(cpu={cpu_count}/{num_cpu_workers}, "
                        f"gpu={gpu_count}/{num_gpu_workers})"
                    )
                else:
                    monitored_node_ids.add(node["NodeID"])
                    print(f"Perf failed to start on {node['NodeManagerAddress']}")
            except Exception as e:
                # Mark the node as monitored so we don't retry it forever.
                monitored_node_ids.add(node["NodeID"])
                print(f"Failed perf on {node['NodeManagerAddress']}: {e}")
        if not found_new:
            stale_polls += 1
            if stale_polls >= max_stale_polls:
                print(
                    f"Perf: no new worker nodes for {max_stale_polls * 2}s, "
                    f"proceeding with {cpu_count} CPU + {gpu_count} GPU "
                    f"({len(actors)} actors attached)"
                )
                break
        else:
            stale_polls = 0
        time.sleep(2)
    if (cpu_count + gpu_count) >= target_count:
        print(f"Perf profiling active on {cpu_count} CPU + {gpu_count} GPU workers")
    return actors


def stop_workers(actors):
    """Stop worker-node perf profilers.

    Args:
        actors: List of actor handles from start_worker_nodes.
    """
    if not actors:
        return
    print(f"Stopping {len(actors)} worker perf profilers...")
    ray.get([a.stop.remote() for a in actors])
